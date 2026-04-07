package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/warpstreamlabs/bento/public/service"
)

// errBackpressureTimeout is returned when WaitForSpace times out due to sustained backpressure.
// This is a retryable error that should trigger a backoff before resubscribing.
var errBackpressureTimeout = errors.New("backpressure timeout waiting for space in pending pool")

// kinesisEFOAPI is the subset of kinesis.Client methods used by kinesisEFOManager.
type kinesisEFOAPI interface {
	RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
	DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
}

// kinesisEFOManager handles Enhanced Fan Out consumer registration and lifecycle
type kinesisEFOManager struct {
	streamARN    string
	consumerName string
	consumerARN  string
	svc          kinesisEFOAPI
	log          *service.Logger
	// pollInterval controls how long waitForActiveConsumer waits between status
	// checks. Defaults to 2 seconds; overridable in tests for faster iteration.
	pollInterval time.Duration
}

// newKinesisEFOManager creates a new EFO manager
func newKinesisEFOManager(conf *kiEFOConfig, streamARN, clientID string, svc *kinesis.Client, log *service.Logger) (*kinesisEFOManager, error) {
	if conf == nil {
		return nil, errors.New("enhanced fan out config is nil")
	}

	if conf.ConsumerName != "" && conf.ConsumerARN != "" {
		return nil, errors.New("cannot specify both consumer_name and consumer_arn")
	}

	consumerName := conf.ConsumerName
	if consumerName == "" && conf.ConsumerARN == "" {
		consumerName = "bento-" + clientID
	}

	return &kinesisEFOManager{
		streamARN:    streamARN,
		consumerName: consumerName,
		consumerARN:  conf.ConsumerARN,
		svc:          svc,
		log:          log,
	}, nil
}

// ensureConsumerRegistered registers the consumer if needed and returns the consumer ARN
func (m *kinesisEFOManager) ensureConsumerRegistered(ctx context.Context) (string, error) {
	if m.consumerARN != "" {
		m.log.Debugf("Using provided consumer ARN: %s", m.consumerARN)
		return m.consumerARN, nil
	}

	m.log.Debugf("Registering Enhanced Fan Out consumer: %s for stream: %s", m.consumerName, m.streamARN)

	registerInput := &kinesis.RegisterStreamConsumerInput{
		StreamARN:    aws.String(m.streamARN),
		ConsumerName: aws.String(m.consumerName),
	}

	output, err := m.svc.RegisterStreamConsumer(ctx, registerInput)
	if err != nil {
		var resourceInUse *types.ResourceInUseException
		if errors.As(err, &resourceInUse) {
			m.log.Debugf("Consumer %s already exists, describing to get ARN", m.consumerName)
			return m.describeAndWaitForActive(ctx)
		}
		return "", fmt.Errorf("failed to register consumer: %w", err)
	}

	if output.Consumer == nil || output.Consumer.ConsumerARN == nil {
		return "", errors.New("RegisterStreamConsumer succeeded but returned no consumer ARN")
	}

	m.consumerARN = *output.Consumer.ConsumerARN
	m.log.Debugf("Registered consumer with ARN: %s, waiting for ACTIVE status", m.consumerARN)

	if err := m.waitForActiveConsumer(ctx); err != nil {
		return "", fmt.Errorf("failed waiting for consumer to become active: %w", err)
	}

	return m.consumerARN, nil
}

// describeAndWaitForActive describes an existing consumer and waits for it to be active
func (m *kinesisEFOManager) describeAndWaitForActive(ctx context.Context) (string, error) {
	describeInput := &kinesis.DescribeStreamConsumerInput{
		StreamARN:    aws.String(m.streamARN),
		ConsumerName: aws.String(m.consumerName),
	}

	output, err := m.svc.DescribeStreamConsumer(ctx, describeInput)
	if err != nil {
		return "", fmt.Errorf("failed to describe consumer: %w", err)
	}

	if output.ConsumerDescription == nil || output.ConsumerDescription.ConsumerARN == nil {
		return "", errors.New("consumer description missing ARN")
	}

	m.consumerARN = *output.ConsumerDescription.ConsumerARN
	m.log.Debugf("Found existing consumer with ARN: %s", m.consumerARN)

	if output.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
		m.log.Debugf("Consumer is already ACTIVE")
		return m.consumerARN, nil
	}

	if err := m.waitForActiveConsumer(ctx); err != nil {
		return "", fmt.Errorf("failed waiting for consumer to become active: %w", err)
	}

	return m.consumerARN, nil
}

// waitForActiveConsumer waits for the consumer to reach ACTIVE status
func (m *kinesisEFOManager) waitForActiveConsumer(ctx context.Context) error {
	waiterCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	interval := m.pollInterval
	if interval == 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// Check consumer status immediately before waiting for the next tick
		describeInput := &kinesis.DescribeStreamConsumerInput{
			ConsumerARN: aws.String(m.consumerARN),
		}

		output, err := m.svc.DescribeStreamConsumer(waiterCtx, describeInput)
		if err != nil {
			return fmt.Errorf("failed to describe consumer: %w", err)
		}

		if output.ConsumerDescription != nil {
			status := output.ConsumerDescription.ConsumerStatus
			m.log.Debugf("Consumer status: %s", status)

			if status == types.ConsumerStatusActive {
				m.log.Debugf("Consumer is now ACTIVE")
				return nil
			}

			if status == types.ConsumerStatusDeleting {
				return errors.New("consumer is being deleted")
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for consumer to become ACTIVE: %w", ctx.Err())
		case <-waiterCtx.Done():
			return fmt.Errorf("timeout waiting for consumer to become ACTIVE: %w", waiterCtx.Err())
		case <-ticker.C:
		}
	}
}

// runEFOConsumer consumes from a shard using Enhanced Fan Out
func (k *kinesisReader) runEFOConsumer(wg *sync.WaitGroup, info streamInfo, shardID, startingSequence string) error {
	// Create record batcher (same as polling mode)
	var recordBatcher *awsKinesisRecordBatcher
	var err error
	if recordBatcher, err = k.newAWSKinesisRecordBatcher(info, shardID, startingSequence); err != nil {
		wg.Done()
		if _, checkErr := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, startingSequence, true); checkErr != nil {
			k.log.Errorf("Failed to gracefully yield checkpoint: %v\n", checkErr)
		}
		return err
	}

	// Track consumer state
	state := awsKinesisConsumerConsuming
	var pendingMsg asyncMessage

	// Buffer for pending records from the subscription
	var pending []types.Record
	var pendingBytes int

	// Track records moved into the batcher but not yet flushed to msgChan.
	var batcherCount int
	var batcherBytes int

	// Track records sent to the pipeline (msgChan) but not yet acknowledged.
	// Pool space is held until the output acks, so backpressure accounts for
	// the full lifecycle: pending + batcher + in-flight pipeline data.
	var inFlightCount atomic.Int64
	var inFlightBytes atomic.Int64

	// Channels for subscription control
	subscriptionTrigger := make(chan string, 1) // Trigger for initial subscription or resubscription
	subscriptionTrigger <- startingSequence     // Start with initial sequence

	// Channels for timed batches and message flush
	var nextTimedBatchChan <-chan time.Time
	var nextFlushChan chan<- asyncMessage
	var nextRecordsChan <-chan []types.Record
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, k.commitPeriod)

	go func() {
		defer func() {
			commitCtxClose()
			recordBatcher.Close(context.Background(), state == awsKinesisConsumerFinished)

			// Release any remaining pool space: pending + batcher + in-flight pipeline data.
			// On normal shutdown, in-flight ackFns may have already released their share
			// (atomic counters track this). On hard stop, ackFns may not fire, so this
			// defer acts as the safety net. The pool clamps to zero on over-release.
			remainingInFlight := int(inFlightCount.Load())
			remainingInFlightBytes := int(inFlightBytes.Load())
			totalRelease := len(pending) + batcherCount + remainingInFlight
			totalReleaseBytes := pendingBytes + batcherBytes + remainingInFlightBytes
			if totalRelease > 0 || totalReleaseBytes > 0 {
				k.globalPendingPool.Release(totalRelease, totalReleaseBytes)
			}

			reason := ""
			switch state {
			case awsKinesisConsumerFinished:
				reason = " because the shard is closed"
				if err := k.checkpointer.Delete(k.ctx, info.id, shardID); err != nil {
					k.log.Errorf("Failed to remove checkpoint for finished stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := k.checkpointer.Yield(k.ctx, info.id, shardID, recordBatcher.GetSequence()); err != nil {
					k.log.Errorf("Failed to yield checkpoint for stolen stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, recordBatcher.GetSequence(), true); err != nil {
					k.log.Errorf("Failed to store final checkpoint for stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			}

			wg.Done()
			k.log.Debugf("Closing stream '%v' shard '%v' as client '%v'%v", info.id, shardID, k.checkpointer.clientID, reason)
		}()

		k.log.Debugf("Consuming stream '%v' shard '%v' with Enhanced Fan Out as client '%v'", info.id, shardID, k.checkpointer.clientID)

		// Start subscription in a separate goroutine
		bufferCap := 0
		if k.conf.EnhancedFanOut != nil {
			bufferCap = k.conf.EnhancedFanOut.RecordBufferCap
		}
		recordsChan := make(chan []types.Record, bufferCap)
		// errorsChan is used for logging/monitoring only - subscription goroutine
		// handles its own retries. Non-blocking sends handle overflow gracefully.
		errorsChan := make(chan error, 1)
		resubscribeChan := make(chan string, 1)
		shardFinishedChan := make(chan struct{}, 1)

		// drainRecordsChan drains any remaining records from recordsChan after
		// the subscription goroutine has stopped, releasing their pool capacity.
		// This prevents leaking pool capacity when the consumer exits with buffered records.
		drainRecordsChan := func() {
			for {
				select {
				case records := <-recordsChan:
					drainBytes := 0
					for _, r := range records {
						drainBytes += len(r.Data)
					}
					k.globalPendingPool.Release(len(records), drainBytes)
				default:
					return
				}
			}
		}

		var subscriptionWg sync.WaitGroup
		subscriptionWg.Go(func() {
			// Subscription goroutine manages its own backoff for retries
			subBoff := backoff.NewExponentialBackOff()
			subBoff.InitialInterval = 300 * time.Millisecond
			subBoff.MaxInterval = 5 * time.Second
			subBoff.MaxElapsedTime = 0 // Never stop retrying

			for sequence := range subscriptionTrigger {
				currentSeq := sequence

				// Inner retry loop - keeps trying until success or context cancellation
				for {
					select {
					case <-k.ctx.Done():
						return
					default:
					}

					continuationSeq, shardFinished, err := k.efoSubscribeAndStream(k.ctx, info, shardID, currentSeq, recordsChan)

					if err != nil {
						// Check for non-retryable errors - these should stop the subscription
						var resourceNotFound *types.ResourceNotFoundException
						var invalidArg *types.InvalidArgumentException
						if errors.As(err, &resourceNotFound) || errors.As(err, &invalidArg) {
							// Send to errorsChan for main loop to handle shutdown.
							// Use blocking send (with context) to ensure fatal errors are not dropped.
							k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, err)
							select {
							case <-k.ctx.Done():
							case errorsChan <- err:
							}
							return // Stop retrying
						}

						// Log retryable error (non-blocking)
						select {
						case errorsChan <- err:
						default:
							// Channel full, just log locally
							if errors.Is(err, errBackpressureTimeout) {
								k.log.Debugf("EFO backpressure timeout for shard %v, will retry", shardID)
							} else {
								k.log.Warnf("EFO subscription error for shard %v, will retry: %v", shardID, err)
							}
						}

						// Update sequence for retry if we got a continuation
						if continuationSeq != "" {
							currentSeq = continuationSeq
						}

						// Backoff before retry
						backoffDuration := subBoff.NextBackOff()
						select {
						case <-k.ctx.Done():
							return
						case <-time.After(backoffDuration):
						}
						continue // Retry the subscription
					}

					// Success - reset backoff
					subBoff.Reset()

					if shardFinished {
						// Shard is closed, signal to main loop
						select {
						case shardFinishedChan <- struct{}{}:
						default:
						}
						return
					}

					// Subscription completed normally, update sequence and notify main loop
					if continuationSeq != "" {
						currentSeq = continuationSeq
					}
					select {
					case <-k.ctx.Done():
						return
					case resubscribeChan <- currentSeq:
					}
					break // Exit retry loop, wait for next trigger from main loop
				}
			}
		})

		// Main consumer loop (similar to polling consumer)
		for {
			if pendingMsg.msg == nil {
				// If our consumer is finished and we've run out of pending
				// records then we're done.
				if len(pending) == 0 && state == awsKinesisConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(k.ctx); pendingMsg.msg == nil {
						close(subscriptionTrigger)
						subscriptionWg.Wait()
						drainRecordsChan()
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					var err error
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var r types.Record
					for i, r = range pending {
						if recordBatcher.AddRecord(r) {
							var err error
							if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
								k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
							}
							break
						}
					}
					// Transfer processed records from pending tracking to batcher tracking.
					// Pool space is NOT released here — it stays held until the batched
					// message is flushed to msgChan, so backpressure accounts for all
					// in-memory data (pending + batcher + in-flight messages).
					processedCount := i + 1
					processedBytes := 0
					for _, r := range pending[:processedCount] {
						processedBytes += len(r.Data)
					}
					batcherCount += processedCount
					batcherBytes += processedBytes
					pendingBytes -= processedBytes
					pending = pending[processedCount:]
				}
			}

			// Decide whether to flush. Wrap ackFn once so pool.Release fires
			// when the pipeline acks, not when the message is sent to msgChan.
			if pendingMsg.msg != nil {
				if batcherCount > 0 {
					releaseCount := batcherCount
					releaseBytes := batcherBytes
					originalAckFn := pendingMsg.ackFn
					pendingMsg.ackFn = func(ctx context.Context, err error) error {
						k.globalPendingPool.Release(releaseCount, releaseBytes)
						inFlightCount.Add(-int64(releaseCount))
						inFlightBytes.Add(-int64(releaseBytes))
						return originalAckFn(ctx, err)
					}
					// Move batcher tracking to in-flight now — the wrapped ackFn
					// captures these values and will release them on ack.
					inFlightCount.Add(int64(batcherCount))
					inFlightBytes.Add(int64(batcherBytes))
					batcherCount = 0
					batcherBytes = 0
				}
				nextFlushChan = k.msgChan
			} else {
				nextFlushChan = nil
			}

			// Always listen for records - backpressure is applied in efoSubscribeAndStream
			// via globalPendingPool.Acquire() before sending to recordsChan
			nextRecordsChan = recordsChan

			if nextTimedBatchChan == nil {
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
				}
			}

			select {
			case <-commitCtx.Done():
				if k.ctx.Err() != nil {
					state = awsKinesisConsumerClosing
					close(subscriptionTrigger)
					subscriptionWg.Wait()
					drainRecordsChan()
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(k.ctx, k.commitPeriod)

				if state == awsKinesisConsumerConsuming {
					stillOwned, err := k.checkpointer.Checkpoint(k.ctx, info.id, shardID, recordBatcher.GetSequence(), false)
					if err != nil {
						k.log.Errorf("Failed to store checkpoint for Kinesis stream '%v' shard '%v': %v", info.id, shardID, err)
					} else if !stillOwned {
						state = awsKinesisConsumerYielding
						close(subscriptionTrigger)
						subscriptionWg.Wait()
						drainRecordsChan()
						return
					}
				}

			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil

			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}

			case records := <-nextRecordsChan:
				// Received records from subscription
				// Space was already acquired in efoSubscribeAndStream before sending
				for _, r := range records {
					pendingBytes += len(r.Data)
				}
				pending = append(pending, records...)

			case err := <-errorsChan:
				// Subscription error received - log it.
				// The subscription goroutine handles its own retry logic with backoff,
				// so we don't need to trigger resubscription from here.
				var resourceNotFound *types.ResourceNotFoundException
				var invalidArg *types.InvalidArgumentException

				if errors.As(err, &resourceNotFound) || errors.As(err, &invalidArg) {
					// Non-retryable errors are still fatal
					k.log.Errorf("Non-retryable EFO error for shard %v: %v", shardID, err)
					state = awsKinesisConsumerClosing
					close(subscriptionTrigger)
					subscriptionWg.Wait()
					drainRecordsChan()
					return
				}

				// Log retryable errors (subscription goroutine handles retry)
				if errors.Is(err, errBackpressureTimeout) {
					k.log.Debugf("EFO backpressure timeout for shard %v, subscription goroutine will retry", shardID)
				} else {
					k.log.Warnf("EFO subscription error for shard %v, subscription goroutine will retry: %v", shardID, err)
				}

			case sequence := <-resubscribeChan:
				// Subscription completed successfully, resubscribe immediately to maintain continuous data flow
				select {
				case subscriptionTrigger <- sequence:
				case <-k.ctx.Done():
				}

			case <-shardFinishedChan:
				// Shard is closed, mark as finished so we can drain pending records
				state = awsKinesisConsumerFinished

			case <-k.ctx.Done():
				state = awsKinesisConsumerClosing
				close(subscriptionTrigger)
				subscriptionWg.Wait()
				drainRecordsChan()
				return
			}
		}
	}()

	return nil
}

// efoSubscribeAndStream subscribes to a shard and streams records to a channel
// Returns: continuationSequence, shardFinished, error
func (k *kinesisReader) efoSubscribeAndStream(ctx context.Context, info streamInfo, shardID, startingSequence string, recordsChan chan<- []types.Record) (string, bool, error) {
	if info.efoManager == nil || info.efoManager.consumerARN == "" {
		return "", false, errors.New("EFO manager or consumer ARN not initialized")
	}

	// Build starting position
	var startingPosition *types.StartingPosition
	if startingSequence == "" {
		// No sequence yet, use TRIM_HORIZON or LATEST based on config
		if k.conf.StartFromOldest {
			startingPosition = &types.StartingPosition{
				Type: types.ShardIteratorTypeTrimHorizon,
			}
		} else {
			startingPosition = &types.StartingPosition{
				Type: types.ShardIteratorTypeLatest,
			}
		}
	} else {
		// Continue from last sequence
		startingPosition = &types.StartingPosition{
			Type:           types.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber: aws.String(startingSequence),
		}
	}

	// Determine initial reservation size: use rolling average if available,
	// otherwise fall back to the configured initial value.
	reservationCfg := 0
	if k.conf.EnhancedFanOut != nil {
		reservationCfg = k.conf.EnhancedFanOut.ShardReadReservationBytes
	}
	reservationBytes := reservationCfg
	if k.eventSizeAvg != nil {
		if avg := k.eventSizeAvg.Get(); avg > 0 {
			reservationBytes = avg
		}
	}

	// Acquire byte reservation before subscribing — limits how many shards
	// can have active subscriptions concurrently.
	if reservationBytes > 0 {
		if !k.globalPendingPool.Acquire(ctx, 0, reservationBytes) {
			if ctx.Err() != nil {
				return "", false, ctx.Err()
			}
			// Reservation exceeds pool max (canNeverFit). Fall back to no reservation.
			k.log.Warnf("Shard %v: reservation %d bytes exceeds pool max %d bytes, subscribing without reservation",
				shardID, reservationBytes, k.globalPendingPool.MaxBytes())
			reservationBytes = 0
		}
	}

	// releaseReservation refunds any held reservation bytes back to the pool.
	// Must be called on every exit path from this function.
	releaseReservation := func() {
		if reservationBytes > 0 {
			k.globalPendingPool.Release(0, reservationBytes)
			reservationBytes = 0
		}
	}

	k.log.Debugf("Subscribing to shard %v with sequence %v (reservation: %d bytes)", shardID, startingSequence, reservationBytes)

	input := &kinesis.SubscribeToShardInput{
		ConsumerARN:      aws.String(info.efoManager.consumerARN),
		ShardId:          aws.String(shardID),
		StartingPosition: startingPosition,
	}

	output, err := k.svc.SubscribeToShard(ctx, input)
	if err != nil {
		releaseReservation()
		return "", false, fmt.Errorf("failed to subscribe to shard: %w", err)
	}

	// Process the event stream
	eventStream := output.GetStream()
	defer eventStream.Close()

	continuationSeq := ""
	lastReceivedSeq := ""
	shardFinished := false
	eventsChan := eventStream.Events()

	// Timeout for waiting on backpressure - if we wait too long, close the subscription
	// cleanly and resubscribe rather than letting AWS forcibly terminate the connection.
	// 30 seconds is well under the 5-minute EFO subscription timeout.
	const backpressureTimeout = 30 * time.Second

	for {
		// If no reservation held, apply backpressure via WaitForSpace (original behaviour).
		// When reservations are enabled, the reservation itself provides backpressure.
		if reservationBytes == 0 && reservationCfg == 0 {
			switch k.globalPendingPool.WaitForSpace(ctx, backpressureTimeout) {
			case WaitForSpaceCancelled:
				if continuationSeq == "" {
					continuationSeq = lastReceivedSeq
				}
				return continuationSeq, false, ctx.Err()
			case WaitForSpaceTimeout:
				k.log.Debugf("Backpressure timeout for shard %v, closing subscription to resubscribe with backoff", shardID)
				if continuationSeq == "" {
					continuationSeq = lastReceivedSeq
				}
				return continuationSeq, false, errBackpressureTimeout
			case WaitForSpaceOK:
			}
		}

		// Now fetch the next event
		event, ok := <-eventsChan
		if !ok {
			break
		}

		switch e := event.(type) {
		case *types.SubscribeToShardEventStreamMemberSubscribeToShardEvent:
			// Got records event
			shardEvent := e.Value

			// Send records to channel and track last received sequence
			if len(shardEvent.Records) > 0 {
				// Compute byte size for the batch
				totalBytes := 0
				for _, r := range shardEvent.Records {
					totalBytes += len(r.Data)
				}

				// Update the global event size average for adaptive reservations
				if k.eventSizeAvg != nil {
					k.eventSizeAvg.Update(totalBytes)
				}

				if reservationBytes > 0 {
					// Convert reservation into actual record tracking (atomic, non-blocking).
					// If actual < reservation: frees pool space.
					// If actual > reservation: pool grows (next Acquire will enforce limits).
					k.globalPendingPool.ConvertReservation(len(shardEvent.Records), totalBytes, reservationBytes)
					reservationBytes = 0
				} else {
					// No reservation held — acquire normally
					if !k.globalPendingPool.Acquire(ctx, len(shardEvent.Records), totalBytes) {
						if ctx.Err() != nil {
							if continuationSeq == "" {
								continuationSeq = lastReceivedSeq
							}
							return continuationSeq, false, ctx.Err()
						}
						// Batch exceeds pool maximum (canNeverFit)
						k.log.Warnf("EFO batch for shard %v exceeds pool limits (%d records, %d bytes) — "+
							"increase max_pending_records (currently %d) or max_pending_bytes (currently %d)",
							shardID, len(shardEvent.Records), totalBytes,
							k.globalPendingPool.Max(), k.globalPendingPool.MaxBytes())
						if continuationSeq == "" {
							continuationSeq = lastReceivedSeq
						}
						return continuationSeq, false, fmt.Errorf(
							"EFO batch too large for pool: %d records (%d bytes)", len(shardEvent.Records), totalBytes)
					}
				}

				// Track the last record's sequence number for fallback
				lastRecord := shardEvent.Records[len(shardEvent.Records)-1]
				if lastRecord.SequenceNumber != nil {
					lastReceivedSeq = *lastRecord.SequenceNumber
				}
				select {
				case recordsChan <- shardEvent.Records:
				case <-ctx.Done():
					// Release the acquired space since we couldn't send
					k.globalPendingPool.Release(len(shardEvent.Records), totalBytes)
					if continuationSeq == "" {
						continuationSeq = lastReceivedSeq
					}
					return continuationSeq, false, ctx.Err()
				}
			}

			// Update continuation sequence for next subscription
			if shardEvent.ContinuationSequenceNumber != nil {
				continuationSeq = *shardEvent.ContinuationSequenceNumber
			}

			// Check if shard is closed (has child shards)
			if len(shardEvent.ChildShards) > 0 {
				k.log.Debugf("Shard %v is closed, child shards: %v", shardID, len(shardEvent.ChildShards))
				shardFinished = true
			}

			if shardEvent.MillisBehindLatest != nil {
				k.log.Debugf("Shard %v is %d milliseconds behind latest", shardID, *shardEvent.MillisBehindLatest)
			}

		default:
			k.log.Warnf("Unknown event type received: %T", event)
		}

		// Re-acquire reservation for the next event read.
		// Use the adaptive average if available, otherwise the configured initial value.
		if reservationCfg > 0 && reservationBytes == 0 {
			nextReservation := reservationCfg
			if k.eventSizeAvg != nil {
				if avg := k.eventSizeAvg.Get(); avg > 0 {
					nextReservation = avg
				}
			}
			if !k.globalPendingPool.Acquire(ctx, 0, nextReservation) {
				if ctx.Err() != nil {
					if continuationSeq == "" {
						continuationSeq = lastReceivedSeq
					}
					return continuationSeq, false, ctx.Err()
				}
				// Reservation can't fit — close subscription for backoff
				k.log.Debugf("Backpressure: shard %v cannot re-acquire reservation (%d bytes), closing subscription", shardID, nextReservation)
				if continuationSeq == "" {
					continuationSeq = lastReceivedSeq
				}
				return continuationSeq, false, errBackpressureTimeout
			}
			reservationBytes = nextReservation
		}
	}

	// Release any held reservation on stream end
	releaseReservation()

	// Use lastReceivedSeq as fallback if continuationSeq not set
	if continuationSeq == "" {
		continuationSeq = lastReceivedSeq
	}

	// Check for stream errors
	if err := eventStream.Err(); err != nil {
		// Check if it's just end of stream
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return continuationSeq, shardFinished, nil
		}
		return continuationSeq, shardFinished, fmt.Errorf("error receiving event: %w", err)
	}

	return continuationSeq, shardFinished, nil
}
