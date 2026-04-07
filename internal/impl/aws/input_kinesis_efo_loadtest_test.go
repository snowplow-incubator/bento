package aws

import (
	"context"
	"crypto/rand"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/require"
)

// shardConsumerMode controls whether the simulated consumer releases pool space
// when records move into the batcher ("early", old behaviour) or when the
// batched message is flushed to msgChan ("deferred", new behaviour).
type shardConsumerMode int

const (
	releaseEarly    shardConsumerMode = iota // old: release when pending → batcher
	releaseDeferred                          // mid: release when batcher → msgChan
	releaseOnAck                             // new: release when output acks (end-to-end)
)

// efoLoadTestConfig holds all tunables for the load simulation.
type efoLoadTestConfig struct {
	numShards         int
	maxPendingRecords int
	maxPendingBytes   int
	recordsPerBatch   int           // records per EFO SubscribeToShardEvent
	recordSizeBytes   int           // bytes per record
	batchSize         int           // records accumulated before flush (batch policy count trigger)
	testDuration      time.Duration //nolint:unused
	shardStartupDelay time.Duration
	outputLatency     time.Duration // simulated downstream write time
	subscriptionDelay time.Duration // delay between EFO events (simulates HTTP/2 read)
	mode              shardConsumerMode
}

// efoLoadTestResult captures metrics from a load test run.
type efoLoadTestResult struct {
	recordsProduced   int64
	recordsConsumed   int64
	bytesProduced     int64
	bytesConsumed     int64
	backpressureCount int64
	peakPoolRecords   int64
	peakPoolBytes     int64
	peakHeapAlloc     int64
	finalHeapAlloc    uint64
	totalHeapAlloc    uint64
	numGC             uint32
}

// runEFOLoadTest executes the load simulation and returns metrics.
//
// Architecture mirrors the real code:
//
//	Subscription goroutine (per shard):
//	  WaitForSpace → read event → Acquire → send to recordsChan
//
//	Consumer goroutine (per shard):
//	  receive from recordsChan → append to pending
//	  move pending → batcher (release pool in early mode)
//	  when batcher full → flush to msgChan (release pool in deferred mode)
//
//	Output goroutine (shared):
//	  drain msgChan with simulated latency
func runEFOLoadTest(t *testing.T, cfg efoLoadTestConfig, testDuration time.Duration) efoLoadTestResult {
	t.Helper()

	pool := newGlobalPendingPool(cfg.maxPendingRecords, cfg.maxPendingBytes)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration+5*time.Second)
	defer cancel()

	var res efoLoadTestResult

	// Atomic peak trackers
	var peakPoolRecords, peakPoolBytes, peakAlloc atomic.Int64

	// Peak pool tracker
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cur := int64(pool.Current())
				curB := int64(pool.CurrentBytes())
				for {
					old := peakPoolRecords.Load()
					if cur <= old || peakPoolRecords.CompareAndSwap(old, cur) {
						break
					}
				}
				for {
					old := peakPoolBytes.Load()
					if curB <= old || peakPoolBytes.CompareAndSwap(old, curB) {
						break
					}
				}
			}
		}
	}()

	// Peak memory tracker
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				alloc := int64(m.Alloc)
				for {
					old := peakAlloc.Load()
					if alloc <= old || peakAlloc.CompareAndSwap(old, alloc) {
						break
					}
				}
			}
		}
	}()

	// Shared msgChan — consumer goroutines flush batches here
	type flushedBatch struct {
		count int
		bytes int
		// data holds references to the actual record payloads so they stay
		// live on the heap until the output "processes" them. This makes
		// memory tracking realistic — without it the GC would collect the
		// payloads immediately after they leave the batcher.
		data [][]byte
		// ackFn is called after the output "processes" the batch (releaseOnAck mode).
		ackFn func()
	}
	msgChan := make(chan flushedBatch, 100)

	// Downstream output goroutine
	var totalConsumed, totalBytesConsumed atomic.Int64
	var outputWg sync.WaitGroup
	outputWg.Add(1)
	go func() {
		defer outputWg.Done()
		for batch := range msgChan {
			totalConsumed.Add(int64(batch.count))
			totalBytesConsumed.Add(int64(batch.bytes))
			_ = batch.data // keep data live until after sleep
			time.Sleep(cfg.outputLatency)
			if batch.ackFn != nil {
				batch.ackFn() // release pool space on ack
			}
		}
	}()

	testCtx, testCancel := context.WithTimeout(ctx, testDuration)
	defer testCancel()

	// Per-shard channels: subscription → consumer
	type recordBatch struct {
		count int
		bytes int
		data  [][]byte
	}

	var totalProduced, totalBytesProduced, backpressureEvents atomic.Int64

	var shardWg sync.WaitGroup
	for shard := range cfg.numShards {
		// Staggered startup
		select {
		case <-time.After(time.Duration(shard) * cfg.shardStartupDelay):
		case <-testCtx.Done():
			break
		}

		recordsChan := make(chan recordBatch, 1) // like record_buffer_cap: 1

		// --- Subscription goroutine (simulates efoSubscribeAndStream) ---
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()
			defer close(recordsChan)

			for {
				select {
				case <-testCtx.Done():
					return
				default:
				}

				// WaitForSpace before reading the next event
				switch pool.WaitForSpace(testCtx, 1*time.Second) {
				case WaitForSpaceCancelled:
					return
				case WaitForSpaceTimeout:
					backpressureEvents.Add(1)
					continue
				case WaitForSpaceOK:
				}

				// Simulate HTTP/2 event read latency
				select {
				case <-time.After(cfg.subscriptionDelay):
				case <-testCtx.Done():
					return
				}

				// Acquire pool space for this batch
				batchBytes := cfg.recordsPerBatch * cfg.recordSizeBytes
				if !pool.Acquire(testCtx, cfg.recordsPerBatch, batchBytes) {
					return
				}
				totalProduced.Add(int64(cfg.recordsPerBatch))
				totalBytesProduced.Add(int64(batchBytes))

				// Create payloads that occupy real heap memory
				data := make([][]byte, cfg.recordsPerBatch)
				for i := range data {
					buf := make([]byte, cfg.recordSizeBytes)
					data[i] = buf
				}

				// Send to consumer (like recordsChan in real code)
				select {
				case recordsChan <- recordBatch{
					count: cfg.recordsPerBatch,
					bytes: batchBytes,
					data:  data,
				}:
				case <-testCtx.Done():
					pool.Release(cfg.recordsPerBatch, batchBytes)
					return
				}
			}
		}(shard)

		// --- Consumer goroutine (simulates runEFOConsumer main loop) ---
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()

			var pendingCount, pendingBytes int
			var pendingData [][]byte
			var batcherCount, batcherBytes int
			var batcherData [][]byte

			cleanup := func() {
				total := pendingCount + batcherCount
				totalB := pendingBytes + batcherBytes
				if total > 0 {
					pool.Release(total, totalB)
				}
			}

			for {
				// Try to receive more records (non-blocking if we have pending work)
				if pendingCount == 0 {
					// Block waiting for records (like select on recordsChan)
					batch, ok := <-recordsChan
					if !ok {
						cleanup()
						return
					}
					pendingCount += batch.count
					pendingBytes += batch.bytes
					pendingData = append(pendingData, batch.data...)
				}

				// Move pending → batcher (like AddRecord loop)
				toMove := pendingCount
				if toMove > cfg.batchSize-batcherCount {
					toMove = cfg.batchSize - batcherCount
				}
				if toMove > 0 {
					moveBytes := toMove * cfg.recordSizeBytes

					if cfg.mode == releaseEarly {
						// OLD behaviour: release pool space immediately
						pool.Release(toMove, moveBytes)
					}

					batcherCount += toMove
					batcherBytes += moveBytes
					batcherData = append(batcherData, pendingData[:toMove]...)
					pendingCount -= toMove
					pendingBytes -= moveBytes
					pendingData = pendingData[toMove:]
				}

				// Flush batcher when full (like batch policy count trigger)
				if batcherCount >= cfg.batchSize {
					batch := flushedBatch{
						count: batcherCount,
						bytes: batcherBytes,
						data:  batcherData,
					}
					if cfg.mode == releaseOnAck {
						// End-to-end: pool released by output consumer on ack
						rc, rb := batcherCount, batcherBytes
						batch.ackFn = func() { pool.Release(rc, rb) }
					}
					select {
					case msgChan <- batch:
						if cfg.mode == releaseDeferred {
							pool.Release(batcherCount, batcherBytes)
						}
						batcherCount = 0
						batcherBytes = 0
						batcherData = nil
					case <-testCtx.Done():
						cleanup()
						return
					}
				}

				// Drain any buffered records from the subscription (non-blocking)
				drained := true
				for drained {
					select {
					case batch, ok := <-recordsChan:
						if !ok {
							// Subscription closed — flush remaining and exit
							if batcherCount > 0 {
								fb := flushedBatch{count: batcherCount, bytes: batcherBytes, data: batcherData}
								if cfg.mode == releaseOnAck {
									rc, rb := batcherCount, batcherBytes
									fb.ackFn = func() { pool.Release(rc, rb) }
								}
								select {
								case msgChan <- fb:
									if cfg.mode == releaseDeferred {
										pool.Release(batcherCount, batcherBytes)
									}
								default:
								}
								batcherCount = 0
								batcherBytes = 0
								batcherData = nil
							}
							cleanup()
							return
						}
						pendingCount += batch.count
						pendingBytes += batch.bytes
						pendingData = append(pendingData, batch.data...)
					default:
						drained = false
					}
				}
			}
		}(shard)
	}

	// Wait for test duration then collect results
	<-testCtx.Done()
	shardWg.Wait()
	close(msgChan)
	outputWg.Wait()

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	res.recordsProduced = totalProduced.Load()
	res.recordsConsumed = totalConsumed.Load()
	res.bytesProduced = totalBytesProduced.Load()
	res.bytesConsumed = totalBytesConsumed.Load()
	res.backpressureCount = backpressureEvents.Load()
	res.peakPoolRecords = peakPoolRecords.Load()
	res.peakPoolBytes = peakPoolBytes.Load()
	res.peakHeapAlloc = peakAlloc.Load()
	res.finalHeapAlloc = finalMem.Alloc
	res.totalHeapAlloc = finalMem.TotalAlloc
	res.numGC = finalMem.NumGC

	return res
}

func logResult(t *testing.T, label string, cfg efoLoadTestConfig, res efoLoadTestResult, duration time.Duration) {
	t.Helper()
	t.Logf("=== %s (%d shards, %s) ===", label, cfg.numShards, duration)
	t.Logf("Pool config: max_pending_records=%d, max_pending_bytes=%d KB",
		cfg.maxPendingRecords, cfg.maxPendingBytes/1024)
	t.Logf("Record config: %d records/batch × %d bytes, batch_size=%d, output_latency=%s",
		cfg.recordsPerBatch, cfg.recordSizeBytes, cfg.batchSize, cfg.outputLatency)
	t.Logf("")
	t.Logf("Records produced:    %d", res.recordsProduced)
	t.Logf("Records consumed:    %d", res.recordsConsumed)
	t.Logf("Bytes produced:      %d MB", res.bytesProduced/(1024*1024))
	t.Logf("Bytes consumed:      %d MB", res.bytesConsumed/(1024*1024))
	t.Logf("")
	t.Logf("Peak pool records:   %d / %d", res.peakPoolRecords, cfg.maxPendingRecords)
	t.Logf("Peak pool bytes:     %d KB / %d KB", res.peakPoolBytes/1024, cfg.maxPendingBytes/1024)
	t.Logf("Backpressure events: %d", res.backpressureCount)
	t.Logf("")
	t.Logf("Peak heap alloc:     %d MB", res.peakHeapAlloc/(1024*1024))
	t.Logf("Final heap alloc:    %d MB", res.finalHeapAlloc/(1024*1024))
	t.Logf("Total heap alloc:    %d MB", res.totalHeapAlloc/(1024*1024))
	t.Logf("Num GC cycles:       %d", res.numGC)
}

// TestEFOLoadSimulation_EarlyVsDeferred runs the same workload under both
// release strategies and compares peak memory. This demonstrates the memory
// leak in the early-release approach.
//
// Run with:
//
//	go test ./internal/impl/aws/... -run TestEFOLoadSimulation_EarlyVsDeferred -v -timeout 120s
func TestEFOLoadSimulation_EarlyVsDeferred(t *testing.T) {
	duration := 10 * time.Second

	baseCfg := efoLoadTestConfig{
		numShards:         60,
		maxPendingRecords: 1000,
		maxPendingBytes:   5 * 1024 * 1024, // 5 MB
		recordsPerBatch:   50,              // EFO event batch size
		recordSizeBytes:   1024,            // 1 KB records
		batchSize:         200,             // flush every 200 records (accumulates 4 batches)
		shardStartupDelay: 10 * time.Millisecond,
		outputLatency:     20 * time.Millisecond, // slow output to cause batcher accumulation
		subscriptionDelay: 2 * time.Millisecond,  // fast subscription delivery
	}

	// Force GC before each run for clean baseline
	runtime.GC()

	t.Run("early_release", func(t *testing.T) {
		cfg := baseCfg
		cfg.mode = releaseEarly
		res := runEFOLoadTest(t, cfg, duration)
		logResult(t, "EARLY RELEASE (old behaviour)", cfg, res, duration)

		// Pool accounting will show low values because space was released
		// before data left memory — but heap tells the real story.
		t.Logf("")
		t.Logf("NOTE: Pool shows low usage, but heap is high because records")
		t.Logf("      sit in batcher/msgChan after pool space was released.")
	})

	runtime.GC()

	t.Run("deferred_release", func(t *testing.T) {
		cfg := baseCfg
		cfg.mode = releaseDeferred
		res := runEFOLoadTest(t, cfg, duration)
		logResult(t, "DEFERRED RELEASE (mid)", cfg, res, duration)

		require.LessOrEqual(t, res.peakPoolBytes, int64(cfg.maxPendingBytes),
			"pool bytes exceeded maximum")
	})

	runtime.GC()

	t.Run("release_on_ack", func(t *testing.T) {
		cfg := baseCfg
		cfg.mode = releaseOnAck
		res := runEFOLoadTest(t, cfg, duration)
		logResult(t, "RELEASE ON ACK (end-to-end)", cfg, res, duration)

		// With end-to-end tracking, pool should reflect ALL in-flight data
		// including data in the output pipeline
		require.LessOrEqual(t, res.peakPoolBytes, int64(cfg.maxPendingBytes),
			"pool bytes exceeded maximum")
	})
}

// TestEFOLoadSimulation_LargeRecords_EarlyVsDeferred tests with 100KB records
// where the byte limit should be the binding constraint, not record count.
func TestEFOLoadSimulation_LargeRecords_EarlyVsDeferred(t *testing.T) {
	duration := 5 * time.Second

	baseCfg := efoLoadTestConfig{
		numShards:         60,
		maxPendingRecords: 1000,
		maxPendingBytes:   5 * 1024 * 1024, // 5 MB
		recordsPerBatch:   10,              // fewer but larger
		recordSizeBytes:   100 * 1024,      // 100 KB per record
		batchSize:         50,              // flush every 50 records (5 MB per flush)
		shardStartupDelay: 5 * time.Millisecond,
		outputLatency:     30 * time.Millisecond, // slow output
		subscriptionDelay: 2 * time.Millisecond,
	}

	runtime.GC()

	t.Run("early_release", func(t *testing.T) {
		cfg := baseCfg
		cfg.mode = releaseEarly
		res := runEFOLoadTest(t, cfg, duration)
		logResult(t, "EARLY RELEASE - Large Records", cfg, res, duration)
	})

	runtime.GC()

	t.Run("deferred_release", func(t *testing.T) {
		cfg := baseCfg
		cfg.mode = releaseDeferred
		res := runEFOLoadTest(t, cfg, duration)
		logResult(t, "DEFERRED RELEASE - Large Records", cfg, res, duration)

		require.LessOrEqual(t, res.peakPoolBytes, int64(cfg.maxPendingBytes),
			"pool bytes exceeded maximum")
	})
}

// TestEFOLoadSimulation_WithReservation simulates the full reservation lifecycle:
// Acquire reservation → subscribe → read event → ConvertReservation → send records →
// re-acquire reservation for next event. Compares with the deferred-release
// baseline (no reservation) to show how reservations limit concurrent readers
// and bound memory.
func TestEFOLoadSimulation_WithReservation(t *testing.T) {
	const (
		numShards             = 60
		maxPendingRecords     = 50000
		maxPendingBytes       = 200 * 1024 * 1024 // 200 MB
		initialReservation    = 50 * 1024 * 1024   // 50 MB → ~4 concurrent shards
		recordsPerBatch       = 50
		recordSizeBytes       = 10 * 1024 // 10 KB per record → 500 KB per batch
		batchSize             = 200
		testDuration          = 10 * time.Second
		outputLatency         = 20 * time.Millisecond
		subscriptionDelay     = 2 * time.Millisecond
	)

	pool := newGlobalPendingPool(maxPendingRecords, maxPendingBytes)
	avg := newEventSizeAverage(0.1)

	ctx, cancel := context.WithTimeout(context.Background(), testDuration+5*time.Second)
	defer cancel()

	var (
		totalProduced, totalConsumed       atomic.Int64
		totalBytesProduced, totalBytesConsumed atomic.Int64
		backpressureEvents                 atomic.Int64
		peakPoolBytes, peakPoolRecords     atomic.Int64
		peakAlloc                          atomic.Int64
		peakConcurrentReaders              atomic.Int32
		currentReaders                     atomic.Int32
	)

	// Peak trackers
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curB := int64(pool.CurrentBytes())
				curR := int64(pool.Current())
				for {
					old := peakPoolBytes.Load()
					if curB <= old || peakPoolBytes.CompareAndSwap(old, curB) {
						break
					}
				}
				for {
					old := peakPoolRecords.Load()
					if curR <= old || peakPoolRecords.CompareAndSwap(old, curR) {
						break
					}
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				alloc := int64(m.Alloc)
				for {
					old := peakAlloc.Load()
					if alloc <= old || peakAlloc.CompareAndSwap(old, alloc) {
						break
					}
				}
			}
		}
	}()

	type flushedBatch struct {
		count int
		bytes int
		data  [][]byte
	}
	msgChan := make(chan flushedBatch, 100)

	var outputWg sync.WaitGroup
	outputWg.Add(1)
	go func() {
		defer outputWg.Done()
		for batch := range msgChan {
			totalConsumed.Add(int64(batch.count))
			totalBytesConsumed.Add(int64(batch.bytes))
			_ = batch.data
			time.Sleep(outputLatency)
		}
	}()

	testCtx, testCancel := context.WithTimeout(ctx, testDuration)
	defer testCancel()

	type recordBatch struct {
		count int
		bytes int
		data  [][]byte
	}

	var shardWg sync.WaitGroup
	for shard := range numShards {
		recordsChan := make(chan recordBatch, 1)

		// --- Subscription goroutine (simulates efoSubscribeAndStream with reservation) ---
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()
			defer close(recordsChan)

			for {
				select {
				case <-testCtx.Done():
					return
				default:
				}

				// Determine reservation size (adaptive)
				reservation := initialReservation
				if a := avg.Get(); a > 0 {
					reservation = a
				}

				// Acquire reservation before "subscribing"
				if !pool.Acquire(testCtx, 0, reservation) {
					return // context cancelled
				}

				// Track concurrent readers
				cur := currentReaders.Add(1)
				for {
					old := peakConcurrentReaders.Load()
					if cur <= old || peakConcurrentReaders.CompareAndSwap(old, cur) {
						break
					}
				}

				// Simulate reading events from one subscription (up to 5 events per sub)
				for event := range 5 {
					_ = event
					select {
					case <-testCtx.Done():
						currentReaders.Add(-1)
						if reservation > 0 {
							pool.Release(0, reservation)
						}
						return
					case <-time.After(subscriptionDelay):
					}

					batchBytes := recordsPerBatch * recordSizeBytes

					// Update global average
					avg.Update(batchBytes)

					// ConvertReservation: swap reservation for actual bytes
					if reservation > 0 {
						pool.ConvertReservation(recordsPerBatch, batchBytes, reservation)
						reservation = 0
					} else {
						// No reservation for subsequent events — acquire normally
						if !pool.Acquire(testCtx, recordsPerBatch, batchBytes) {
							currentReaders.Add(-1)
							return
						}
					}
					totalProduced.Add(int64(recordsPerBatch))
					totalBytesProduced.Add(int64(batchBytes))

					// Create heap-live payloads
					data := make([][]byte, recordsPerBatch)
					for i := range data {
						data[i] = make([]byte, recordSizeBytes)
					}

					select {
					case recordsChan <- recordBatch{count: recordsPerBatch, bytes: batchBytes, data: data}:
					case <-testCtx.Done():
						pool.Release(recordsPerBatch, batchBytes)
						currentReaders.Add(-1)
						return
					}

					// Re-acquire reservation for next event
					nextReservation := initialReservation
					if a := avg.Get(); a > 0 {
						nextReservation = a
					}
					if !pool.Acquire(testCtx, 0, nextReservation) {
						currentReaders.Add(-1)
						return
					}
					reservation = nextReservation
				}

				// Subscription ends — release reservation
				currentReaders.Add(-1)
				if reservation > 0 {
					pool.Release(0, reservation)
				}
			}
		}(shard)

		// --- Consumer goroutine ---
		shardWg.Add(1)
		go func(shardID int) {
			defer shardWg.Done()
			var batcherCount, batcherBytes int
			var batcherData [][]byte

			cleanup := func() {
				if batcherCount > 0 {
					pool.Release(batcherCount, batcherBytes)
					batcherCount = 0
					batcherBytes = 0
				}
			}

			for batch := range recordsChan {
				batcherCount += batch.count
				batcherBytes += batch.bytes
				batcherData = append(batcherData, batch.data...)

				if batcherCount >= batchSize {
					select {
					case msgChan <- flushedBatch{count: batcherCount, bytes: batcherBytes, data: batcherData}:
						pool.Release(batcherCount, batcherBytes)
						batcherCount = 0
						batcherBytes = 0
						batcherData = nil
					case <-testCtx.Done():
						cleanup()
						// Drain remaining records from channel
						for leftover := range recordsChan {
							pool.Release(leftover.count, leftover.bytes)
						}
						return
					}
				}
			}
			// Flush remaining
			if batcherCount > 0 {
				select {
				case msgChan <- flushedBatch{count: batcherCount, bytes: batcherBytes, data: batcherData}:
					pool.Release(batcherCount, batcherBytes)
				default:
					pool.Release(batcherCount, batcherBytes)
				}
			}
		}(shard)
	}

	<-testCtx.Done()
	shardWg.Wait()
	close(msgChan)
	outputWg.Wait()

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	t.Logf("=== RESERVATION-BASED BACKPRESSURE (%d shards, %s) ===", numShards, testDuration)
	t.Logf("Pool config: max_pending_bytes=%d MB, initial_reservation=%d MB",
		maxPendingBytes/(1024*1024), initialReservation/(1024*1024))
	t.Logf("Record config: %d records/batch × %d KB = %d KB/batch",
		recordsPerBatch, recordSizeBytes/1024, recordsPerBatch*recordSizeBytes/1024)
	t.Logf("")
	t.Logf("Records produced:       %d", totalProduced.Load())
	t.Logf("Records consumed:       %d", totalConsumed.Load())
	t.Logf("Bytes produced:         %d MB", totalBytesProduced.Load()/(1024*1024))
	t.Logf("Bytes consumed:         %d MB", totalBytesConsumed.Load()/(1024*1024))
	t.Logf("")
	t.Logf("Peak pool bytes:        %d MB / %d MB", peakPoolBytes.Load()/(1024*1024), maxPendingBytes/(1024*1024))
	t.Logf("Peak pool records:      %d / %d", peakPoolRecords.Load(), maxPendingRecords)
	t.Logf("Backpressure events:    %d", backpressureEvents.Load())
	t.Logf("Peak concurrent readers: %d (expected ~%d)", peakConcurrentReaders.Load(), maxPendingBytes/initialReservation)
	t.Logf("Final avg event size:   %d KB (from %d observations)", avg.Get()/1024, avg.Count())
	t.Logf("")
	t.Logf("Peak heap alloc:        %d MB", peakAlloc.Load()/(1024*1024))
	t.Logf("Final heap alloc:       %d MB", finalMem.Alloc/(1024*1024))
	t.Logf("Total heap alloc:       %d MB", finalMem.TotalAlloc/(1024*1024))
	t.Logf("Num GC cycles:          %d", finalMem.NumGC)

	require.LessOrEqual(t, peakPoolBytes.Load(), int64(maxPendingBytes),
		"pool bytes exceeded maximum")
	require.Equal(t, 0, pool.Current(), "pool should be empty after test")
	require.Equal(t, 0, pool.CurrentBytes(), "pool bytes should be zero after test")
}

// makeTestRecords creates a batch of Kinesis records with the given size for testing.
func makeTestRecords(count, sizeBytes int, seqStart int) []types.Record {
	payload := make([]byte, sizeBytes)
	_, _ = rand.Read(payload)

	records := make([]types.Record, count)
	for i := range records {
		seq := fmt.Sprintf("%012d", seqStart+i)
		pk := fmt.Sprintf("pk-%d", i)
		records[i] = types.Record{
			Data:           payload,
			SequenceNumber: aws.String(seq),
			PartitionKey:   aws.String(pk),
		}
	}
	return records
}
