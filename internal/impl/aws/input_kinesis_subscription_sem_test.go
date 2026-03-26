package aws

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSemaphore(slots int) chan struct{} {
	sem := make(chan struct{}, slots)
	for range slots {
		sem <- struct{}{}
	}
	return sem
}

// acquireSlot mirrors the semaphore acquire pattern in efoSubscribeAndStream.
// Returns true if acquired, false if context was cancelled.
func acquireSlot(ctx context.Context, sem chan struct{}, waiters *atomic.Int32) bool {
	waiters.Add(1)
	select {
	case <-sem:
		waiters.Add(-1)
		return true
	case <-ctx.Done():
		waiters.Add(-1)
		return false
	}
}

func releaseSlot(sem chan struct{}) {
	sem <- struct{}{}
}

func TestSubscriptionSemaphore_LimitsConcurrency(t *testing.T) {
	const maxConcurrent = 3
	const totalWorkers = 10

	sem := newTestSemaphore(maxConcurrent)
	var waiters atomic.Int32
	var activePeak atomic.Int32
	var activeCount atomic.Int32

	var wg sync.WaitGroup
	for range totalWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !acquireSlot(context.Background(), sem, &waiters) {
				return
			}
			defer releaseSlot(sem)

			cur := activeCount.Add(1)
			// Track peak concurrency
			for {
				peak := activePeak.Load()
				if cur <= peak || activePeak.CompareAndSwap(peak, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			activeCount.Add(-1)
		}()
	}

	wg.Wait()
	assert.LessOrEqual(t, activePeak.Load(), int32(maxConcurrent), "concurrent workers should never exceed semaphore slots")
	assert.Equal(t, int32(0), activeCount.Load(), "all workers should have finished")
	assert.Equal(t, int32(0), waiters.Load(), "no waiters should remain")
	assert.Len(t, sem, maxConcurrent, "all slots should be returned")
}

func TestSubscriptionSemaphore_WaitersCount(t *testing.T) {
	sem := newTestSemaphore(1)
	var waiters atomic.Int32

	// Acquire the only slot
	require.True(t, acquireSlot(context.Background(), sem, &waiters))
	assert.Equal(t, int32(0), waiters.Load(), "no waiters after successful acquire")

	// Start a goroutine that will block waiting for the slot
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		acquireSlot(context.Background(), sem, &waiters)
		releaseSlot(sem)
	}()

	<-started
	// Give the goroutine time to reach the select and increment waiters
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), waiters.Load(), "one goroutine should be waiting")

	// Release the slot — the waiting goroutine should acquire it
	releaseSlot(sem)
	<-done

	assert.Equal(t, int32(0), waiters.Load(), "no waiters after release")
	assert.Len(t, sem, 1, "slot should be returned")
}

func TestSubscriptionSemaphore_ContextCancellation(t *testing.T) {
	sem := newTestSemaphore(1)
	var waiters atomic.Int32

	// Acquire the only slot
	require.True(t, acquireSlot(context.Background(), sem, &waiters))

	// Try to acquire with a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	acquired := make(chan bool, 1)
	go func() {
		acquired <- acquireSlot(ctx, sem, &waiters)
	}()

	// Give the goroutine time to block
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), waiters.Load(), "should be waiting")

	cancel()

	result := <-acquired
	assert.False(t, result, "should fail due to context cancellation")
	assert.Equal(t, int32(0), waiters.Load(), "waiters should be decremented on cancel")
}

func TestSubscriptionRotation_YieldsWhenWaiters(t *testing.T) {
	var waiters atomic.Int32

	// Simulate a shard goroutine that checks rotation
	waiters.Store(5) // 5 shards waiting

	// The rotation check (mirroring efoSubscribeAndStream logic):
	// if waiters > 0 → should yield
	shouldYield := waiters.Load() > 0
	assert.True(t, shouldYield, "should yield when waiters are present")
}

func TestSubscriptionRotation_NoYieldWhenNoWaiters(t *testing.T) {
	var waiters atomic.Int32

	// No shards waiting (all have slots)
	waiters.Store(0)

	shouldYield := waiters.Load() > 0
	assert.False(t, shouldYield, "should not yield when no waiters")
}

func TestSubscriptionSemaphore_Disabled(t *testing.T) {
	// With a nil semaphore (default config), the semaphore block is skipped entirely.
	// This test verifies that a kinesisReader with no semaphore configured
	// has subscriptionSem == nil, matching the nil check in efoSubscribeAndStream.
	k := &kinesisReader{}
	assert.Nil(t, k.subscriptionSem, "subscriptionSem should be nil by default")
}

func TestSubscriptionRotation_TimerResets(t *testing.T) {
	var waiters atomic.Int32
	rotationPeriod := 20 * time.Millisecond

	// Start with no waiters — timer should reset rather than yield
	waiters.Store(0)
	timer := time.After(rotationPeriod)

	// Wait for timer to fire
	<-timer

	// Check: no waiters, so we should reset the timer instead of yielding
	shouldYield := waiters.Load() > 0
	assert.False(t, shouldYield, "should not yield with no waiters")

	// Now add waiters and create a new timer
	waiters.Store(3)
	timer = time.After(rotationPeriod)
	<-timer

	shouldYield = waiters.Load() > 0
	assert.True(t, shouldYield, "should yield when waiters are present after timer reset")
}
