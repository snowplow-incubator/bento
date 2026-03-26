package aws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalPendingPool_AcquireRelease(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Acquire some space
	assert.True(t, pool.Acquire(context.Background(), 50, 0))
	assert.Equal(t, 50, pool.Current())

	// Acquire more space
	assert.True(t, pool.Acquire(context.Background(), 30, 0))
	assert.Equal(t, 80, pool.Current())

	// Release some space
	pool.Release(20, 0)
	assert.Equal(t, 60, pool.Current())

	// Release all
	pool.Release(60, 0)
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_AcquireExceedsMax(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Trying to acquire more than max should fail immediately
	assert.False(t, pool.Acquire(context.Background(), 150, 0))
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_AcquireBlocksUntilSpace(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100, 0))

	// Start a goroutine that will try to acquire more
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 50, 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release some space
	pool.Release(50, 0)

	// The acquire should succeed now
	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after Release")
	}

	assert.Equal(t, 100, pool.Current())
}

func TestGlobalPendingPool_AcquireContextCancellation(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100, 0))

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will try to acquire more
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(ctx, 50, 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// The acquire should fail due to cancellation
	select {
	case result := <-acquired:
		assert.False(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have returned after context cancellation")
	}

	// Pool should still have the original 100
	assert.Equal(t, 100, pool.Current())
}

func TestGlobalPendingPool_WaitForSpace(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// With space available, should return immediately
	result := pool.WaitForSpace(context.Background(), 0)
	assert.Equal(t, WaitForSpaceOK, result)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100, 0))

	// Start a goroutine that will wait for space
	waitResult := make(chan WaitForSpaceResult, 1)
	go func() {
		waitResult <- pool.WaitForSpace(context.Background(), 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release some space
	pool.Release(10, 0)

	// The wait should succeed now
	select {
	case result := <-waitResult:
		assert.Equal(t, WaitForSpaceOK, result)
	case <-time.After(time.Second):
		t.Fatal("WaitForSpace should have returned after Release")
	}
}

func TestGlobalPendingPool_WaitForSpaceTimeout(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100, 0))

	// Wait with a short timeout
	start := time.Now()
	result := pool.WaitForSpace(context.Background(), 100*time.Millisecond)
	elapsed := time.Since(start)

	assert.Equal(t, WaitForSpaceTimeout, result)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.Less(t, elapsed, 200*time.Millisecond) // Should not take too long
}

func TestGlobalPendingPool_WaitForSpaceContextCancellation(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100, 0))

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will wait for space
	waitResult := make(chan WaitForSpaceResult, 1)
	go func() {
		waitResult <- pool.WaitForSpace(ctx, 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// The wait should return cancelled
	select {
	case result := <-waitResult:
		assert.Equal(t, WaitForSpaceCancelled, result)
	case <-time.After(time.Second):
		t.Fatal("WaitForSpace should have returned after context cancellation")
	}
}

func TestGlobalPendingPool_ReleaseBelowZero(t *testing.T) {
	pool := newGlobalPendingPool(100, 0)

	// Release more than current (should clamp to 0)
	pool.Release(50, 0)
	assert.Equal(t, 0, pool.Current())

	// Acquire and release more than acquired
	assert.True(t, pool.Acquire(context.Background(), 30, 0))
	pool.Release(50, 0)
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_ConcurrentAccess(t *testing.T) {
	pool := newGlobalPendingPool(1000, 0)

	var wg sync.WaitGroup
	acquireCount := 100
	acquireSize := 10

	// Start many goroutines acquiring and releasing
	for range acquireCount {
		wg.Go(func() {
			require.True(t, pool.Acquire(context.Background(), acquireSize, 0))
			time.Sleep(10 * time.Millisecond)
			pool.Release(acquireSize, 0)
		})
	}

	wg.Wait()
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_WaitForSpaceVsAcquire(t *testing.T) {
	// Test that WaitForSpace returns as soon as current < max,
	// but Acquire might still need to wait if current + count > max
	pool := newGlobalPendingPool(100, 0)

	// Fill to 90
	assert.True(t, pool.Acquire(context.Background(), 90, 0))

	// WaitForSpace should return OK (there's space)
	result := pool.WaitForSpace(context.Background(), 100*time.Millisecond)
	assert.Equal(t, WaitForSpaceOK, result)

	// But Acquire of 20 should block until more space is released
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 20, 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release 10 more to make room
	pool.Release(10, 0)

	// Now Acquire should succeed
	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after Release")
	}

	assert.Equal(t, 100, pool.Current())
}

// --- Byte-limit tests ---

func TestGlobalPendingPool_ByteLimit_AcquireRelease(t *testing.T) {
	// 1000 records max, 500 bytes max
	pool := newGlobalPendingPool(1000, 500)

	// Acquire 10 records, 200 bytes — should succeed
	assert.True(t, pool.Acquire(context.Background(), 10, 200))
	assert.Equal(t, 10, pool.Current())
	assert.Equal(t, 200, pool.CurrentBytes())

	// Acquire 10 more records, 200 bytes — should succeed (400 total)
	assert.True(t, pool.Acquire(context.Background(), 10, 200))
	assert.Equal(t, 20, pool.Current())
	assert.Equal(t, 400, pool.CurrentBytes())

	// Release first batch
	pool.Release(10, 200)
	assert.Equal(t, 10, pool.Current())
	assert.Equal(t, 200, pool.CurrentBytes())

	// Release all
	pool.Release(10, 200)
	assert.Equal(t, 0, pool.Current())
	assert.Equal(t, 0, pool.CurrentBytes())
}

func TestGlobalPendingPool_ByteLimit_ExceedsMax(t *testing.T) {
	pool := newGlobalPendingPool(1000, 500)

	// Request that can never fit in byte limit
	assert.False(t, pool.Acquire(context.Background(), 1, 600))
	assert.Equal(t, 0, pool.Current())
	assert.Equal(t, 0, pool.CurrentBytes())
}

func TestGlobalPendingPool_ByteLimit_BlocksOnBytes(t *testing.T) {
	pool := newGlobalPendingPool(1000, 500)

	// Fill to byte limit
	assert.True(t, pool.Acquire(context.Background(), 5, 500))

	// Acquire should block (record limit has space, but byte limit is full)
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 1, 100)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release some bytes
	pool.Release(2, 200)

	// The acquire should succeed now
	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after byte release")
	}

	assert.Equal(t, 4, pool.Current())
	assert.Equal(t, 400, pool.CurrentBytes())
}

func TestGlobalPendingPool_ByteLimit_WaitForSpace(t *testing.T) {
	pool := newGlobalPendingPool(1000, 500)

	// Fill byte limit only (record limit has plenty of space)
	assert.True(t, pool.Acquire(context.Background(), 5, 500))

	// WaitForSpace should block because bytes are full
	waitResult := make(chan WaitForSpaceResult, 1)
	go func() {
		waitResult <- pool.WaitForSpace(context.Background(), 0)
	}()

	time.Sleep(50 * time.Millisecond)

	// Release some bytes
	pool.Release(1, 100)

	select {
	case result := <-waitResult:
		assert.Equal(t, WaitForSpaceOK, result)
	case <-time.After(time.Second):
		t.Fatal("WaitForSpace should have returned after byte release")
	}
}

func TestGlobalPendingPool_DualLimit(t *testing.T) {
	// Record limit 10, byte limit 1000
	pool := newGlobalPendingPool(10, 1000)

	// Fill record limit (but byte limit has space)
	assert.True(t, pool.Acquire(context.Background(), 10, 100))

	// Should block due to record limit
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 1, 10)
	}()

	time.Sleep(50 * time.Millisecond)

	pool.Release(1, 10)

	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after record release")
	}
}

func TestGlobalPendingPool_ByteLimit_Disabled(t *testing.T) {
	// maxBytes=0 means byte tracking is disabled
	pool := newGlobalPendingPool(100, 0)

	// Acquire with large byte value — should be ignored
	assert.True(t, pool.Acquire(context.Background(), 10, 999999))
	assert.Equal(t, 10, pool.Current())
	// currentBytes still tracks (for monitoring) even when limit is disabled
	assert.Equal(t, 999999, pool.CurrentBytes())

	pool.Release(10, 999999)
	assert.Equal(t, 0, pool.Current())
	assert.Equal(t, 0, pool.CurrentBytes())
}

func TestGlobalPendingPool_ByteLimit_ReleaseBelowZero(t *testing.T) {
	pool := newGlobalPendingPool(100, 500)

	// Release bytes when empty — should clamp to 0
	pool.Release(10, 100)
	assert.Equal(t, 0, pool.Current())
	assert.Equal(t, 0, pool.CurrentBytes())
}
