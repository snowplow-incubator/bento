package aws

import (
	"context"
	"sync"
	"time"
)

// globalPendingPool limits the total number of pending records (and optionally
// bytes) across all shards. Each shard must acquire space from this pool before
// accepting records from Kinesis, ensuring bounded memory usage regardless of
// shard count.
//
// When maxBytes > 0, both the record count limit and the byte limit apply —
// whichever is reached first triggers backpressure. When maxBytes == 0, only
// the record count limit is enforced (backwards compatible).
type globalPendingPool struct {
	mu           sync.Mutex
	cond         *sync.Cond
	current      int
	max          int
	currentBytes int
	maxBytes     int // 0 means byte limit disabled
}

// newGlobalPendingPool creates a new pool with the specified maximum record
// count and optional maximum byte capacity. Set maxBytes to 0 to disable
// byte-level accounting.
func newGlobalPendingPool(maximum, maxBytes int) *globalPendingPool {
	p := &globalPendingPool{
		max:      maximum,
		maxBytes: maxBytes,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// isFull returns true when either limit is saturated (used by WaitForSpace).
func (p *globalPendingPool) isFull() bool {
	if p.current >= p.max {
		return true
	}
	if p.maxBytes > 0 && p.currentBytes >= p.maxBytes {
		return true
	}
	return false
}

// wouldExceed returns true when adding count records of the given byte size
// would exceed either limit (used by Acquire).
func (p *globalPendingPool) wouldExceed(count, bytes int) bool {
	if p.current+count > p.max {
		return true
	}
	if p.maxBytes > 0 && p.currentBytes+bytes > p.maxBytes {
		return true
	}
	return false
}

// canNeverFit returns true when the request can never be satisfied regardless
// of how much space is released.
func (p *globalPendingPool) canNeverFit(count, bytes int) bool {
	if count > p.max {
		return true
	}
	if p.maxBytes > 0 && bytes > p.maxBytes {
		return true
	}
	return false
}

// Acquire acquires space for count records totalling bytes, blocking if
// necessary until space is available. Returns false immediately if the request
// can never be satisfied (count > max or bytes > maxBytes) or if ctx is
// cancelled.
func (p *globalPendingPool) Acquire(ctx context.Context, count, bytes int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If the request can never fit, return false immediately.
	if p.canNeverFit(count, bytes) {
		return false
	}

	// Start a goroutine to handle context cancellation by broadcasting to wake up waiters
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			p.cond.Broadcast()
		case <-done:
		}
	}()

	for p.wouldExceed(count, bytes) {
		// Check if context is cancelled before waiting
		if ctx.Err() != nil {
			return false
		}
		p.cond.Wait()
	}

	// Final check after waking up
	if ctx.Err() != nil {
		return false
	}

	p.current += count
	p.currentBytes += bytes
	return true
}

// WaitForSpaceResult indicates the outcome of WaitForSpace.
type WaitForSpaceResult int

const (
	// WaitForSpaceOK indicates space is available.
	WaitForSpaceOK WaitForSpaceResult = iota
	// WaitForSpaceCancelled indicates the context was cancelled.
	WaitForSpaceCancelled
	// WaitForSpaceTimeout indicates the timeout was reached while waiting.
	WaitForSpaceTimeout
)

// WaitForSpace blocks until there is any space available in the pool.
// This is used to apply backpressure before fetching new data from Kinesis.
// Returns WaitForSpaceOK if space is available, WaitForSpaceCancelled if
// context is cancelled, or WaitForSpaceTimeout if the timeout is reached.
// A timeout of 0 means no timeout (wait indefinitely).
func (p *globalPendingPool) WaitForSpace(ctx context.Context, timeout time.Duration) WaitForSpaceResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Set up deadline if timeout is specified
	var deadline time.Time
	var timer *time.Timer
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	// Start a goroutine to handle context cancellation and timeout by broadcasting
	done := make(chan struct{})
	defer close(done)
	go func() {
		if timer != nil {
			select {
			case <-ctx.Done():
				p.cond.Broadcast()
			case <-timer.C:
				p.cond.Broadcast()
			case <-done:
			}
		} else {
			select {
			case <-ctx.Done():
				p.cond.Broadcast()
			case <-done:
			}
		}
	}()

	for p.isFull() {
		// Check if context is cancelled before waiting
		if ctx.Err() != nil {
			return WaitForSpaceCancelled
		}

		// Check if we've exceeded the timeout
		if timeout > 0 && time.Now().After(deadline) {
			return WaitForSpaceTimeout
		}

		p.cond.Wait()
	}

	// Final checks after waking up
	if ctx.Err() != nil {
		return WaitForSpaceCancelled
	}
	if timeout > 0 && time.Now().After(deadline) {
		return WaitForSpaceTimeout
	}

	return WaitForSpaceOK
}

// Release returns count records and bytes worth of space back to the pool.
func (p *globalPendingPool) Release(count, bytes int) {
	p.mu.Lock()
	p.current -= count
	if p.current < 0 {
		p.current = 0
	}
	p.currentBytes -= bytes
	if p.currentBytes < 0 {
		p.currentBytes = 0
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Current returns the current number of records in the pool (for monitoring/debugging).
func (p *globalPendingPool) Current() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

// CurrentBytes returns the current byte count in the pool (for monitoring/debugging).
func (p *globalPendingPool) CurrentBytes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentBytes
}
