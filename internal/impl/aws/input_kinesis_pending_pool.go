package aws

import (
	"context"
	"sync"
	"time"
)

// globalPendingPool limits the total number of pending records across all shards.
// Each shard must acquire space from this pool before accepting records from Kinesis,
// ensuring bounded memory usage regardless of shard count.
type globalPendingPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	current int
	max     int
}

// newGlobalPendingPool creates a new pool with the specified maximum capacity.
func newGlobalPendingPool(max int) *globalPendingPool {
	p := &globalPendingPool{
		max: max,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire acquires space for count records, blocking if necessary until space is available.
// Returns immediately if ctx is cancelled.
func (p *globalPendingPool) Acquire(ctx context.Context, count int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.current+count > p.max {
		// Check if context is cancelled before waiting
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Wait for space to become available
		// We need to release the lock while waiting, so use a channel-based approach
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			p.mu.Lock()
			return false
		case <-time.After(10 * time.Millisecond): // Poll periodically
			p.mu.Lock()
		}
	}
	p.current += count
	return true
}

// WaitForSpace blocks until there is any space available in the pool.
// This is used to apply backpressure before fetching new data from Kinesis.
// Returns false if the context is cancelled.
func (p *globalPendingPool) WaitForSpace(ctx context.Context) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.current >= p.max {
		// Check if context is cancelled before waiting
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Wait for space to become available
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			p.mu.Lock()
			return false
		case <-time.After(10 * time.Millisecond): // Poll periodically
			p.mu.Lock()
		}
	}
	return true
}

// Release returns count records worth of space back to the pool.
func (p *globalPendingPool) Release(count int) {
	p.mu.Lock()
	p.current -= count
	if p.current < 0 {
		p.current = 0
	}
	p.cond.Broadcast() // Wake up any waiting goroutines
	p.mu.Unlock()
}

// Current returns the current number of records in the pool (for monitoring/debugging).
func (p *globalPendingPool) Current() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}
