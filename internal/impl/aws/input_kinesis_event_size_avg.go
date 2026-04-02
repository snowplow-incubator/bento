package aws

import "sync"

// eventSizeAverage tracks a global exponential moving average of EFO event
// byte sizes across all shards. It is used to adaptively size the per-shard
// byte reservation in the global pending pool, so the reservation tracks
// actual event sizes rather than requiring a static guess.
//
// Thread-safe: all methods may be called concurrently from multiple shard
// goroutines.
type eventSizeAverage struct {
	mu      sync.Mutex
	average float64
	count   int64
	alpha   float64 // EMA smoothing factor (0 < alpha ≤ 1)
}

// newEventSizeAverage creates an EMA tracker with the given smoothing factor.
// Alpha controls how quickly the average responds to new values:
//   - alpha=0.1: slow-moving, smooths out spikes (recommended)
//   - alpha=0.5: responds quickly to changes
//   - alpha=1.0: no smoothing, just tracks the last value
func newEventSizeAverage(alpha float64) *eventSizeAverage {
	return &eventSizeAverage{alpha: alpha}
}

// Update records a new event byte size observation.
func (a *eventSizeAverage) Update(bytes int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.count == 0 {
		a.average = float64(bytes)
	} else {
		a.average = a.alpha*float64(bytes) + (1-a.alpha)*a.average
	}
	a.count++
}

// Get returns the current average event size in bytes, or 0 if no
// observations have been recorded yet.
func (a *eventSizeAverage) Get() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.count == 0 {
		return 0
	}
	return int(a.average)
}

// Count returns the number of observations recorded.
func (a *eventSizeAverage) Count() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count
}
