package aws

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventSizeAverage_Initial(t *testing.T) {
	avg := newEventSizeAverage(0.1)

	assert.Equal(t, 0, avg.Get(), "should return 0 before any observations")
	assert.Equal(t, int64(0), avg.Count())
}

func TestEventSizeAverage_FirstUpdate(t *testing.T) {
	avg := newEventSizeAverage(0.1)

	avg.Update(1000)

	assert.Equal(t, 1000, avg.Get(), "first observation should set the average")
	assert.Equal(t, int64(1), avg.Count())
}

func TestEventSizeAverage_EMA(t *testing.T) {
	avg := newEventSizeAverage(0.5) // alpha=0.5 for easier math

	avg.Update(1000)
	assert.Equal(t, 1000, avg.Get())

	avg.Update(2000)
	// EMA: 0.5 * 2000 + 0.5 * 1000 = 1500
	assert.Equal(t, 1500, avg.Get())

	avg.Update(2000)
	// EMA: 0.5 * 2000 + 0.5 * 1500 = 1750
	assert.Equal(t, 1750, avg.Get())
}

func TestEventSizeAverage_SlowAlpha(t *testing.T) {
	avg := newEventSizeAverage(0.1)

	// Start at 1000
	avg.Update(1000)
	assert.Equal(t, 1000, avg.Get())

	// One big outlier shouldn't move it much
	avg.Update(10000)
	// EMA: 0.1 * 10000 + 0.9 * 1000 = 1900
	assert.Equal(t, 1900, avg.Get())

	// After many more normal values, it should recover
	for range 20 {
		avg.Update(1000)
	}
	// Should converge back toward 1000
	assert.Less(t, avg.Get(), 1200, "should converge back toward 1000 after many normal observations")
}

func TestEventSizeAverage_ConcurrentAccess(t *testing.T) {
	avg := newEventSizeAverage(0.1)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			avg.Update(1000)
			_ = avg.Get()
		})
	}
	wg.Wait()

	require.Equal(t, int64(100), avg.Count())
	// All updates were 1000, so average should be 1000
	assert.Equal(t, 1000, avg.Get())
}
