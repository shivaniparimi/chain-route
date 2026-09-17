package main

import (
	"testing"
	"time"
)

func TestPercentile_ComputesExpectedValues(t *testing.T) {
	durations := []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond,
		40 * time.Millisecond, 50 * time.Millisecond, 60 * time.Millisecond,
		70 * time.Millisecond, 80 * time.Millisecond, 90 * time.Millisecond,
		100 * time.Millisecond,
	}
	p50 := percentile(durations, 0.50)
	p95 := percentile(durations, 0.95)
	p99 := percentile(durations, 0.99)

	if p50 != 50*time.Millisecond {
		t.Errorf("p50 = %v, want 50ms", p50)
	}
	if p95 != 90*time.Millisecond && p95 != 100*time.Millisecond {
		// nearest-rank percentile on a 10-element set for p95 lands on
		// index 9 (0-indexed) with ceil(0.95*10)=10th rank -> the 10th
		// element (index 9) = 100ms under a standard nearest-rank
		// definition; accept either of the two commonly-used
		// conventions rather than pin one arbitrary rounding rule.
		t.Errorf("p95 = %v, want 90ms or 100ms", p95)
	}
	if p99 != 100*time.Millisecond {
		t.Errorf("p99 = %v, want 100ms", p99)
	}
}

func TestPercentile_EmptyInputReturnsZero(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
}
