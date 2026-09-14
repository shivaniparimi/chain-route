package execution

import "hash/fnv"

// Result is the outcome of a simulated payment execution.
type Result struct {
	Success bool
	Reason  string
}

// Execute is a pure, deterministic stand-in for a real payment executor.
// It has no I/O, no external calls, and no randomness: the same paymentID
// always produces the same Result. This determinism is load-bearing -- it
// is what lets the recovery sweep safely recompute a crashed or stalled
// execution and be certain it reproduces the exact outcome the original
// attempt would have persisted.
func Execute(paymentID string) Result {
	h := fnv.New64a()
	h.Write([]byte(paymentID))
	if h.Sum64()%10 == 0 {
		return Result{Success: false, Reason: "simulated execution failure"}
	}
	return Result{Success: true}
}
