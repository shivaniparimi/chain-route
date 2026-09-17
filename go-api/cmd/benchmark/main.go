// go-api/cmd/benchmark/main.go
//
// Reproducible, network-free-execution load-test for ChainRoute's
// payment pipeline. Defines "processed" as reaching a terminal payment
// status (COMPLETED or FAILED) observed via GET /payments/{id} -- the
// full HTTP -> DB -> outbox -> Kafka -> worker -> terminal-state pipeline,
// not merely HTTP-accept. Reports both accept-latency and end-to-end
// latency so the distinction is never hidden (Phase 10 design doc §8).
//
// This benchmark ONLY exercises execution_mode=simulated (never testnet)
// -- it must never spend real testnet funds.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	acceptLatency time.Duration
	e2eLatency    time.Duration
	success       bool
}

type benchmarkOutput struct {
	Timestamp           time.Time     `json:"timestamp"`
	DurationSeconds     float64       `json:"duration_seconds"`
	Concurrency         int           `json:"concurrency"`
	BaseURL             string        `json:"base_url"`
	Hardware            hardwareInfo  `json:"hardware"`
	TotalRequests       int64         `json:"total_requests"`
	Successful          int64         `json:"successful"`
	Failed              int64         `json:"failed"`
	ThroughputAccept    float64       `json:"throughput_accept_per_sec"`
	ThroughputProcessed float64       `json:"throughput_processed_per_sec"`
	AcceptLatencyMs     latencyReport `json:"accept_latency_ms"`
	E2ELatencyMs        latencyReport `json:"e2e_latency_ms"`
}

type hardwareInfo struct {
	NumCPU int    `json:"num_cpu"`
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
}

type latencyReport struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// percentile returns the nearest-rank percentile p (0.0-1.0) of durations.
// It does not mutate the input slice.
func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted))*p+0.9999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	concurrency := flag.Int("concurrency", runtime.GOMAXPROCS(0), "number of concurrent workers")
	baseURL := flag.String("base-url", "http://localhost:8099", "ChainRoute server base URL")
	outPath := flag.String("out", "", "path to write machine-readable JSON result (optional)")
	flag.Parse()

	fmt.Printf("ChainRoute payment pipeline benchmark\n")
	fmt.Printf("  duration=%s concurrency=%d base_url=%s\n", *duration, *concurrency, *baseURL)
	fmt.Printf("  mode: simulated (network-free execution; no real blockchain spending)\n\n")

	client := &http.Client{Timeout: 15 * time.Second}
	start := time.Now()
	stop := start.Add(*duration)

	var total, successful, failed int64
	resultsCh := make(chan result, 10000)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			seq := 0
			for time.Now().Before(stop) {
				seq++
				idempotencyKey := fmt.Sprintf("bench-%d-%d-%d", workerID, seq, time.Now().UnixNano())
				r, ok := runOnePayment(client, *baseURL, idempotencyKey)
				atomic.AddInt64(&total, 1)
				if ok {
					atomic.AddInt64(&successful, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
				resultsCh <- r
			}
		}(w)
	}

	// Drain resultsCh concurrently with the workers via a collector
	// goroutine started BEFORE wg.Wait(). The channel is buffered
	// (10000) purely to smooth bursts between producers and this
	// collector; drainage must not depend on that buffer being large
	// enough to hold every result from the whole run. If the collector
	// only started reading after wg.Wait() (i.e. after all workers
	// finished), a long/high-throughput run producing more than 10000
	// results before workers stop would fill the buffer and every
	// worker would block forever on `resultsCh <- r`, while wg.Wait()
	// itself blocks forever waiting for those same workers -- a
	// deadlock. Starting the reader now means resultsCh is continuously
	// drained for the whole run, so the buffer can never fill.
	var acceptLatencies, e2eLatencies []time.Duration
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for r := range resultsCh {
			acceptLatencies = append(acceptLatencies, r.acceptLatency)
			if r.success {
				e2eLatencies = append(e2eLatencies, r.e2eLatency)
			}
		}
	}()

	wg.Wait()
	close(resultsCh)
	collectWg.Wait()

	elapsed := time.Since(start).Seconds()
	out := benchmarkOutput{
		Timestamp: time.Now().UTC(), DurationSeconds: elapsed, Concurrency: *concurrency, BaseURL: *baseURL,
		Hardware:      hardwareInfo{NumCPU: runtime.NumCPU(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH},
		TotalRequests: total, Successful: successful, Failed: failed,
		ThroughputAccept:    float64(total) / elapsed,
		ThroughputProcessed: float64(successful) / elapsed,
		AcceptLatencyMs: latencyReport{
			P50: float64(percentile(acceptLatencies, 0.50).Microseconds()) / 1000,
			P95: float64(percentile(acceptLatencies, 0.95).Microseconds()) / 1000,
			P99: float64(percentile(acceptLatencies, 0.99).Microseconds()) / 1000,
		},
		E2ELatencyMs: latencyReport{
			P50: float64(percentile(e2eLatencies, 0.50).Microseconds()) / 1000,
			P95: float64(percentile(e2eLatencies, 0.95).Microseconds()) / 1000,
			P99: float64(percentile(e2eLatencies, 0.99).Microseconds()) / 1000,
		},
	}

	fmt.Printf("Total requests:        %d\n", out.TotalRequests)
	fmt.Printf("Successful (processed):%d\n", out.Successful)
	fmt.Printf("Failed:                %d\n", out.Failed)
	fmt.Printf("Throughput (accept):   %.2f req/sec\n", out.ThroughputAccept)
	fmt.Printf("Throughput (processed):%.2f payments/sec\n", out.ThroughputProcessed)
	fmt.Printf("Accept latency:  p50=%.1fms p95=%.1fms p99=%.1fms\n", out.AcceptLatencyMs.P50, out.AcceptLatencyMs.P95, out.AcceptLatencyMs.P99)
	fmt.Printf("E2E latency:     p50=%.1fms p95=%.1fms p99=%.1fms\n", out.E2ELatencyMs.P50, out.E2ELatencyMs.P95, out.E2ELatencyMs.P99)

	if *outPath != "" {
		data, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal result: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write result to %s: %v\n", *outPath, err)
			os.Exit(1)
		}
		fmt.Printf("\nMachine-readable result written to %s\n", *outPath)
	}
}

// runOnePayment posts one simulated-mode payment and polls until terminal
// (COMPLETED/FAILED) or a bounded number of retries elapses. success=true
// only if the payment reached COMPLETED -- reaching FAILED is a "failed"
// outcome for the benchmark's own success/failure counters, distinct
// from an HTTP/transport error, both of which count as failed=true here.
func runOnePayment(client *http.Client, baseURL, idempotencyKey string) (result, bool) {
	body := []byte(`{"source_chain":"ethereum","destination_chain":"base","asset":"usdc","amount":"100"}`)
	acceptStart := time.Now()
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/payments", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := client.Do(req)
	acceptLatency := time.Since(acceptStart)
	if err != nil || (resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK) {
		if resp != nil {
			resp.Body.Close()
		}
		return result{acceptLatency: acceptLatency, success: false}, false
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()

	e2eStart := time.Now()
	for i := 0; i < 100; i++ {
		time.Sleep(50 * time.Millisecond)
		getResp, err := client.Get(baseURL + "/payments/" + created.ID)
		if err != nil {
			continue
		}
		var payment struct {
			Status string `json:"status"`
		}
		_ = json.NewDecoder(getResp.Body).Decode(&payment)
		getResp.Body.Close()
		if payment.Status == "COMPLETED" {
			return result{acceptLatency: acceptLatency, e2eLatency: time.Since(e2eStart), success: true}, true
		}
		if payment.Status == "FAILED" {
			return result{acceptLatency: acceptLatency, e2eLatency: time.Since(e2eStart), success: false}, false
		}
	}
	return result{acceptLatency: acceptLatency, success: false}, false
}
