// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/internal/metricstest"
	"github.com/jaegertracing/jaeger/internal/sampling/samplingstrategy/file"
)

func writeStrategiesFile(t *testing.T, path string, probability float64) {
	t.Helper()
	content := fmt.Sprintf(`{
  "default_strategy": {"type": "probabilistic", "param": 1.0},
  "service_strategies": [
    {"service": "bursty-svc", "type": "probabilistic", "param": %v}
  ]
}`, probability)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

// TestSlowStorageBackpressure exercises the whole acceptance scenario
// end to end:
//
//   - a burst of trace traffic from concurrent producers,
//   - a storage backend that is slower than the ingress rate,
//   - a sampling strategy updated mid-flight via a strategies file,
//
// and asserts the four acceptance properties: bounded queue depth (memory),
// strategy updates taking effect, critical traces retained, and every
// accepted batch accounted for (written, shed, or sampled out).
func TestSlowStorageBackpressure(t *testing.T) {
	const (
		capacity  = 20
		workers   = 2
		producers = 4
		batches   = 125 // per producer; every 5th is critical
	)

	strategiesPath := filepath.Join(t.TempDir(), "strategies.json")
	writeStrategiesFile(t, strategiesPath, 1.0)

	provider, err := file.NewProvider(file.Options{
		StrategiesFile: strategiesPath,
		ReloadInterval: 10 * time.Millisecond,
	}, zap.NewNop())
	require.NoError(t, err)
	defer provider.Close()

	// Storage is an order of magnitude slower than the ingress rate.
	storage := &fakeStorage{latency: 10 * time.Millisecond}
	mf := metricstest.NewFactory(0)
	w, err := NewWriter(
		Config{
			Capacity:       capacity,
			Workers:        workers,
			EnqueueTimeout: 2 * time.Second,
		},
		storage,
		Options{
			Logger:         zap.NewNop(),
			Sampler:        NewProviderSampler(provider, 1.0, zap.NewNop()),
			MetricsFactory: mf,
		},
	)
	require.NoError(t, err)

	// Track the maximum observed queue depth while the burst runs.
	var maxDepth atomic.Int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
				if d := int64(w.Depth()); d > maxDepth.Load() {
					maxDepth.Store(d)
				}
			}
		}
	}()

	criticalIDs := make(chan pcommon.TraceID, producers*batches)
	var normalSent atomic.Int64
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range batches {
				critical := i%5 == 0
				id := makeTraceID(byte(p*64 + i))
				err := w.WriteTraces(context.Background(),
					makeTraces("bursty-svc", critical, id))
				if critical {
					// A critical batch may only fail with ErrQueueFull.
					if err == nil {
						criticalIDs <- id
					} else {
						require.ErrorIs(t, err, ErrQueueFull)
					}
				} else {
					normalSent.Add(1)
					require.NoError(t, err)
				}
				// Pace the producers so the mid-burst strategy update
				// lands while traffic is still flowing.
				time.Sleep(time.Millisecond)
			}
		})
	}

	// Mid-burst, drop the sampling probability to 0 for the service.
	time.Sleep(50 * time.Millisecond)
	writeStrategiesFile(t, strategiesPath, 0.0)

	wg.Wait()
	close(done)
	close(criticalIDs)
	require.NoError(t, w.Close())

	// 1. Memory stayed bounded: depth never exceeded the configured capacity.
	assert.LessOrEqual(t, maxDepth.Load(), int64(capacity),
		"queue depth must never exceed capacity")
	assert.Positive(t, maxDepth.Load(), "queue should have buffered under slow storage")

	counters, _ := mf.Snapshot()

	// 2. The strategy update took effect: after the file switched to 0%,
	// non-critical spans were sampled out.
	sampledOut := counters["writequeue.sampled_out_spans"]
	assert.Positive(t, sampledOut,
		"strategy update to 0%% must sample out later non-critical spans")

	// 3. Every accepted critical trace was retained, despite the full queue
	// and the 0% sampling strategy (critical batches bypass sampling and
	// are never shed).
	storedIDs := storage.traceIDs()
	criticalAccepted := 0
	for id := range criticalIDs {
		criticalAccepted++
		assert.True(t, storedIDs[id], "critical trace %x must be persisted", id)
	}
	assert.Positive(t, criticalAccepted, "the burst must contain critical batches")

	// 4. Bookkeeping adds up: every non-critical batch was either written,
	// shed at a full queue, or sampled out. Each batch carries one span.
	shed := counters["writequeue.dropped|reason=queue_full"]
	normalWritten := int64(storage.spanCount()) - int64(criticalAccepted)
	assert.Equal(t, normalSent.Load(), normalWritten+shed+sampledOut,
		"written + shed + sampled-out must account for every non-critical batch")
}

// TestStrategyUpdateRestoresSampling verifies that raising the probability
// back to 1 takes effect promptly: traffic is admitted again within a
// bounded time after the strategies file is updated.
func TestStrategyUpdateRestoresSampling(t *testing.T) {
	strategiesPath := filepath.Join(t.TempDir(), "strategies.json")
	require.NoError(t, os.WriteFile(strategiesPath, []byte(
		`{"default_strategy": {"type": "probabilistic", "param": 0.0}}`), 0o600))

	provider, err := file.NewProvider(file.Options{
		StrategiesFile: strategiesPath,
		ReloadInterval: 10 * time.Millisecond,
	}, zap.NewNop())
	require.NoError(t, err)
	defer provider.Close()

	storage := &fakeStorage{}
	w, err := NewWriter(
		Config{Capacity: 64, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), Sampler: NewProviderSampler(provider, 0, zap.NewNop())},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// 0% strategy: nothing is admitted.
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 0 }, time.Second, time.Millisecond)
	assert.Equal(t, 0, storage.spanCount(), "0%% strategy must sample everything out")

	// Update the strategy to 100% and confirm new traffic is admitted promptly.
	require.NoError(t, os.WriteFile(strategiesPath, []byte(
		`{"default_strategy": {"type": "probabilistic", "param": 1.0}}`), 0o600))
	require.Eventually(t, func() bool {
		err := w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(3)))
		if err != nil {
			return false
		}
		return storage.spanCount() > 0
	}, 5*time.Second, 20*time.Millisecond,
		"updated strategy must admit new traffic promptly")
}
