// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/internal/metricstest"
)

func newTestWriter(t *testing.T, cfg Config, storage *fakeStorage) *Writer {
	t.Helper()
	w, err := NewWriter(cfg, storage, Options{Logger: zap.NewNop()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	return w
}

func TestNewWriterNilStorage(t *testing.T) {
	_, err := NewWriter(Config{}, nil, Options{})
	require.ErrorContains(t, err, "next storage writer must not be nil")
}

func TestNewWriterInvalidConfig(t *testing.T) {
	_, err := NewWriter(Config{Capacity: -5}, &fakeStorage{}, Options{})
	require.ErrorContains(t, err, "invalid configuration")
	require.ErrorContains(t, err, "Capacity")
}

func TestWriteAndDrainOnClose(t *testing.T) {
	storage := &fakeStorage{}
	// Capacity exceeds the number of batches so the queue never fills and
	// nothing is shed.
	w := newTestWriter(t, Config{Capacity: 64, Workers: 2}, storage)

	const batches = 50
	for i := range batches {
		err := w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(byte(i))))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	assert.Equal(t, batches, storage.spanCount())
}

func TestWriteAfterClose(t *testing.T) {
	storage := &fakeStorage{}
	w := newTestWriter(t, Config{Capacity: 1, Workers: 1}, storage)
	require.NoError(t, w.Close())

	err := w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1)))
	require.ErrorIs(t, err, ErrWriterClosed)
	assert.Contains(t, err.Error(), "1 spans")

	// Double Close is a no-op.
	require.NoError(t, w.Close())
}

// TestQueueFullShedsNonCritical fills the queue behind a blocked storage and
// verifies that further non-critical batches are shed (acked and counted),
// keeping memory bounded at the configured capacity.
func TestQueueFullShedsNonCritical(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	storage := &fakeStorage{gate: gate, started: started}
	mf := metricstest.NewFactory(0)
	w, err := NewWriter(
		Config{Capacity: 1, Workers: 1, EnqueueTimeout: 50 * time.Millisecond},
		storage,
		Options{Logger: zap.NewNop(), MetricsFactory: mf},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// Batch 1 occupies the single worker, blocked on the gate.
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	<-started
	// Batch 2 fills the queue (capacity 1).
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 1 },
		time.Second, time.Millisecond, "queue should hold exactly one batch")
	// Batch 3 is non-critical: shed, but acked.
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(3))))

	close(gate)
	require.NoError(t, w.Close())

	assert.Equal(t, 2, storage.spanCount(), "only the two buffered batches may reach storage")
	mf.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "writequeue.dropped", Tags: map[string]string{"reason": "queue_full"}, Value: 1,
	})
}

// TestQueueFullCriticalBlocksThenFails verifies that a critical batch waits
// for capacity up to EnqueueTimeout and is then rejected with ErrQueueFull
// carrying the trigger conditions.
func TestQueueFullCriticalBlocksThenFails(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	storage := &fakeStorage{gate: gate, started: started}
	w, err := NewWriter(
		Config{Capacity: 1, Workers: 1, EnqueueTimeout: 50 * time.Millisecond},
		storage,
		Options{Logger: zap.NewNop()},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	<-started
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 1 }, time.Second, time.Millisecond)

	start := time.Now()
	err = w.WriteTraces(context.Background(), makeTraces("svc", true, makeTraceID(3)))
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrQueueFull)
	assert.Contains(t, err.Error(), "critical batch of 1 spans")
	assert.Contains(t, err.Error(), "queue_capacity=1")
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "critical batch must wait for the timeout")

	close(gate)
	require.NoError(t, w.Close())
	assert.Equal(t, 2, storage.spanCount(), "rejected critical batch must not be written")
}

// TestQueueFullCriticalZeroTimeout verifies EnqueueTimeout=0 rejects critical
// batches immediately instead of blocking.
func TestQueueFullCriticalZeroTimeout(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	storage := &fakeStorage{gate: gate, started: started}
	cfg := Config{Capacity: 1, Workers: 1, EnqueueTimeout: 0}
	w, err := NewWriter(cfg, storage, Options{Logger: zap.NewNop()})
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	<-started
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 1 }, time.Second, time.Millisecond)

	err = w.WriteTraces(context.Background(), makeTraces("svc", true, makeTraceID(3)))
	require.ErrorIs(t, err, ErrQueueFull)
	assert.Contains(t, err.Error(), "EnqueueTimeout=0")

	close(gate)
}

// TestCriticalEnqueueContextCanceled verifies the caller's context aborts a
// blocked critical enqueue and the error names the trigger.
func TestCriticalEnqueueContextCanceled(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	storage := &fakeStorage{gate: gate, started: started}
	w, err := NewWriter(
		Config{Capacity: 1, Workers: 1, EnqueueTimeout: time.Minute},
		storage,
		Options{Logger: zap.NewNop()},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	<-started
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 1 }, time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = w.WriteTraces(ctx, makeTraces("svc", true, makeTraceID(3)))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "critical batch of 1 spans")

	close(gate)
}

// TestCriticalBatchEnqueuedWhenCapacityFrees verifies a blocked critical
// batch gets in once a worker frees capacity.
func TestCriticalBatchEnqueuedWhenCapacityFrees(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	storage := &fakeStorage{gate: gate, started: started}
	w, err := NewWriter(
		Config{Capacity: 1, Workers: 1, EnqueueTimeout: 5 * time.Second},
		storage,
		Options{Logger: zap.NewNop()},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	<-started
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(2))))
	require.Eventually(t, func() bool { return w.Depth() == 1 }, time.Second, time.Millisecond)

	criticalID := makeTraceID(3)
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.WriteTraces(context.Background(), makeTraces("svc", true, criticalID))
	}()
	// Let the critical enqueue block, then free the worker.
	time.Sleep(50 * time.Millisecond)
	close(gate)
	require.NoError(t, <-errCh)
	require.NoError(t, w.Close())
	assert.True(t, storage.traceIDs()[criticalID], "critical trace must be persisted")
}

// dropIDsSampler drops the trace IDs in its set, admits everything else.
type dropIDsSampler map[pcommon.TraceID]bool

func (s dropIDsSampler) ShouldSample(_ string, traceID pcommon.TraceID) bool {
	return !s[traceID]
}

func TestSamplingFiltersTracesFromBatch(t *testing.T) {
	storage := &fakeStorage{}
	droppedID := makeTraceID(1)
	keptID := makeTraceID(2)
	mf := metricstest.NewFactory(0)
	w, err := NewWriter(
		Config{Capacity: 4, Workers: 1},
		storage,
		Options{
			Logger:         zap.NewNop(),
			Sampler:        dropIDsSampler{droppedID: true},
			MetricsFactory: mf,
		},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// One batch carries both a dropped and a kept trace.
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, droppedID, keptID)))
	require.NoError(t, w.Close())

	ids := storage.traceIDs()
	assert.False(t, ids[droppedID], "sampled-out trace must not reach storage")
	assert.True(t, ids[keptID], "sampled-in trace must reach storage")
	mf.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "writequeue.sampled_out_spans", Value: 1,
	})
}

func TestSamplingDropsWholeBatch(t *testing.T) {
	storage := &fakeStorage{}
	w, err := NewWriter(
		Config{Capacity: 4, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), Sampler: dropIDsSampler{makeTraceID(1): true}},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	require.NoError(t, w.Close())
	assert.Equal(t, 0, storage.spanCount())
}

func TestCriticalBatchBypassesSampling(t *testing.T) {
	storage := &fakeStorage{}
	criticalID := makeTraceID(1)
	w, err := NewWriter(
		Config{Capacity: 4, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), Sampler: dropIDsSampler{criticalID: true}},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", true, criticalID)))
	require.NoError(t, w.Close())
	assert.True(t, storage.traceIDs()[criticalID], "critical trace must bypass sampling")
}

func TestStorageWriteErrorIsCountedNotSwallowed(t *testing.T) {
	storage := &fakeStorage{err: errors.New("storage unavailable")}
	mf := metricstest.NewFactory(0)
	w, err := NewWriter(
		Config{Capacity: 4, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), MetricsFactory: mf},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	require.NoError(t, w.Close())
	mf.AssertCounterMetrics(t, metricstest.ExpectedMetric{
		Name: "writequeue.write_errors", Value: 1,
	})
}

func TestMetricsRecorded(t *testing.T) {
	storage := &fakeStorage{}
	mf := metricstest.NewFactory(0)
	w, err := NewWriter(
		Config{Capacity: 8, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), MetricsFactory: mf},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", false, makeTraceID(1))))
	require.NoError(t, w.WriteTraces(context.Background(), makeTraces("svc", true, makeTraceID(2))))
	require.NoError(t, w.Close())

	mf.AssertCounterMetrics(t,
		metricstest.ExpectedMetric{
			Name: "writequeue.enqueued", Tags: map[string]string{"priority": "normal"}, Value: 1,
		},
		metricstest.ExpectedMetric{
			Name: "writequeue.enqueued", Tags: map[string]string{"priority": "critical"}, Value: 1,
		},
	)
	_, gauges := mf.Snapshot()
	assert.Contains(t, gauges, "writequeue.queue_depth")
	assert.Contains(t, gauges, "writequeue.write_latency.P50")
}

func TestDepthAndCapacity(t *testing.T) {
	storage := &fakeStorage{}
	w := newTestWriter(t, Config{Capacity: 7, Workers: 1}, storage)
	assert.Equal(t, 7, w.Capacity())
	assert.Equal(t, 0, w.Depth())
}

// recordingSampler captures the service names it is asked about.
type recordingSampler struct {
	services []string
}

func (s *recordingSampler) ShouldSample(serviceName string, _ pcommon.TraceID) bool {
	s.services = append(s.services, serviceName)
	return true
}

func TestServiceNameFallsBackToEmpty(t *testing.T) {
	storage := &fakeStorage{}
	sampler := &recordingSampler{}
	w, err := NewWriter(
		Config{Capacity: 4, Workers: 1},
		storage,
		Options{Logger: zap.NewNop(), Sampler: sampler},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	// A batch without a service.name resource attribute.
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetTraceID(makeTraceID(1))
	require.NoError(t, w.WriteTraces(context.Background(), td))
	require.NoError(t, w.Close())
	assert.Equal(t, []string{""}, sampler.services)
}

// closableStorage records Close calls.
type closableStorage struct {
	fakeStorage
	closeErr error
	closed   bool
}

func (c *closableStorage) Close() error {
	c.closed = true
	return c.closeErr
}

func TestCloseClosesUnderlyingWriter(t *testing.T) {
	storage := &closableStorage{closeErr: errors.New("close failed")}
	w, err := NewWriter(Config{Capacity: 4, Workers: 1}, storage, Options{Logger: zap.NewNop()})
	require.NoError(t, err)
	err = w.Close()
	require.ErrorContains(t, err, "close failed")
	assert.True(t, storage.closed)
}

// TestConcurrentProducers hammers the writer from many goroutines to surface
// data races (run with -race) and lost batches.
func TestConcurrentProducers(t *testing.T) {
	storage := &fakeStorage{}
	// Capacity exceeds the total number of batches so nothing is shed.
	w := newTestWriter(t, Config{Capacity: 256, Workers: 4}, storage)

	const producers = 8
	const batchesPerProducer = 25
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range batchesPerProducer {
				id := makeTraceID(byte(p*32 + i))
				err := w.WriteTraces(context.Background(), makeTraces("svc", false, id))
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	require.NoError(t, w.Close())
	assert.Equal(t, producers*batchesPerProducer, storage.spanCount())
}

func TestCloseDrainsInFlightBatches(t *testing.T) {
	storage := &fakeStorage{latency: 5 * time.Millisecond}
	w := newTestWriter(t, Config{Capacity: 100, Workers: 2}, storage)

	const batches = 40
	for i := range batches {
		require.NoError(t, w.WriteTraces(context.Background(),
			makeTraces("svc", false, makeTraceID(byte(i)))))
	}
	// Close immediately: every buffered batch must still be written.
	require.NoError(t, w.Close())
	assert.Equal(t, batches, storage.spanCount())
}

func ExampleWriter() {
	storage := &fakeStorage{}
	w, _ := NewWriter(Config{Capacity: 100, Workers: 2}, storage, Options{})
	_ = w.WriteTraces(context.Background(), ptrace.NewTraces())
	_ = w.Close()
	fmt.Println("spans written:", storage.spanCount())
	// Output: spans written: 0
}
