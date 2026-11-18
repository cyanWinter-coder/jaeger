// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/internal/metrics"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	"github.com/jaegertracing/jaeger/internal/telemetry/otelsemconv"
)

var (
	// ErrQueueFull is returned (wrapped) when a critical batch cannot be
	// enqueued within Config.EnqueueTimeout because the queue is full.
	ErrQueueFull = errors.New("write queue is full")
	// ErrWriterClosed is returned (wrapped) when WriteTraces is called after
	// Close.
	ErrWriterClosed = errors.New("write queue is closed")
)

var _ tracestore.Writer = (*Writer)(nil)

// Writer buffers trace batches in a bounded queue and drains them into the
// underlying storage writer from a fixed number of worker goroutines. See
// the package documentation for the backpressure and sampling semantics.
type Writer struct {
	config   Config
	next     tracestore.Writer
	sampler  Sampler
	classify Classifier
	logger   *zap.Logger
	metrics  queueMetrics

	queue chan queueItem

	// mu guarantees no goroutine sends on queue once Close has closed it:
	// sends happen under RLock, close happens under Lock.
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

type queueItem struct {
	traces ptrace.Traces
}

// Options carries the optional dependencies of NewWriter; nil fields get
// defaults (AlwaysSampler, DefaultClassifier, no-op logger and metrics).
type Options struct {
	Sampler        Sampler
	Classifier     Classifier
	Logger         *zap.Logger
	MetricsFactory metrics.Factory
}

// NewWriter creates a Writer and starts its worker goroutines. The returned
// Writer must be closed to drain the queued batches and stop the workers.
func NewWriter(config Config, next tracestore.Writer, options Options) (*Writer, error) {
	if next == nil {
		return nil, errors.New("writequeue: next storage writer must not be nil")
	}
	config.applyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("writequeue: invalid configuration: %w", err)
	}
	if options.Sampler == nil {
		options.Sampler = AlwaysSampler()
	}
	if options.Classifier == nil {
		options.Classifier = DefaultClassifier(config.CriticalAttributes...)
	}
	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}
	if options.MetricsFactory == nil {
		options.MetricsFactory = metrics.NullFactory
	}
	w := &Writer{
		config:   config,
		next:     next,
		sampler:  options.Sampler,
		classify: options.Classifier,
		logger:   options.Logger,
		metrics:  newQueueMetrics(options.MetricsFactory),
		queue:    make(chan queueItem, config.Capacity),
	}
	for range config.Workers {
		w.wg.Add(1)
		go w.worker()
	}
	return w, nil
}

// WriteTraces classifies, samples, and enqueues the batch. It returns nil
// once the batch is buffered (or deliberately shed), and a non-nil error
// only when a critical batch could not be buffered — see the package
// documentation for the durability trade-off.
func (w *Writer) WriteTraces(ctx context.Context, td ptrace.Traces) error {
	priority := w.classify(td)
	if priority != PriorityCritical {
		td = w.filterSampled(td)
		if td.SpanCount() == 0 {
			return nil
		}
	}

	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return fmt.Errorf("writequeue: cannot enqueue batch of %d spans: %w",
			td.SpanCount(), ErrWriterClosed)
	}

	item := queueItem{traces: td}
	select {
	case w.queue <- item:
		w.onEnqueued(priority)
		return nil
	default:
	}

	if priority != PriorityCritical {
		w.metrics.droppedQueueFull.Inc(1)
		w.logger.Warn("writequeue: queue full, shedding non-critical batch",
			zap.Int("spans", td.SpanCount()),
			zap.Int("queue_depth", len(w.queue)),
			zap.Int("queue_capacity", w.config.Capacity))
		return nil
	}

	if w.config.EnqueueTimeout <= 0 {
		return fmt.Errorf("writequeue: %w: critical batch of %d spans rejected immediately "+
			"(EnqueueTimeout=0, queue_depth=%d, queue_capacity=%d)",
			ErrQueueFull, td.SpanCount(), len(w.queue), w.config.Capacity)
	}
	waitStart := time.Now()
	timer := time.NewTimer(w.config.EnqueueTimeout)
	defer timer.Stop()
	select {
	case w.queue <- item:
		w.metrics.enqueueWait.Record(time.Since(waitStart))
		w.onEnqueued(priority)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("writequeue: enqueue of critical batch of %d spans canceled "+
			"while waiting for queue capacity (queue_depth=%d, queue_capacity=%d): %w",
			td.SpanCount(), len(w.queue), w.config.Capacity, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("writequeue: %w: critical batch of %d spans could not be enqueued "+
			"within %s (queue_depth=%d, queue_capacity=%d)",
			ErrQueueFull, td.SpanCount(), w.config.EnqueueTimeout,
			len(w.queue), w.config.Capacity)
	}
}

// filterSampled returns a copy of td containing only the traces the sampler
// admits, and records how many spans were sampled out.
func (w *Writer) filterSampled(td ptrace.Traces) ptrace.Traces {
	decisions := make(map[pcommon.TraceID]bool)
	kept := ptrace.NewTraces()
	td.CopyTo(kept)
	resourceSpans := kept.ResourceSpans()
	resourceSpans.RemoveIf(func(rs ptrace.ResourceSpans) bool {
		serviceName := serviceNameOf(rs)
		scopeSpans := rs.ScopeSpans()
		scopeSpans.RemoveIf(func(ss ptrace.ScopeSpans) bool {
			spans := ss.Spans()
			spans.RemoveIf(func(span ptrace.Span) bool {
				traceID := span.TraceID()
				keep, ok := decisions[traceID]
				if !ok {
					keep = w.sampler.ShouldSample(serviceName, traceID)
					decisions[traceID] = keep
				}
				return !keep
			})
			return spans.Len() == 0
		})
		return scopeSpans.Len() == 0
	})
	w.metrics.sampledOutSpans.Inc(int64(td.SpanCount() - kept.SpanCount()))
	return kept
}

func serviceNameOf(rs ptrace.ResourceSpans) string {
	if v, ok := rs.Resource().Attributes().Get(otelsemconv.ServiceNameKey); ok {
		return v.Str()
	}
	return ""
}

func (w *Writer) onEnqueued(priority Priority) {
	w.metrics.incEnqueued(priority)
	w.metrics.queueDepth.Update(int64(len(w.queue)))
}

func (w *Writer) worker() {
	defer w.wg.Done()
	for item := range w.queue {
		w.metrics.queueDepth.Update(int64(len(w.queue)))
		start := time.Now()
		// The enqueue-side context may already be canceled; writes must
		// still complete, so the worker uses a detached context.
		err := w.next.WriteTraces(context.Background(), item.traces)
		w.metrics.writeLatency.Record(time.Since(start))
		if err != nil {
			w.metrics.writeErrors.Inc(1)
			w.logger.Error("writequeue: failed to write batch to storage",
				zap.Int("spans", item.traces.SpanCount()),
				zap.Error(err))
		}
	}
}

// Depth returns the number of batches currently buffered in the queue.
func (w *Writer) Depth() int {
	return len(w.queue)
}

// Capacity returns the maximum number of batches the queue can buffer.
func (w *Writer) Capacity() int {
	return w.config.Capacity
}

// Close stops accepting new batches, drains the buffered ones into storage,
// waits for the workers to finish, and closes the underlying writer if it
// implements io.Closer.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.queue)
	w.mu.Unlock()
	w.wg.Wait()
	if closer, ok := w.next.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
