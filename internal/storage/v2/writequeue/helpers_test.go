// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/telemetry/otelsemconv"
)

// fakeStorage is a tracestore.Writer test double with controllable latency
// and an optional gate that blocks writes until released.
type fakeStorage struct {
	mu      sync.Mutex
	latency time.Duration
	// gate, when non-nil, blocks every write until a token is received.
	gate <-chan struct{}
	// started, when non-nil, receives one notification per write attempt.
	started chan<- struct{}
	err     error
	batches []ptrace.Traces
}

func (f *fakeStorage) WriteTraces(ctx context.Context, td ptrace.Traces) error {
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.latency > 0 {
		select {
		case <-time.After(f.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.batches = append(f.batches, td)
	return nil
}

func (f *fakeStorage) spanCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, b := range f.batches {
		total += b.SpanCount()
	}
	return total
}

// traceIDs returns the set of trace IDs persisted to storage.
func (f *fakeStorage) traceIDs() map[pcommon.TraceID]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make(map[pcommon.TraceID]bool)
	for _, b := range f.batches {
		for _, rs := range b.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, span := range ss.Spans().All() {
					ids[span.TraceID()] = true
				}
			}
		}
	}
	return ids
}

func makeTraceID(b byte) pcommon.TraceID {
	var id pcommon.TraceID
	id[0] = b
	id[8] = b
	id[15] = b
	return id
}

// makeTraces builds a batch with one span per trace ID, all belonging to
// service. When critical is true the spans carry an error status.
func makeTraces(service string, critical bool, traceIDs ...pcommon.TraceID) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(otelsemconv.ServiceNameKey, service)
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for _, id := range traceIDs {
		span := spans.AppendEmpty()
		span.SetTraceID(id)
		span.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, byte(spans.Len())})
		span.SetName("op")
		if critical {
			span.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	return td
}
