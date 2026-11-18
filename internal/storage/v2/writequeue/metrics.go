// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"github.com/jaegertracing/jaeger/internal/metrics"
)

// queueMetrics holds the writer's metrics, all under the "writequeue"
// namespace.
type queueMetrics struct {
	// enqueued counts batches accepted into the queue, tagged by priority.
	enqueuedNormal   metrics.Counter
	enqueuedCritical metrics.Counter
	// droppedQueueFull counts non-critical batches shed because the queue
	// was full.
	droppedQueueFull metrics.Counter
	// sampledOutSpans counts spans removed by admission sampling.
	sampledOutSpans metrics.Counter
	// queueDepth is the number of batches currently buffered.
	queueDepth metrics.Gauge
	// writeErrors counts failed writes to the underlying storage.
	writeErrors metrics.Counter
	// writeLatency measures the underlying storage write latency.
	writeLatency metrics.Timer
	// enqueueWait measures how long critical batches blocked waiting for
	// queue capacity.
	enqueueWait metrics.Timer
}

func newQueueMetrics(factory metrics.Factory) queueMetrics {
	scoped := factory.Namespace(metrics.NSOptions{Name: "writequeue"})
	return queueMetrics{
		enqueuedNormal: scoped.Counter(metrics.Options{
			Name: "enqueued",
			Tags: map[string]string{"priority": PriorityNormal.String()},
		}),
		enqueuedCritical: scoped.Counter(metrics.Options{
			Name: "enqueued",
			Tags: map[string]string{"priority": PriorityCritical.String()},
		}),
		droppedQueueFull: scoped.Counter(metrics.Options{
			Name: "dropped",
			Tags: map[string]string{"reason": "queue_full"},
		}),
		sampledOutSpans: scoped.Counter(metrics.Options{
			Name: "sampled_out_spans",
		}),
		queueDepth:   scoped.Gauge(metrics.Options{Name: "queue_depth"}),
		writeErrors:  scoped.Counter(metrics.Options{Name: "write_errors"}),
		writeLatency: scoped.Timer(metrics.TimerOptions{Name: "write_latency"}),
		enqueueWait:  scoped.Timer(metrics.TimerOptions{Name: "enqueue_wait"}),
	}
}

func (m queueMetrics) incEnqueued(priority Priority) {
	if priority == PriorityCritical {
		m.enqueuedCritical.Inc(1)
	} else {
		m.enqueuedNormal.Inc(1)
	}
}
