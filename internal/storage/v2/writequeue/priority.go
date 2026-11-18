// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/jptrace"
)

// Priority is the admission class of a batch.
type Priority int

const (
	// PriorityNormal batches may be sampled out and are shed when the queue
	// is full.
	PriorityNormal Priority = iota
	// PriorityCritical batches bypass sampling and are never shed silently:
	// when the queue is full they wait up to Config.EnqueueTimeout and are
	// then rejected with ErrQueueFull so the caller can retry.
	PriorityCritical
)

func (p Priority) String() string {
	if p == PriorityCritical {
		return "critical"
	}
	return "normal"
}

// Classifier assigns a Priority to a batch. A batch is classified as a
// whole: a single critical span makes the whole batch critical, so no
// critical trace is ever shed because it shared a batch with normal ones.
type Classifier func(td ptrace.Traces) Priority

// DefaultClassifier marks a batch critical when any span has an error
// status or carries any of criticalAttributes with a truthy value
// (true, a non-zero number, or a non-empty string other than "false").
func DefaultClassifier(criticalAttributes ...string) Classifier {
	keys := make(map[string]struct{}, len(criticalAttributes))
	for _, k := range criticalAttributes {
		keys[k] = struct{}{}
	}
	return func(td ptrace.Traces) Priority {
		for _, span := range jptrace.SpanIter(td) {
			if span.Status().Code() == ptrace.StatusCodeError {
				return PriorityCritical
			}
			for key := range keys {
				if v, ok := span.Attributes().Get(key); ok && isTruthy(v) {
					return PriorityCritical
				}
			}
		}
		return PriorityNormal
	}
}

func isTruthy(v pcommon.Value) bool {
	switch v.Type() {
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeStr:
		return v.Str() != "" && v.Str() != "false"
	case pcommon.ValueTypeInt:
		return v.Int() != 0
	case pcommon.ValueTypeDouble:
		return v.Double() != 0
	default:
		// A present complex value (slice, map, bytes) counts as set.
		return true
	}
}
