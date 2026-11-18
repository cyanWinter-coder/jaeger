// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestPriorityString(t *testing.T) {
	assert.Equal(t, "critical", PriorityCritical.String())
	assert.Equal(t, "normal", PriorityNormal.String())
}

func TestDefaultClassifier(t *testing.T) {
	classifier := DefaultClassifier("jaeger.critical")

	t.Run("error status is critical", func(t *testing.T) {
		td := makeTraces("svc", true, makeTraceID(1))
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("plain batch is normal", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		assert.Equal(t, PriorityNormal, classifier(td))
	})

	t.Run("critical attribute bool", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutBool("jaeger.critical", true)
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("critical attribute false bool is normal", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutBool("jaeger.critical", false)
		assert.Equal(t, PriorityNormal, classifier(td))
	})

	t.Run("critical attribute string", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutStr("jaeger.critical", "yes")
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("critical attribute string false is normal", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutStr("jaeger.critical", "false")
		assert.Equal(t, PriorityNormal, classifier(td))
	})

	t.Run("critical attribute int", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutInt("jaeger.critical", 1)
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("critical attribute zero int is normal", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutInt("jaeger.critical", 0)
		assert.Equal(t, PriorityNormal, classifier(td))
	})

	t.Run("critical attribute double", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutDouble("jaeger.critical", 0.5)
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("critical attribute zero double is normal", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutDouble("jaeger.critical", 0)
		assert.Equal(t, PriorityNormal, classifier(td))
	})

	t.Run("critical attribute complex value counts as set", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutEmptySlice("jaeger.critical")
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("one critical span marks the whole batch", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1), makeTraceID(2))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(1)
		span.Status().SetCode(ptrace.StatusCodeError)
		assert.Equal(t, PriorityCritical, classifier(td))
	})

	t.Run("unconfigured attribute is ignored", func(t *testing.T) {
		td := makeTraces("svc", false, makeTraceID(1))
		span := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		span.Attributes().PutBool("other.attr", true)
		assert.Equal(t, PriorityNormal, classifier(td))
	})
}
