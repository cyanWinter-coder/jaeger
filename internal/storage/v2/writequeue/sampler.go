// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"encoding/binary"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/internal/sampling/samplingstrategy"
)

// Sampler makes a per-trace admission decision. The decision must be stable
// for a given trace ID so that all spans of one trace are kept or dropped
// together.
type Sampler interface {
	// ShouldSample reports whether the trace should be admitted to the
	// write queue.
	ShouldSample(serviceName string, traceID pcommon.TraceID) bool
}

// AlwaysSampler admits every trace; it is the default when no Sampler is
// configured.
func AlwaysSampler() Sampler {
	return alwaysSampler{}
}

type alwaysSampler struct{}

func (alwaysSampler) ShouldSample(string, pcommon.TraceID) bool { return true }

// ProviderSampler derives admission decisions from a samplingstrategy.Provider.
// It queries the provider on every decision, so a strategy update propagated
// to the provider (for example a strategies-file reload) takes effect on the
// very next batch — there is no local caching layer to go stale.
//
// Only probabilistic strategies are honored: a probabilistic strategy
// contributes its sampling rate, a per-operation strategy contributes its
// default probability (the admission decision has no operation name), and
// any other response (or a provider error) falls back to defaultProbability.
type ProviderSampler struct {
	provider           samplingstrategy.Provider
	defaultProbability float64
	logger             *zap.Logger
}

// NewProviderSampler creates a ProviderSampler. defaultProbability must be in
// [0, 1]; values outside the range are clamped.
func NewProviderSampler(
	provider samplingstrategy.Provider,
	defaultProbability float64,
	logger *zap.Logger,
) *ProviderSampler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ProviderSampler{
		provider:           provider,
		defaultProbability: min(max(defaultProbability, 0), 1),
		logger:             logger,
	}
}

// ShouldSample implements Sampler.
func (s *ProviderSampler) ShouldSample(serviceName string, traceID pcommon.TraceID) bool {
	probability := s.defaultProbability
	strategy, err := s.provider.GetSamplingStrategy(context.Background(), serviceName)
	switch {
	case err != nil:
		s.logger.Warn(
			"writequeue: failed to load sampling strategy, using default probability",
			zap.String("service", serviceName),
			zap.Float64("default_probability", probability),
			zap.Error(err),
		)
	case strategy.GetProbabilisticSampling() != nil:
		probability = strategy.GetProbabilisticSampling().GetSamplingRate()
	case strategy.GetOperationSampling() != nil:
		probability = strategy.GetOperationSampling().GetDefaultSamplingProbability()
	default:
		// Rate-limiting and empty responses keep the default probability.
	}
	return SampleTrace(traceID, probability)
}

// SampleTrace makes a deterministic probabilistic decision from the lower 64
// bits of the trace ID, matching how Jaeger clients hash trace IDs, so all
// spans of a trace share one decision and decisions are stable across
// processes.
func SampleTrace(traceID pcommon.TraceID, probability float64) bool {
	if probability <= 0 {
		return false
	}
	if probability >= 1 {
		return true
	}
	hash := binary.BigEndian.Uint64(traceID[8:16])
	return hash < uint64(probability*float64(math.MaxUint64))
}
