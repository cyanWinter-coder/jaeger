// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger-idl/proto-gen/api_v2"
)

// stubProvider is a samplingstrategy.Provider whose strategy can be swapped
// atomically, to emulate a strategy update propagating from the source.
type stubProvider struct {
	strategy atomic.Pointer[api_v2.SamplingStrategyResponse]
	err      error
}

func (*stubProvider) Close() error { return nil }

func (s *stubProvider) GetSamplingStrategy(
	context.Context,
	string,
) (*api_v2.SamplingStrategyResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.strategy.Load(), nil
}

func probabilisticStrategy(rate float64) *api_v2.SamplingStrategyResponse {
	return &api_v2.SamplingStrategyResponse{
		ProbabilisticSampling: &api_v2.ProbabilisticSamplingStrategy{SamplingRate: rate},
	}
}

func TestAlwaysSampler(t *testing.T) {
	assert.True(t, AlwaysSampler().ShouldSample("svc", makeTraceID(1)))
}

func TestSampleTraceBoundaries(t *testing.T) {
	id := makeTraceID(42)
	assert.False(t, SampleTrace(id, 0))
	assert.False(t, SampleTrace(id, -1))
	assert.True(t, SampleTrace(id, 1))
	assert.True(t, SampleTrace(id, 2))
}

func TestSampleTraceDeterministic(t *testing.T) {
	id := makeTraceID(7)
	first := SampleTrace(id, 0.5)
	for range 100 {
		assert.Equal(t, first, SampleTrace(id, 0.5))
	}
}

func TestSampleTraceDistribution(t *testing.T) {
	const total = 10000
	sampled := 0
	for i := range total {
		var id pcommon.TraceID
		// Spread the lower 64 bits (the sampled half) across the full
		// uint64 range via the golden-ratio multiplier.
		binary.BigEndian.PutUint64(id[8:16], uint64(i)*0x9E3779B97F4A7C15)
		if SampleTrace(id, 0.5) {
			sampled++
		}
	}
	// Loose statistical bounds: expect 50% +/- 5%.
	assert.InDelta(t, total/2, sampled, float64(total)*0.05)
}

func TestProviderSamplerProbabilistic(t *testing.T) {
	provider := &stubProvider{}
	provider.strategy.Store(probabilisticStrategy(0))
	sampler := NewProviderSampler(provider, 1, zap.NewNop())
	assert.False(t, sampler.ShouldSample("svc", makeTraceID(1)))

	provider.strategy.Store(probabilisticStrategy(1))
	assert.True(t, sampler.ShouldSample("svc", makeTraceID(1)))
}

func TestProviderSamplerUpdateTakesEffectImmediately(t *testing.T) {
	provider := &stubProvider{}
	provider.strategy.Store(probabilisticStrategy(0))
	sampler := NewProviderSampler(provider, 1, zap.NewNop())

	id := makeTraceID(9)
	require.False(t, sampler.ShouldSample("svc", id))
	// A strategy update must be visible to the very next decision.
	provider.strategy.Store(probabilisticStrategy(1))
	assert.True(t, sampler.ShouldSample("svc", id))
	provider.strategy.Store(probabilisticStrategy(0))
	assert.False(t, sampler.ShouldSample("svc", id))
}

func TestProviderSamplerPerOperationStrategy(t *testing.T) {
	provider := &stubProvider{}
	provider.strategy.Store(&api_v2.SamplingStrategyResponse{
		OperationSampling: &api_v2.PerOperationSamplingStrategies{
			DefaultSamplingProbability: 1,
		},
	})
	sampler := NewProviderSampler(provider, 0, zap.NewNop())
	assert.True(t, sampler.ShouldSample("svc", makeTraceID(1)))
}

func TestProviderSamplerFallsBackToDefaultOnError(t *testing.T) {
	provider := &stubProvider{err: errors.New("strategy source unavailable")}
	sampler := NewProviderSampler(provider, 1, zap.NewNop())
	assert.True(t, sampler.ShouldSample("svc", makeTraceID(1)))

	sampler = NewProviderSampler(provider, 0, zap.NewNop())
	assert.False(t, sampler.ShouldSample("svc", makeTraceID(1)))
}

func TestProviderSamplerFallsBackForRateLimitingStrategy(t *testing.T) {
	provider := &stubProvider{}
	provider.strategy.Store(&api_v2.SamplingStrategyResponse{
		RateLimitingSampling: &api_v2.RateLimitingSamplingStrategy{MaxTracesPerSecond: 10},
	})
	sampler := NewProviderSampler(provider, 1, zap.NewNop())
	assert.True(t, sampler.ShouldSample("svc", makeTraceID(1)))
}

func TestNewProviderSamplerClampsProbability(t *testing.T) {
	provider := &stubProvider{err: errors.New("no strategies")}
	assert.True(t, NewProviderSampler(provider, 2, nil).ShouldSample("svc", makeTraceID(1)))
	assert.False(t, NewProviderSampler(provider, -1, nil).ShouldSample("svc", makeTraceID(1)))
}
