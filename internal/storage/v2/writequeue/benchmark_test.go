// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

// BenchmarkDirectWrite is the baseline: writing straight to storage.
func BenchmarkDirectWrite(b *testing.B) {
	storage := &fakeStorage{}
	td := makeTraces("svc", false, makeTraceID(1))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := storage.WriteTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueuedWrite measures the enqueue path of the write queue with a
// storage backend fast enough to never fill the queue. The performance
// target is <= 10% added latency over BenchmarkDirectWrite; see the package
// README for measured numbers.
func BenchmarkQueuedWrite(b *testing.B) {
	storage := &fakeStorage{}
	w, err := NewWriter(
		Config{Capacity: 1 << 16, Workers: 4},
		storage,
		Options{Logger: zap.NewNop()},
	)
	if err != nil {
		b.Fatal(err)
	}
	td := makeTraces("svc", false, makeTraceID(1))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkQueuedWriteSlowStorage shows that caller-perceived write latency
// stays bounded (microseconds) even when the storage backend is slow
// (500µs per batch): excess traffic is shed, not queued in memory.
func BenchmarkQueuedWriteSlowStorage(b *testing.B) {
	storage := &fakeStorage{latency: 500 * time.Microsecond}
	w, err := NewWriter(
		Config{Capacity: 1024, Workers: 4},
		storage,
		Options{Logger: zap.NewNop()},
	)
	if err != nil {
		b.Fatal(err)
	}
	td := makeTraces("svc", false, makeTraceID(1))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkQueuedWriteSampled measures the enqueue path with admission
// sampling enabled (per-trace decisions plus batch filtering).
func BenchmarkQueuedWriteSampled(b *testing.B) {
	storage := &fakeStorage{}
	provider := &stubProvider{}
	provider.strategy.Store(probabilisticStrategy(0.5))
	w, err := NewWriter(
		Config{Capacity: 1 << 16, Workers: 4},
		storage,
		Options{
			Logger:  zap.NewNop(),
			Sampler: NewProviderSampler(provider, 1.0, zap.NewNop()),
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	td := makeTraces("svc", false, makeTraceID(1))
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
}
