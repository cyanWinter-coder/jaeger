// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

// Package writequeue provides a bounded, priority-aware, sampling-aware
// buffering decorator around a tracestore.Writer.
//
// The decorator absorbs bursty trace traffic and applies explicit
// backpressure when the storage backend slows down, instead of growing
// memory without bound:
//
//   - Batches are buffered in a fixed-capacity queue drained by a
//     configurable number of worker goroutines.
//   - Each batch is classified (see DefaultClassifier): batches containing
//     error spans or configured critical attributes are critical.
//   - Non-critical batches are admitted through a Sampler (for example
//     ProviderSampler, which re-reads the sampling strategy on every
//     decision so strategy updates take effect immediately) and are
//     shed — counted and acked — when the queue is full.
//   - Critical batches bypass sampling and, when the queue is full, block
//     up to Config.EnqueueTimeout; if the wait expires they are rejected
//     with an error wrapping ErrQueueFull so the caller can retry.
//
// The asynchronous write path is a deliberate deviation from the
// synchronous contract documented on tracestore.Writer: WriteTraces
// returns after the batch is buffered, not after it is persisted. This
// trades durability for bounded memory and stable latency under
// backpressure, and is therefore an explicit, opt-in component rather
// than a default (see the package README).
package writequeue
