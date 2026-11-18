// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"errors"
	"fmt"
	"time"
)

// Default configuration values applied by NewWriter for zero-valued fields.
const (
	// DefaultCapacity is the default number of batches the queue buffers.
	DefaultCapacity = 1000
	// DefaultWorkers is the default number of goroutines draining the queue.
	DefaultWorkers = 4
	// DefaultEnqueueTimeout is the recommended bound on how long a critical
	// batch waits for queue capacity before WriteTraces rejects it with
	// ErrQueueFull. It is not applied implicitly: a zero EnqueueTimeout is
	// meaningful (reject immediately), so it cannot double as "unset".
	DefaultEnqueueTimeout = 5 * time.Second
	// DefaultSamplingProbability is used when sampling is enabled but the
	// strategy provider has no strategy for a service (or fails to load one).
	DefaultSamplingProbability = 1.0
)

// defaultCriticalAttributes lists the span attribute keys that mark a trace
// as critical when Config.CriticalAttributes is not set.
var defaultCriticalAttributes = []string{"jaeger.critical"}

// Config controls the behavior of the write queue Writer.
type Config struct {
	// Capacity is the maximum number of batches buffered in the queue.
	// It bounds the memory the writer can hold back from storage.
	Capacity int
	// Workers is the number of goroutines concurrently draining the queue
	// into the underlying storage writer.
	Workers int
	// EnqueueTimeout bounds how long WriteTraces blocks a critical batch
	// when the queue is full. Zero (also the default) means critical batches
	// are rejected immediately with ErrQueueFull instead of waiting; use
	// DefaultEnqueueTimeout for a blocking behavior.
	EnqueueTimeout time.Duration
	// CriticalAttributes lists span attribute keys whose truthy value marks
	// every trace in the batch as critical. Spans with an error status are
	// always critical regardless of this list.
	CriticalAttributes []string
	// Sampling configures admission sampling of non-critical batches.
	Sampling SamplingConfig
}

// SamplingConfig controls admission sampling of non-critical traces.
type SamplingConfig struct {
	// Enabled turns on per-trace admission sampling. Critical batches are
	// never sampled out.
	Enabled bool
	// DefaultProbability is the fallback probability in [0, 1] used when the
	// strategy provider has no probabilistic strategy for a service or fails
	// to load one.
	DefaultProbability float64
}

func (c *Config) applyDefaults() {
	if c.Capacity == 0 {
		c.Capacity = DefaultCapacity
	}
	if c.Workers == 0 {
		c.Workers = DefaultWorkers
	}
	if c.CriticalAttributes == nil {
		c.CriticalAttributes = defaultCriticalAttributes
	}
	if c.Sampling.Enabled && c.Sampling.DefaultProbability == 0 {
		c.Sampling.DefaultProbability = DefaultSamplingProbability
	}
}

// Validate reports every invalid field, not just the first one.
func (c Config) Validate() error {
	var errs []error
	if c.Capacity <= 0 {
		errs = append(errs, fmt.Errorf("Capacity must be positive, got %d", c.Capacity))
	}
	if c.Workers <= 0 {
		errs = append(errs, fmt.Errorf("Workers must be positive, got %d", c.Workers))
	}
	if c.EnqueueTimeout < 0 {
		errs = append(errs, fmt.Errorf("EnqueueTimeout must not be negative, got %s", c.EnqueueTimeout))
	}
	if p := c.Sampling.DefaultProbability; p < 0 || p > 1 {
		errs = append(errs, fmt.Errorf("Sampling.DefaultProbability must be in [0, 1], got %f", p))
	}
	return errors.Join(errs...)
}
