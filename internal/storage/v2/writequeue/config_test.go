// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package writequeue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()
	assert.Equal(t, DefaultCapacity, cfg.Capacity)
	assert.Equal(t, DefaultWorkers, cfg.Workers)
	// EnqueueTimeout is intentionally not defaulted: zero means
	// "reject critical batches immediately".
	assert.Zero(t, cfg.EnqueueTimeout)
	assert.Equal(t, defaultCriticalAttributes, cfg.CriticalAttributes)
	require.NoError(t, cfg.Validate())
}

func TestConfigDefaultsSamplingProbability(t *testing.T) {
	cfg := Config{Sampling: SamplingConfig{Enabled: true}}
	cfg.applyDefaults()
	assert.InDelta(t, DefaultSamplingProbability, cfg.Sampling.DefaultProbability, 1e-9)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "valid",
			config:  Config{Capacity: 10, Workers: 2, EnqueueTimeout: time.Second},
			wantErr: "",
		},
		{
			name:    "non-positive capacity",
			config:  Config{Capacity: -1, Workers: 1},
			wantErr: "Capacity must be positive",
		},
		{
			name:    "non-positive workers",
			config:  Config{Capacity: 1, Workers: 0},
			wantErr: "Workers must be positive",
		},
		{
			name:    "negative enqueue timeout",
			config:  Config{Capacity: 1, Workers: 1, EnqueueTimeout: -time.Second},
			wantErr: "EnqueueTimeout must not be negative",
		},
		{
			name: "probability above one",
			config: Config{
				Capacity: 1,
				Workers:  1,
				Sampling: SamplingConfig{DefaultProbability: 1.5},
			},
			wantErr: "Sampling.DefaultProbability must be in [0, 1]",
		},
		{
			name: "probability below zero",
			config: Config{
				Capacity: 1,
				Workers:  1,
				Sampling: SamplingConfig{DefaultProbability: -0.1},
			},
			wantErr: "Sampling.DefaultProbability must be in [0, 1]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestConfigValidateReportsAllErrors(t *testing.T) {
	err := Config{Capacity: -1, Workers: -1}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Capacity")
	assert.Contains(t, err.Error(), "Workers")
}
