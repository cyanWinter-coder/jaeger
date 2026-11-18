# writequeue — bounded, priority-aware storage write buffer

`writequeue` decorates a [`tracestore.Writer`](../api/tracestore/writer.go)
with a fixed-capacity queue, admission sampling, and explicit backpressure,
so that bursty trace traffic or a slow storage backend cannot grow memory
without bound or block the ingest pipeline indefinitely.

## How it works

```
producers ──▶ classify ──▶ sample (non-critical only) ──▶ bounded queue ──▶ N workers ──▶ storage
                 │                                            │
                 │                                            ├─ full + non-critical → shed (counted, acked)
                 │                                            └─ full + critical     → wait EnqueueTimeout,
                 │                                                                               else ErrQueueFull
                 └─ critical = any span with error status or a configured critical attribute
```

1. **Classification.** Each batch is classified by a `Classifier`
   (`DefaultClassifier` by default): a batch is *critical* if any span has an
   error status or carries any of `Config.CriticalAttributes` (default
   `jaeger.critical`) with a truthy value. Classification is per batch: one
   critical span makes the whole batch critical.
2. **Admission sampling.** Non-critical batches pass through a `Sampler`.
   `ProviderSampler` resolves the strategy from a
   `samplingstrategy.Provider` (e.g. the file provider with
   `ReloadInterval`) **on every decision**, so a strategy update takes effect
   on the very next batch — there is no local cache to go stale. Decisions
   are deterministic per trace ID (lower 64 bits, like Jaeger clients), so a
   trace is kept or dropped as a whole. Critical batches bypass sampling.
3. **Bounded queue.** Batches are buffered in a channel of
   `Config.Capacity` batches, drained by `Config.Workers` goroutines. When
   the queue is full:
   - *non-critical* batches are **shed**: counted in
     `writequeue.dropped{reason=queue_full}`, logged, and acknowledged —
     this is deliberate load shedding, not an error;
   - *critical* batches block up to `Config.EnqueueTimeout` (0 = reject
     immediately) and are then rejected with an error wrapping
     `ErrQueueFull`, so the caller can retry. The error message carries the
     trigger conditions (span count, queue depth, capacity, timeout).
4. **Drain on close.** `Close` stops accepting batches (`ErrWriterClosed`),
   drains everything buffered into storage, waits for the workers, and
   closes the underlying writer if it is an `io.Closer`.

## Usage

```go
provider, _ := file.NewProvider(file.Options{
    StrategiesFile: "/etc/jaeger/strategies.json",
    ReloadInterval: 10 * time.Second,
}, logger)
defer provider.Close()

w, err := writequeue.NewWriter(
    writequeue.Config{
        Capacity:         1000,
        Workers:          4,
        EnqueueTimeout:   writequeue.DefaultEnqueueTimeout, // 5s
        Sampling:         writequeue.SamplingConfig{Enabled: true, DefaultProbability: 1.0},
    },
    storageWriter, // any tracestore.Writer
    writequeue.Options{
        Sampler:        writequeue.NewProviderSampler(provider, 1.0, logger),
        Logger:         logger,
        MetricsFactory: metricsFactory,
    },
)
if err != nil { ... }
defer w.Close()

err = w.WriteTraces(ctx, traces) // buffered; errors only for critical batches under pressure
```

All existing interfaces are untouched: `Writer` is an opt-in decorator that
itself implements `tracestore.Writer`.

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `Capacity` | `1000` | Max batches buffered; bounds memory. |
| `Workers` | `4` | Goroutines draining the queue into storage. |
| `EnqueueTimeout` | `0` | How long a critical batch waits for capacity before `ErrQueueFull`. `0` rejects immediately; `DefaultEnqueueTimeout` (5s) is the recommended blocking value. |
| `CriticalAttributes` | `["jaeger.critical"]` | Span attribute keys marking a trace critical. Error-status spans are always critical. |
| `Sampling.Enabled` | `false` | Turn on admission sampling of non-critical traces. |
| `Sampling.DefaultProbability` | `1.0` | Fallback when the provider has no probabilistic strategy or fails. |

## Metrics

All under the `writequeue` namespace:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `enqueued` | counter | `priority` | Batches accepted into the queue. |
| `dropped` | counter | `reason=queue_full` | Non-critical batches shed at a full queue. |
| `sampled_out_spans` | counter | — | Spans removed by admission sampling. |
| `queue_depth` | gauge | — | Batches currently buffered. |
| `write_errors` | counter | — | Failed storage writes (logged, not swallowed). |
| `write_latency` | timer | — | Storage write latency. |
| `enqueue_wait` | timer | — | Time critical batches blocked on a full queue. |

## Tests

```bash
# unit + integration tests (includes the slow-storage backpressure scenario)
go test -race ./internal/storage/v2/writequeue/

# coverage
go test -covermode=atomic -coverprofile=cover.out ./internal/storage/v2/writequeue/
go tool cover -func=cover.out

# benchmarks (direct baseline vs queued vs sampled vs slow storage)
go test -run=NONE -bench=. ./internal/storage/v2/writequeue/
```

`TestSlowStorageBackpressure` is the acceptance scenario: concurrent
producers burst traffic at a storage backend 10x slower than ingress while
the sampling strategy is updated mid-flight via a strategies file; it
asserts bounded queue depth, strategy updates taking effect, retention of
every accepted critical trace, and exact write/shed/sampled-out bookkeeping.

## Performance

Measured on an AMD Ryzen 7 9700X (go1.27, `-benchtime=1s`):

| Benchmark | ns/op |
|---|---|
| `BenchmarkDirectWrite` (baseline, no-op storage) | ~60–240 |
| `BenchmarkQueuedWrite` | ~700–2300 |
| `BenchmarkQueuedWriteSampled` | ~640–3100 |
| `BenchmarkQueuedWriteSlowStorage` (500µs storage) | ~3600 |

The queue adds a fixed ~1–3µs of enqueue overhead per batch. Against any
realistic storage write latency (≥30µs) that is well under the 10% overhead
target, and under a slow backend the caller-perceived latency stays in
microseconds because excess traffic is shed rather than buffered.

## Known limitations

- **Durability trade-off.** `WriteTraces` returns after buffering, not after
  persistence (a deliberate, documented deviation from the synchronous
  `tracestore.Writer` contract). Buffered batches can be lost if the process
  crashes before draining. If you need synchronous durability, do not use
  this decorator.
- **Per-operation strategies are approximated.** Admission has no operation
  name, so a per-operation strategy contributes only its default
  probability; rate-limiting strategies fall back to
  `Sampling.DefaultProbability`.
- **Batch-level criticality.** One critical span marks the whole batch
  critical; a batch that mixes critical and normal traces keeps both.
- **Shed data is gone.** Non-critical batches dropped at a full queue are
  counted and logged but not retried; route them to a dead-letter connector
  upstream if you need them.
