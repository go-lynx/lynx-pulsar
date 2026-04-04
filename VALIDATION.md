# Validation

## Automated Baseline

Current workspace baseline:

```bash
go test ./... -count=1
go vet ./...
```

Output:

```text
ok   github.com/go-lynx/lynx-pulsar       (passes root package unit tests)
?    github.com/go-lynx/lynx-pulsar/conf  [no test files]
go vet ./...                              (passes)
```

## What This Means

- The root package now has committed unit tests covering default config construction, runtime config scanning, initialization-time defaulting/validation, client option parsing, and retry/manager helpers.
- The `conf` package is generated protobuf code and still has no standalone test files.
- The automated baseline is still unit-level only; there is no committed broker-backed integration test for producer, consumer, or end-to-end health behavior.

## Recommended Manual Smoke Checks

- Start against a reachable Pulsar broker and verify `Produce()` succeeds on a configured topic.
- Verify `Subscribe()` receives messages for at least one configured consumer.
- If you enable health checks and metrics, verify they update after startup and after a forced broker disconnect.
