# Validation

## Automated Baseline

Current workspace baseline:

```bash
go test ./...
```

Output:

```text
?    github.com/go-lynx/lynx-pulsar       [no test files]
?    github.com/go-lynx/lynx-pulsar/conf  [no test files]
```

## What This Means

- This module currently has no committed Go test files.
- The repository has a buildable package baseline, but there is no automated behavior coverage for producer, consumer, retry, or health-check flows yet.

## Recommended Manual Smoke Checks

- Start against a reachable Pulsar broker and verify `Produce()` succeeds on a configured topic.
- Verify `Subscribe()` receives messages for at least one configured consumer.
- If you enable health checks and metrics, verify they update after startup and after a forced broker disconnect.
