# Security Policy

## Supported versions

| Version | Supported          |
| ------- | ------------------ |
| v1.x.x  | :white_check_mark: |
| < v1.0  | :x:                |

## Reporting a vulnerability

**Please do not open a public issue for suspected vulnerabilities.**

Report privately via one of:

- **GitHub Security Advisories** (preferred): use the
  [Report a vulnerability](https://github.com/F2077/go-pubsub/security/advisories/new)
  button on the repository's Security tab.
- **Email**: open an issue requesting the security contact address and
  we will respond with one.

A useful report includes:

- A short description of the impact
- Reproduction steps or a minimal test case
- The commit / version you observed the issue on

We aim to acknowledge reports within **72 hours** and to publish a
fix or mitigation within **30 days** for confirmed issues, sooner
for critical bugs.

## Scope

`go-pubsub` is an in-process, in-memory pub/sub library. Realistic
threats include:

- Concurrency bugs (data races, lock-order deadlocks) under the
  `go test -race ./...` gate.
- Memory growth (unbounded topic / subscription accumulation) —
  mitigate with `pubsub.WithCapacity`.
- Channel-buffer exhaustion causing message loss — this is **by
  design** (fire-and-forget), not a vulnerability, but report it if
  it surprises a downstream user.

## Out of scope

- Issues in `github.com/google/uuid` or `github.com/stretchr/testify` —
  report upstream.
- Theoretical attacks that require arbitrary code execution in the
  host process.
