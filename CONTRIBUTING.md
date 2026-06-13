# Contributing to go-pubsub

Thanks for your interest in `go-pubsub`! This document is short on
purpose — the project follows a small set of well-trodden conventions and
the rest of the workflow is whatever works for you.

## Ground rules

- Be respectful. See [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).
- Discuss non-trivial changes in an issue **before** opening a PR.
  The bar is low — a one-paragraph description is enough to start.
- Keep the public API surface small. Adding exported names is a
  commitment to backward compatibility; deprecate first, remove later.
- All code must run cleanly under `make ci` (fmt + vet + race-test +
  bench). Local `make help` lists the available targets.

## Development workflow

```bash
# 1. Fork & clone
git clone https://github.com/<you>/go-pubsub
cd go-pubsub

# 2. Verify your toolchain (Go ≥ 1.21 per go.mod)
go version

# 3. Run the full quality gate before pushing
make ci
```

`make help` is self-documenting — every target carries a `## comment`
that `make help` greps into a table. If you add a target, add a `##`
comment too.

## Style

- Code is formatted with `gofmt` and vetted with `go vet`. The
  `make fmt` target fails the build on any unformatted file; run
  `gofmt -w .` to fix.
- Exported identifiers must carry a godoc comment that begins with the
  identifier name itself (this is enforced by `revive`).
- Internal comments are welcome in **中文** (Chinese); the maintainers
  write Chinese for the implementation narrative. Public API godoc and
  user-visible strings (error messages, `slog` messages) must be in
  English — see [`CLAUDE.md`](./CLAUDE.md) for the exact split.
- Don't add new dependencies unless they pull their weight. Three
  direct deps today: `github.com/google/uuid` (handle ids),
  `github.com/stretchr/testify` (test ergonomics), and
  `go.uber.org/goleak` (goroutine-leak guard on every test). Adding a
  fourth requires updating this list in the same PR.

## Commit messages

Follow the repository's [Conventional Commits](./.claude/rules/git-commits.md)
convention. One logical change per commit; subject ≤ 72 chars, no
trailing period. The maintainers' commits end with
`Co-Authored-By: <Model Name>` (no email).

## Reporting vulnerabilities

See [`SECURITY.md`](./SECURITY.md). Please **do not** file a public
issue for suspected vulnerabilities.
