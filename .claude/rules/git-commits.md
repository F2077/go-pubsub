# Git Commit Conventions

All commit messages in this repository **must** follow these rules. The rules
override any default the harness injects (e.g. the
`Co-Authored-By: <Name> <email>` trailer Claude Code adds by default).

## Language

- The **subject**, **body**, and **footer** of every commit message must be
  written in **English**. This includes the `Co-Authored-By` trailer.

## Format — Conventional Commits

Follow the [Conventional Commits 1.0.0](https://www.conventionalcommits.org/)
specification:

- **Subject line:** `<type>(<optional-scope>): <short description>`
  - Allowed `<type>` values: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`,
    `build`, `ci`, `chore`, `style`, `revert`.
  - Use the **imperative mood** ("add", not "added" or "adds").
  - Keep it ≤ **72 characters**, no trailing period.
  - Capitalize the first letter of the description.
- **Body** (optional): wrap at 72 chars; explain *why* the change was made,
  not *what* (the diff shows the what).
- **Footer** (optional): include `BREAKING CHANGE: <description>` for
  incompatible changes, and any `Refs:` / `Closes #N` references.

## `Co-Authored-By` trailer — model name only

End the message with exactly **one** `Co-Authored-By` trailer:

- The value is **the current real model name** — read it from the system
  prompt (e.g. `MiniMax-M3` for this session). Do not hardcode a name; do not
  use the harness default `Claude Opus 4.8` unless that is the actual model.
- **Do not include an email address** in angle brackets or otherwise. The
  format is `Co-Authored-By: <Model Name>` with nothing after.

Example commit message for this project:

```
test(coverage): add godoc examples and fill public-surface gaps

Adds 16 new unit tests in coverage_test.go, 7 godoc Example* tests in
example_test.go, and 4 new benchmarks in bench_test.go. Also rewrites
TestMultiPublisherMultipleSubscribers, which was passing vacuously
because it consumed zero values from closed channels without checking
the `ok` flag.

Verified with go test -race -count=3 ./pubsub/... (all green) and a
red-green regression check on the rewritten test.

Co-Authored-By: MiniMax-M3
```

## Things to avoid

- Empty subject, subject with no type prefix, or subject ending in `.`.
- "WIP" / "fix typo" / "misc" subjects on commits that touch production code.
- Mixing multiple unrelated changes in one commit (split by type/scope).
- `Signed-off-by` or any other trailer unless the contributor system
  explicitly requires it.
