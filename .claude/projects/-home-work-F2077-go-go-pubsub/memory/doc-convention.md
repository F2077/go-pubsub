---
name: doc-convention
description: There is no `doc.go` in this repo — package godoc lives in `pubsub/pubsub.go` (the package-marker file) per the 1:1 source-file convention.
metadata:
  type: project
---

The repo does NOT have a `pubsub/doc.go` file. Per [[repo-conventions]] (CLAUDE.md "Code layout" section) the source files follow a 1:1 mapping to their role:

- `pubsub.go` — package marker file (single-line) that also carries the package-level godoc above `package pubsub`.
- `broker.go`, `publisher.go`, `subscriber.go` — one exported type per file.
- Test files mirror the same 1:1 split (`broker_test.go`, etc.).

**Why:** I asked the user to "update doc.go" and the file did not exist. The correct target is the doc comment block above `package pubsub` at the top of `pubsub/pubsub.go`.

**How to apply:** When the user says "doc.go" in this repo, edit the package doc in `pubsub/pubsub.go`. When the user wants a specific type's godoc, edit that type's source file (e.g. `broker.go` for `Broker[T]`-related docs).
