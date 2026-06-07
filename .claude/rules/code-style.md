# Code Style: Comment and Message Language

This repository uses a deliberate language split between **comments that are
part of the public contract** and **comments that are for the maintainers**,
plus a strict rule for **user-visible strings**. Read this before writing or
editing any Go file.

## 1. Comments

### Exported API — English

All documentation that end users can see on pkg.go.dev must be in **English**:

- `doc.go` files
- Comments immediately above exported `func`, `type`, `var`, `const`
  declarations
- Comments on exported struct fields
- Godoc `// Example*` comments in `*_test.go` files
- README content, CHANGELOG entries, and other public-facing docs

These comments are the package's public contract; a non-Chinese-speaking
consumer must be able to read them without translation.

### Internal / implementation — 中文 (Chinese)

Everything else uses **Chinese**:

- Comments inside function bodies
- Comments on unexported `func`, `type`, `var`, `const`
- Inline `// note:` explanations
- Comments in `_test.go` files (other than the `Example*` godoc blocks)
- TODOs, FIXMEs, and similar maintainer notes

Rationale: the maintainers write Chinese, and the internal narrative is for
them. Forcing everything into English would slow them down and add no value
to a non-Chinese-speaking consumer (who never reads internal comments).

## 2. User-visible strings — English (no exceptions)

The following must always be in **English**, regardless of audience:

- `errors.New(...)` and `fmt.Errorf(...)` argument text
- `slog` / `log` / `log/slog` message strings
- `panic(...)` argument text
- Anything written to `os.Stdout` / `os.Stderr` / `fmt.Println` etc.
- `String()` methods on exported types (e.g. `Broker.String()`)
- Error sentinels and `*Error` types' public-facing messages

This rule wins over the "internal comments in Chinese" rule above. The line
is: **if a user can see it at runtime, it is English.**

## 3. Quick reference

| Location | Language |
|---|---|
| `doc.go` | English |
| Comment above exported `func/type/var/const` | English |
| Comment above unexported `func/type/var/const` | 中文 |
| Inline body comment, TODO, FIXME | 中文 |
| `Example*` godoc comment in test files | English |
| Other test-file comments | 中文 |
| `errors.New` / `fmt.Errorf` text | English |
| `slog` / `log` messages | English |
| `panic(...)` text | English |
| `fmt.Println` / stdout / stderr | English |
| `String()` method on exported type | English |

## 4. Editing checklist

Before submitting a change that touches comments or strings:

- Did I put English on every exported declaration that the user can see?
- Did I put 中文 on the internal logic notes and unexported helpers?
- Did I leave the `errors.New` / `slog` / `panic` / `String()` text in
  English?
- If I added a new `Example*` test, is the comment block in English and the
  example name in the form `ExampleType_Method` (see
  `/claude/CLAUDE.md` → "Testing conventions")?
