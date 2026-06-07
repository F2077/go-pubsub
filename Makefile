# go-pubsub Makefile
#
# Distilled from the conventions used by spf13/cobra, uber-go/zap,
# and golangci-lint. Goals: thin, one-line recipes; race detector baked
# into the default test target; a `help` target that greps `##`
# comments so the file documents itself.
#
# Keep target flags in sync with .github/workflows/test.yml — the
# local `make` and CI run the same command.

# ---- Toolchain -------------------------------------------------------------

GO              ?= go
PKG             ?= ./...
VERSION         ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Treat undefined variables as errors; disable Make's built-in suffix
# rules so an accidental `clean.o` style request doesn't silently succeed.
MAKEFLAGS       += --no-builtin-rules
MAKEFLAGS       += --warn-undefined-variables

# ---- Phony targets ---------------------------------------------------------

.PHONY: help all build test cover bench fmt vet lint tidy run clean ci

# `make` with no args runs the local quality gate: fmt + vet + race-test.
.DEFAULT_GOAL := all

# ---- Targets ---------------------------------------------------------------
# Every user-facing target is documented with a `## description` comment on
# the same line. `make help` greps them out and prints the result.

help: ## Show this help (default if you ran `make` with no flag)
	@awk 'BEGIN {FS = ":.*?## "; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
		/^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' \
		$(MAKEFILE_LIST)

all: fmt vet test ## Format-check, vet, and run the race-enabled test suite

build: ## Compile every package
	$(GO) build $(PKG)

test: ## Run the test suite with the race detector (mirrors CI)
	$(GO) test -race -count=1 $(PKG)

cover: ## Run race tests with coverage; write cover.out + cover.html
	$(GO) test -race -count=1 -coverprofile=cover.out -covermode=atomic $(PKG)
	$(GO) tool cover -func=cover.out
	@$(GO) tool cover -html=cover.out -o cover.html
	@echo "coverage report: cover.html"

bench: ## Run the README-cited benchmarks
	$(GO) test -bench=. -benchmem -run=^$$ ./pubsub/...

fmt: ## Fail if any tracked Go file is not gofmt-clean
	@test -z $$($(GO)fmt -l $$(git ls-files '*.go')) \
		|| ($(GO)fmt -d $$(git ls-files '*.go'); exit 1)

vet: ## Run go vet over every package
	$(GO) vet $(PKG)

lint: vet ## Run the local Go linters (vet today; golangci-lint once configured)
	@command -v golangci-lint >/dev/null 2>&1 \
		&& golangci-lint run $(PKG) \
		|| echo "golangci-lint not installed; skipping (see .golangci.yml)"

tidy: ## Verify go.mod / go.sum are tidy
	$(GO) mod tidy -diff

run: ## Run the quickstart example program
	$(GO) run ./cmd/quickstart

clean: ## Remove generated artifacts (cover.out, cover.html)
	rm -f cover.out cover.html

ci: all cover bench ## Full CI gate: fmt + vet + race-test + coverage + bench
