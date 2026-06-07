package pubsub

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs a goroutine-leak guard for every test in this
// package (including the external `package pubsub_test` tests in this
// directory — Go compiles all _test.go files in a directory into one
// test binary, so a single TestMain covers both). Any goroutine the
// library spawns and does not shut down before the test binary exits
// is reported as a failure.
//
// This is the standard `go.uber.org/goleak` pattern: see
// https://pkg.go.dev/go.uber.org/goleak#VerifyTestMain for the exact
// semantics. The default ignore list covers `testing.runTests` and a
// few other test-runner internals; add project-specific ignores via
// goleak.IgnoreTopFunction if future code legitimately spawns long-lived
// goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
