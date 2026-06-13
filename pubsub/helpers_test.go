package pubsub

import (
	"log/slog"
	"os"
)

// testLogger returns a slog.Logger that writes to stderr at Info level.
// Used by every package-internal test that needs a quiet logger.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// benchLogger returns a slog.Logger that writes to stderr at Error level,
// silencing the lock-trace Debug spam that benchmarks would otherwise produce.
func benchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
