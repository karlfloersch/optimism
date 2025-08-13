package supervisor

import (
    "io"
    "log/slog"
    "github.com/ethereum/go-ethereum/log"
)

// testLogger returns a quiet logger for unit tests.
func testLogger() log.Logger {
    return log.NewLogger(slog.NewTextHandler(io.Discard, nil))
}


