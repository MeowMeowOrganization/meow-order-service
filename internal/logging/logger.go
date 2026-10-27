package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON logger with structured timestamps and levels.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
