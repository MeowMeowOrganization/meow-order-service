package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestStructuredLoggingAndLevel(t *testing.T) {
	var output bytes.Buffer
	logger := New(&output, slog.LevelInfo)
	logger.Debug("hidden")
	logger.Info("startup", "component", "rpc")
	var line map[string]any
	if err := json.Unmarshal(output.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line["msg"] != "startup" || line["component"] != "rpc" || line["level"] != "INFO" {
		t.Fatalf("unexpected JSON output: %v", line)
	}
}
