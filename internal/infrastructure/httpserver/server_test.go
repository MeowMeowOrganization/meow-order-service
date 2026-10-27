package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
)

func TestServerSetsHeaderTimeout(t *testing.T) {
	s := New("127.0.0.1:8081", http.NewServeMux(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if s.ReadHeaderTimeout <= 0 || s.WriteTimeout <= 0 {
		t.Fatal("missing request timeouts")
	}
	if s.Handler == nil {
		t.Fatal("missing HTTP handler")
	}
}
