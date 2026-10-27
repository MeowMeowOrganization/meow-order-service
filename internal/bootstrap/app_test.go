package bootstrap

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

type fakeReadyState struct{ serving bool }

func (s *fakeReadyState) MarkServing()  { s.serving = true }
func (s *fakeReadyState) MarkStopping() { s.serving = false }

func TestServeStopsOnCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := &fakeReadyState{}
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := &http.Server{Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}
	go func() { done <- serve(ctx, listener, srv, ready, time.Second, logger) }()

	// Establish that the listener is serving before triggering shutdown.
	testCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	for {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if testCtx.Err() != nil {
			t.Fatal("server failed to start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
	if ready.serving {
		t.Fatal("still ready after shutdown")
	}
}
