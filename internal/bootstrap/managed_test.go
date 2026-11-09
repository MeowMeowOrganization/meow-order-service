package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type atomicReadyState struct{ active atomic.Bool }

func (s *atomicReadyState) MarkServing()  { s.active.Store(true) }
func (s *atomicReadyState) MarkStopping() { s.active.Store(false) }

func TestWorkerFailureStopsHTTPAndPeerWorker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	readiness := &atomicReadyState{}
	fail := errors.New("matching sequence gap")
	result := runManaged(context.Background(), ln, &http.Server{Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second}, readiness,
		2*time.Second, logger, []backgroundTask{
			{name: "lifecycle", run: func(context.Context) error { return fail }},
			{name: "outbox", run: func(ctx context.Context) error { <-ctx.Done(); stopped.Store(true); return nil }},
		})
	if !errors.Is(result, fail) || !strings.Contains(result.Error(), "lifecycle") || !stopped.Load() || readiness.active.Load() {
		t.Fatalf("failure not propagated/cleaned up: err=%v stopped=%v ready=%v", result, stopped.Load(), readiness.active.Load())
	}
}
func TestManagedWorkersExitOnShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- runManaged(ctx, ln, &http.Server{Handler: http.NewServeMux(), ReadHeaderTimeout: time.Second},
			&atomicReadyState{}, time.Second, logger, []backgroundTask{{name: "lifecycle", run: func(ctx context.Context) error { <-ctx.Done(); return nil }}})
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed to stop")
	}
}
