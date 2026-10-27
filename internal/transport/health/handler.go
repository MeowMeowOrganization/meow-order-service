package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

type Pinger interface{ Ping(context.Context) error }

// Handler holds independent liveness and readiness endpoints.
type Handler struct {
	pinger  Pinger
	timeout time.Duration
	serving atomic.Bool
}

func New(pinger Pinger, timeout time.Duration) *Handler {
	return &Handler{pinger: pinger, timeout: timeout}
}
func (h *Handler) MarkServing()  { h.serving.Store(true) }
func (h *Handler) MarkStopping() { h.serving.Store(false) }

func allowProbe(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	return true
}

func (h *Handler) Live(w http.ResponseWriter, r *http.Request) {
	if !allowProbe(w, r) {
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"alive"}` + "\n"))
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if !allowProbe(w, r) {
		return
	}
	if !h.serving.Load() {
		unavailable(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	if err := h.pinger.Ping(ctx); err != nil || ctx.Err() != nil || !h.serving.Load() {
		unavailable(w)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}` + "\n"))
}

func unavailable(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"status":"not_ready"}` + "\n"))
}
