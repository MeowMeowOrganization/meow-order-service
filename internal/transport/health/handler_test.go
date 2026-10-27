package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakePinger struct {
	calls int
	fail  bool
	wait  bool
}

func (p *fakePinger) Ping(ctx context.Context) error {
	p.calls++
	if p.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	if p.fail {
		return errors.New("db unavailable")
	}
	return nil
}

func TestLivenessDoesNotPingDB(t *testing.T) {
	p := &fakePinger{fail: true}
	h := New(p, time.Second)
	rec := httptest.NewRecorder()
	h.Live(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if rec.Code != http.StatusOK || p.calls != 0 {
		t.Fatalf("liveness=%d DB calls=%d", rec.Code, p.calls)
	}
}

func TestReadinessLifecycleAndDBFailure(t *testing.T) {
	p := &fakePinger{}
	h := New(p, time.Second)
	assertStatus := func(want int) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != want {
			t.Fatalf("readiness=%d want=%d", rec.Code, want)
		}
	}
	assertStatus(503) // Not serving until listener is ready.
	if p.calls != 0 {
		t.Fatal("prestart readiness must not touch DB")
	}
	h.MarkServing()
	assertStatus(200)
	p.fail = true
	assertStatus(503)
	h.MarkStopping()
	p.fail = false
	assertStatus(503)
}

func TestReadinessTimesOut(t *testing.T) {
	p := &fakePinger{wait: true}
	h := New(p, time.Millisecond)
	h.MarkServing()
	rec := httptest.NewRecorder()
	h.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestUnsupportedHealthMethods(t *testing.T) {
	h := New(&fakePinger{}, time.Second)
	rec := httptest.NewRecorder()
	h.Live(rec, httptest.NewRequest(http.MethodPost, "/livez", nil))
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatal("expected 405 with Allow header")
	}
}
