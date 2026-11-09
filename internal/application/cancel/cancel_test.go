package cancel

import (
	"context"
	"errors"
	"testing"
)

type fakeRepository struct {
	called  int
	account string
	id      int64
}

func (f *fakeRepository) RequestCancellation(_ context.Context, account string, id int64) (Result, error) {
	f.called++
	f.account = account
	f.id = id
	return Result{OrderID: id, Status: Pending, LifecycleStatus: "OPEN"}, nil
}

func TestCancelRejectsInvalidIdentityBeforeRepository(t *testing.T) {
	f := &fakeRepository{}
	s, _ := New(f)
	for _, id := range []string{"", "0", "-1", "+1", "01", " 1", "1 ", "9223372036854775808", "a"} {
		if _, err := s.Cancel(context.Background(), "account", id); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%q: %v", id, err)
		}
	}
	if _, err := s.Cancel(context.Background(), "", "1"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := s.Cancel(context.Background(), string(make([]byte, 129)), "1"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if f.called != 0 {
		t.Fatalf("invalid identity reached DB %d times", f.called)
	}
}
func TestCancelDelegatesCanonicalAccountScopedID(t *testing.T) {
	f := &fakeRepository{}
	s, _ := New(f)
	r, err := s.Cancel(context.Background(), "acct-1", "123")
	if err != nil || r.OrderID != 123 || r.Status != Pending || f.called != 1 || f.account != "acct-1" || f.id != 123 {
		t.Fatalf("result=%+v err=%v fake=%+v", r, err, *f)
	}
}
