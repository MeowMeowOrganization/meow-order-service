package privatequery

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type fakeRepo struct {
	orders []Order
	trades []Trade
	found  map[int64]Order
}

func (f *fakeRepo) FindOrder(_ context.Context, account string, id int64) (Order, error) {
	o, ok := f.found[id]
	if !ok || o.AccountID != account {
		return Order{}, ErrNotFound
	}
	return o, nil
}
func (f *fakeRepo) ListOrders(_ context.Context, q OrdersFilter) ([]Order, error) {
	out := []Order{}
	for _, o := range f.orders {
		if o.AccountID != q.AccountID || q.MarketID != "" && o.MarketID != q.MarketID {
			continue
		}
		if q.OpenOnly && o.Status != "OPEN" && o.Status != "PARTIALLY_FILLED" {
			continue
		}
		if q.After != nil && !(o.CreatedAt.Before(q.After.CreatedAt) || o.CreatedAt.Equal(q.After.CreatedAt) && o.ID < q.After.ID) {
			continue
		}
		out = append(out, o)
		if len(out) == q.Limit+1 {
			break
		}
	}
	return out, nil
}
func (f *fakeRepo) ListTrades(_ context.Context, q TradesFilter) ([]Trade, error) {
	out := []Trade{}
	for _, tr := range f.trades {
		o := f.found[tr.OrderID]
		if o.AccountID != q.AccountID || q.MarketID != "" && tr.MarketID != q.MarketID || q.OrderID != 0 && tr.OrderID != q.OrderID {
			continue
		}
		if q.After != nil && !(tr.AppliedAt.Before(q.After.AppliedAt) || tr.AppliedAt.Equal(q.After.AppliedAt) && tr.NumericMarketID == q.After.MarketID && tr.EventOrdinal < strconv.FormatUint(q.After.Ordinal, 10)) {
			continue
		}
		out = append(out, tr)
		if len(out) == q.Limit+1 {
			break
		}
	}
	return out, nil
}
func TestPrivateOrdersScopedKeysetAndInputValidation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := &fakeRepo{found: map[int64]Order{}}
	for i := int64(5); i >= 1; i-- {
		o := Order{ID: i, AccountID: "mine", MarketID: "BTC-USDT", Status: "OPEN", CreatedAt: now}
		repo.orders = append(repo.orders, o)
		repo.found[i] = o
	}
	svc, _ := New(repo)
	first, err := svc.ListOrders(context.Background(), "mine", "BTC-USDT", 2, "", true)
	if err != nil || len(first.Items) != 2 || first.Items[0].ID != 5 || first.Items[1].ID != 4 || first.Next == "" {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := svc.ListOrders(context.Background(), "mine", "BTC-USDT", 2, first.Next, true)
	if err != nil || len(second.Items) != 2 || second.Items[0].ID != 3 {
		t.Fatalf("second %+v %v", second, err)
	}
	third, err := svc.ListOrders(context.Background(), "mine", "BTC-USDT", 2, second.Next, true)
	if err != nil || len(third.Items) != 1 || third.Items[0].ID != 1 || third.Next != "" {
		t.Fatalf("third %+v %v", third, err)
	}
	for _, tc := range []struct {
		account, market string
		page            int32
		token           string
	}{
		{"other", "BTC-USDT", 2, first.Next}, {"mine", "ETH-USDT", 2, first.Next},
		{"mine", "BTC-USDT", 2, "bad token"}, {"mine", "BTC-USDT", 101, ""},
		{"", "", 1, ""},
	} {
		if _, err := svc.ListOrders(context.Background(), tc.account, tc.market, tc.page, tc.token, true); !errors.Is(err, ErrInvalid) {
			t.Errorf("expected invalid %v, got %v", tc, err)
		}
	}
	if _, err := svc.GetOrder(context.Background(), "other", "5"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for _, bad := range []string{"0", "01", "+1", "-1", "18446744073709551615", ""} {
		if _, err := ParseOrderID(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("ID %q %v", bad, err)
		}
	}
}
func TestPrivateTradesScopedCursorAndExistence(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := &fakeRepo{found: map[int64]Order{1: {ID: 1, AccountID: "mine", MarketID: "BTC-USDT"}}}
	repo.trades = []Trade{
		{OrderID: 1, MarketID: "BTC-USDT", NumericMarketID: 7, MarketSequence: "2", EventOrdinal: "1", Role: "TAKER", AppliedAt: now},
		{OrderID: 1, MarketID: "BTC-USDT", NumericMarketID: 7, MarketSequence: "2", EventOrdinal: "0", Role: "TAKER", AppliedAt: now},
	}
	svc, _ := New(repo)
	first, err := svc.ListTrades(context.Background(), "mine", "", "1", 1, "")
	if err != nil || len(first.Items) != 1 || first.Next == "" {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := svc.ListTrades(context.Background(), "mine", "", "1", 1, first.Next)
	if err != nil || len(second.Items) != 1 || second.Items[0].EventOrdinal != "0" || second.Next != "" {
		t.Fatalf("second %+v %v", second, err)
	}
	if _, err := svc.ListTrades(context.Background(), "mine", "BTC-USDT", "", 1, first.Next); !errors.Is(err, ErrInvalid) {
		t.Fatalf("filter reuse: %v", err)
	}
	if _, err := svc.ListTrades(context.Background(), "foreign", "", "1", 1, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign: %v", err)
	}
}

func TestPrivateCursorUnsigned64Boundary(t *testing.T) {
	repo := &fakeRepo{found: map[int64]Order{1: {ID: 1, AccountID: "mine", MarketID: "BTC-USDT"}}}
	svc, _ := New(repo)
	key := scope("trades", "mine", "", "0")
	good := encodeCursor(cursorPayload{Version: 1, Scope: key, Micros: time.Now().UnixMicro(),
		MarketID: 7, Sequence: "18446744073709551615", Ordinal: "18446744073709551615", Role: "MAKER"})
	if _, err := svc.ListTrades(context.Background(), "mine", "", "", 1, good); err != nil {
		t.Fatalf("valid uint64 max cursor: %v", err)
	}
	bad := encodeCursor(cursorPayload{Version: 1, Scope: key, Micros: time.Now().UnixMicro(),
		MarketID: 7, Sequence: "18446744073709551616", Ordinal: "0", Role: "MAKER"})
	if _, err := svc.ListTrades(context.Background(), "mine", "", "", 1, bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overflow cursor: %v", err)
	}
}
