// Package privatequery implements account-scoped, read-only order and execution
// queries. Admission, matching results and settlement remain separate owners.
package privatequery

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid private query")
	ErrNotFound = errors.New("order not found")
)

const maxPageSize = 100

// Amounts and matcher sequences are decimal strings, not floats. Empty atom
// fields mean no immutable market snapshot was established during admission.
type Order struct {
	ID                                                            int64
	AccountID, ClientOrderID, MarketID                            string
	Side, TimeInForce                                             int32
	PriceDecimal, QuantityDecimal                                 string
	PriceUnits, QuantityBaseAtoms                                 string
	FilledBaseAtoms, CancelledBaseAtoms, RemainingBaseAtoms       string
	TotalLots, FilledLots, CancelledLots                          int64
	Status, AdmissionStatus, AdmissionRejection, MatcherRejection string
	CancelStatus, CancelRejection                                 string
	Version                                                       int64
	MarketSequence                                                string
	CreatedAt, UpdatedAt                                          time.Time
	OpenedAt, TerminalAt                                          *time.Time
}

type Trade struct {
	OrderID                       int64
	MarketID                      string
	NumericMarketID               uint32
	MarketSequence, EventOrdinal  string
	Role                          string // MAKER or TAKER; the counterparty's order ID is not exposed
	Side                          int32
	PriceTicks, QuantityLots      int64
	PriceUnits, QuantityBaseAtoms string
	AppliedAt                     time.Time
}

type OrderCursor struct {
	CreatedAt time.Time
	ID        int64
}
type TradeCursor struct {
	AppliedAt         time.Time
	MarketID          uint32
	Sequence, Ordinal uint64
	Role              string
}
type OrdersFilter struct {
	AccountID, MarketID string
	OpenOnly            bool
	Limit               int
	After               *OrderCursor
}
type TradesFilter struct {
	AccountID, MarketID string
	OrderID             int64 // 0 means all account-owned orders
	Limit               int
	After               *TradeCursor
}

type Repository interface {
	FindOrder(context.Context, string, int64) (Order, error)
	ListOrders(context.Context, OrdersFilter) ([]Order, error)
	ListTrades(context.Context, TradesFilter) ([]Trade, error)
}
type Service struct{ repo Repository }

func New(repo Repository) (*Service, error) {
	if repo == nil {
		return nil, errors.New("private query repository is required")
	}
	return &Service{repo: repo}, nil
}
func validAccount(s string) bool { return len(s) > 0 && len(s) <= 128 }
func validMarket(s string) bool  { return len(s) <= 64 }
func ParseOrderID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != s {
		return 0, ErrInvalid
	}
	return id, nil
}
func pageSize(n int32) (int, error) {
	if n < 0 || n > maxPageSize {
		return 0, ErrInvalid
	}
	if n == 0 {
		return 50, nil
	}
	return int(n), nil
}
func (s *Service) GetOrder(ctx context.Context, account, idText string) (Order, error) {
	if s == nil || !validAccount(account) {
		return Order{}, ErrInvalid
	}
	id, err := ParseOrderID(idText)
	if err != nil {
		return Order{}, err
	}
	return s.repo.FindOrder(ctx, account, id)
}

type Page[T any] struct {
	Items []T
	Next  string
}
type cursorPayload struct {
	Version  int    `json:"v"`
	Scope    string `json:"s"`
	Micros   int64  `json:"t"`
	ID       int64  `json:"i,omitempty"`
	MarketID uint32 `json:"m,omitempty"`
	Sequence string `json:"q,omitempty"`
	Ordinal  string `json:"e,omitempty"`
	Role     string `json:"r,omitempty"`
}

func scope(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func encodeCursor(c cursorPayload) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeCursor(token, expected string) (cursorPayload, error) {
	if len(token) > 2048 {
		return cursorPayload{}, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursorPayload{}, ErrInvalid
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	var c cursorPayload
	if err := d.Decode(&c); err != nil {
		return cursorPayload{}, ErrInvalid
	}
	var extra any
	if errors.Is(d.Decode(&extra), io.EOF) && c.Version == 1 && c.Scope == expected && c.Micros > 0 {
		return c, nil
	}
	return cursorPayload{}, ErrInvalid
}
func (s *Service) ListOrders(ctx context.Context, account, market string, size int32, token string, openOnly bool) (Page[Order], error) {
	if s == nil || !validAccount(account) || !validMarket(market) {
		return Page[Order]{}, ErrInvalid
	}
	limit, err := pageSize(size)
	if err != nil {
		return Page[Order]{}, err
	}
	filter := OrdersFilter{AccountID: account, MarketID: market, OpenOnly: openOnly, Limit: limit}
	mode := "history"
	if openOnly {
		mode = "open"
	}
	key := scope("orders", mode, account, market)
	if token != "" {
		c, err := decodeCursor(token, key)
		if err != nil || c.ID <= 0 || c.MarketID != 0 || c.Role != "" || c.Sequence != "" || c.Ordinal != "" {
			return Page[Order]{}, ErrInvalid
		}
		filter.After = &OrderCursor{CreatedAt: time.UnixMicro(c.Micros).UTC(), ID: c.ID}
	}
	items, err := s.repo.ListOrders(ctx, filter)
	if err != nil {
		return Page[Order]{}, err
	}
	if len(items) > limit+1 {
		return Page[Order]{}, fmt.Errorf("private query repository returned oversized page")
	}
	out := Page[Order]{Items: items}
	if len(items) > limit {
		out.Items = items[:limit]
		last := out.Items[len(out.Items)-1]
		out.Next = encodeCursor(cursorPayload{Version: 1, Scope: key, Micros: last.CreatedAt.UnixMicro(), ID: last.ID})
	}
	return out, nil
}
func (s *Service) ListTrades(ctx context.Context, account, market, orderID string, size int32, token string) (Page[Trade], error) {
	if s == nil || !validAccount(account) || !validMarket(market) {
		return Page[Trade]{}, ErrInvalid
	}
	limit, err := pageSize(size)
	if err != nil {
		return Page[Trade]{}, err
	}
	filter := TradesFilter{AccountID: account, MarketID: market, Limit: limit}
	if orderID != "" {
		filter.OrderID, err = ParseOrderID(orderID)
		if err != nil {
			return Page[Trade]{}, err
		}
		// An unknown or foreign order is NOT an empty page: it is NOT_FOUND.
		if _, err := s.repo.FindOrder(ctx, account, filter.OrderID); err != nil {
			return Page[Trade]{}, err
		}
	}
	key := scope("trades", account, market, strconv.FormatInt(filter.OrderID, 10))
	if token != "" {
		c, err := decodeCursor(token, key)
		if err != nil || c.ID != 0 || c.MarketID == 0 || c.Role != "MAKER" && c.Role != "TAKER" {
			return Page[Trade]{}, ErrInvalid
		}
		seq, er1 := strconv.ParseUint(c.Sequence, 10, 64)
		ordinal, er2 := strconv.ParseUint(c.Ordinal, 10, 64)
		if er1 != nil || er2 != nil || seq == 0 || c.Sequence != strconv.FormatUint(seq, 10) || c.Ordinal != strconv.FormatUint(ordinal, 10) {
			return Page[Trade]{}, ErrInvalid
		}
		filter.After = &TradeCursor{AppliedAt: time.UnixMicro(c.Micros).UTC(), MarketID: c.MarketID, Sequence: seq, Ordinal: ordinal, Role: c.Role}
	}
	items, err := s.repo.ListTrades(ctx, filter)
	if err != nil {
		return Page[Trade]{}, err
	}
	if len(items) > limit+1 {
		return Page[Trade]{}, fmt.Errorf("private query repository returned oversized page")
	}
	out := Page[Trade]{Items: items}
	if len(items) > limit {
		out.Items = items[:limit]
		last := out.Items[len(out.Items)-1]
		out.Next = encodeCursor(cursorPayload{Version: 1, Scope: key, Micros: last.AppliedAt.UnixMicro(), MarketID: last.NumericMarketID, Sequence: last.MarketSequence, Ordinal: last.EventOrdinal, Role: last.Role})
	}
	return out, nil
}
