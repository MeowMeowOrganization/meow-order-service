package connectrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	pb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
	query "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
)

type fakePrivateStore struct {
	calls int
	fail  error
}

func (f *fakePrivateStore) FindOrder(_ context.Context, account string, id int64) (query.Order, error) {
	if f.fail != nil {
		return query.Order{}, f.fail
	}
	if account != "account-a" || id != 17 {
		return query.Order{}, query.ErrNotFound
	}
	return query.Order{ID: 17, AccountID: account, ClientOrderID: "client-a", MarketID: "BTC-USDT",
		Side: 1, TimeInForce: 1, PriceDecimal: "60000.1", QuantityDecimal: "0.004",
		PriceUnits: "6000010", QuantityBaseAtoms: "400000", FilledBaseAtoms: "200000",
		RemainingBaseAtoms: "200000", TotalLots: 4, FilledLots: 2, Status: "PARTIALLY_FILLED",
		AdmissionStatus: "ACCEPTED", MarketSequence: "18446744073709551614",
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, nil
}
func (f *fakePrivateStore) ListOrders(_ context.Context, filter query.OrdersFilter) ([]query.Order, error) {
	f.calls++
	if f.fail != nil {
		return nil, f.fail
	}
	if filter.AccountID != "account-a" {
		return nil, nil
	}
	o, _ := f.FindOrder(context.Background(), "account-a", 17)
	return []query.Order{o}, nil
}
func (f *fakePrivateStore) ListTrades(_ context.Context, filter query.TradesFilter) ([]query.Trade, error) {
	if f.fail != nil {
		return nil, f.fail
	}
	if filter.AccountID != "account-a" {
		return nil, nil
	}
	return []query.Trade{{OrderID: 17, MarketID: "BTC-USDT", NumericMarketID: 7,
		MarketSequence: "18446744073709551614", EventOrdinal: "0", Role: "MAKER",
		Side: 1, PriceTicks: 1200002, QuantityLots: 2, PriceUnits: "6000010", QuantityBaseAtoms: "200000",
		AppliedAt: time.Now().UTC()}}, nil
}
func TestPrivateQueryConnectRPC(t *testing.T) {
	repo := &fakePrivateStore{}
	svc, err := query.New(repo)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, NewOrderHandler(nil).WithPrivateQueries(svc))
	server := httptest.NewServer(mux)
	defer server.Close()
	ctx := context.Background()
	get := connect.NewClient[pb.GetOrderRequest, pb.GetOrderResponse](server.Client(), server.URL+GetOrderProcedure)
	order, err := get.CallUnary(ctx, connect.NewRequest(&pb.GetOrderRequest{AccountId: "account-a", OrderId: "17"}))
	if err != nil || order.Msg.GetOrder().GetStatus() != pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_PARTIALLY_FILLED ||
		order.Msg.GetOrder().GetLastMarketSequence() != "18446744073709551614" {
		t.Fatalf("get %+v %v", order, err)
	}
	_, err = get.CallUnary(ctx, connect.NewRequest(&pb.GetOrderRequest{AccountId: "account-b", OrderId: "17"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("account isolation: %v", err)
	}
	_, err = get.CallUnary(ctx, connect.NewRequest(&pb.GetOrderRequest{AccountId: "account-a", OrderId: "017"}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("canonical ID: %v", err)
	}
	for _, procedure := range []string{GetOpenOrdersProcedure, GetOrderHistoryProcedure} {
		client := connect.NewClient[pb.ListOrdersRequest, pb.ListOrdersResponse](server.Client(), server.URL+procedure)
		res, err := client.CallUnary(ctx, connect.NewRequest(&pb.ListOrdersRequest{AccountId: "account-a", PageSize: 2}))
		if err != nil || len(res.Msg.GetOrders()) != 1 {
			t.Fatalf("%s: %v %+v", procedure, err, res)
		}
		_, err = client.CallUnary(ctx, connect.NewRequest(&pb.ListOrdersRequest{AccountId: "account-a", PageSize: 101}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("page size: %v", err)
		}
	}
	own := connect.NewClient[pb.GetOrderTradesRequest, pb.ListTradesResponse](server.Client(), server.URL+GetOrderTradesProcedure)
	trades, err := own.CallUnary(ctx, connect.NewRequest(&pb.GetOrderTradesRequest{AccountId: "account-a", OrderId: "17"}))
	if err != nil || len(trades.Msg.GetTrades()) != 1 || trades.Msg.GetTrades()[0].GetLiquidityRole() != pb.LiquidityRole_LIQUIDITY_ROLE_MAKER {
		t.Fatalf("own trades: %+v %v", trades, err)
	}
	_, err = own.CallUnary(ctx, connect.NewRequest(&pb.GetOrderTradesRequest{AccountId: "account-b", OrderId: "17"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("foreign trades: %v", err)
	}
	all := connect.NewClient[pb.GetTradeHistoryRequest, pb.ListTradesResponse](server.Client(), server.URL+GetTradeHistoryProcedure)
	trades, err = all.CallUnary(ctx, connect.NewRequest(&pb.GetTradeHistoryRequest{AccountId: "account-a", MarketId: "BTC-USDT"}))
	if err != nil || len(trades.Msg.GetTrades()) != 1 {
		t.Fatalf("trade history: %+v %v", trades, err)
	}
	repo.fail = errors.New("database offline")
	_, err = get.CallUnary(ctx, connect.NewRequest(&pb.GetOrderRequest{AccountId: "account-a", OrderId: "17"}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("db failure code: %v", err)
	}
}
func TestPrivateQueryHandlerUnconfigured(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, OrderHandler{})
	server := httptest.NewServer(mux)
	defer server.Close()
	client := connect.NewClient[pb.GetOrderRequest, pb.GetOrderResponse](server.Client(), server.URL+GetOrderProcedure)
	_, err := client.CallUnary(context.Background(), connect.NewRequest(&pb.GetOrderRequest{AccountId: "account-a", OrderId: "1"}))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("unconfigured: %v", err)
	}
}
