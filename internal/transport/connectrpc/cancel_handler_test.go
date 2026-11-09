package connectrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	privatepb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
)

type mockCancelStore struct {
	gotAccount string
	gotID      int64
	failure    error
}

func (m *mockCancelStore) RequestCancellation(_ context.Context, account string, id int64) (app.Result, error) {
	m.gotAccount, m.gotID = account, id
	if m.failure != nil {
		return app.Result{}, m.failure
	}
	return app.Result{OrderID: id, Status: app.Pending, LifecycleStatus: "PARTIALLY_FILLED"}, nil
}
func TestCancelOrderPrivateRPC(t *testing.T) {
	repo := &mockCancelStore{}
	service, err := app.New(repo)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, NewOrderHandler(service))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := connect.NewClient[privatepb.CancelOrderRequest, privatepb.CancelOrderResponse](http.DefaultClient, server.URL+CancelOrderProcedure)
	res, err := client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "123"}))
	if err != nil {
		t.Fatal(err)
	}
	if repo.gotAccount != "account-x" || repo.gotID != 123 || res.Msg.GetStatus() != privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_PENDING ||
		res.Msg.GetLifecycleStatus() != orderpb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED {
		t.Fatalf("result %+v; repo %+v", res.Msg, repo)
	}
	_, err = client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "01"}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("invalid identity: %v", err)
	}
	repo.failure = app.ErrNotFound
	_, err = client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "123"}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("foreign order: %v", err)
	}
	repo.failure = app.ErrAdmissionPending
	_, err = client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "123"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("admission pending: %v", err)
	}
	repo.failure = errors.New("database unavailable")
	_, err = client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "123"}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("unavailable database: %v", err)
	}
}
func TestCancelOrderHandlerDisabled(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, OrderHandler{})
	server := httptest.NewServer(mux)
	defer server.Close()
	client := connect.NewClient[privatepb.CancelOrderRequest, privatepb.CancelOrderResponse](http.DefaultClient, server.URL+CancelOrderProcedure)
	_, err := client.CallUnary(context.Background(), connect.NewRequest(&privatepb.CancelOrderRequest{AccountId: "account-x", OrderId: "123"}))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("disabled RPC: %v", err)
	}
}
