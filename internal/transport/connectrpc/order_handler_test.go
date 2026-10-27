package connectrpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
)

func TestOrderRPCReturnsUnimplemented(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, OrderHandler{})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := connect.NewClient[orderpb.SubmitLimitOrderRequest, orderpb.SubmitLimitOrderResponse](
		http.DefaultClient, server.URL+SubmitLimitOrderProcedure,
	)
	_, err := client.CallUnary(context.Background(), connect.NewRequest(&orderpb.SubmitLimitOrderRequest{}))
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("code=%v, err=%v", connect.CodeOf(err), err)
	}
}

func TestOrderRouteDoesNotRegisterPublicGatewayRPC(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, OrderHandler{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/meow.exchange.api.v1alpha1.TradingService/SubmitLimitOrder", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected gateway route: status=%d", rec.Code)
	}
}
