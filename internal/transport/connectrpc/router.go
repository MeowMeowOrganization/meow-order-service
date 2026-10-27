package connectrpc

import (
	"net/http"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
)

// Procedure must exactly match meow-contracts/proto/meow/exchange/internal/order/v1alpha1/order_service.proto.
// The service schema and message types are owned by meow-contracts, not this repository.
const SubmitLimitOrderProcedure = "/meow.exchange.internal.order.v1alpha1.OrderService/SubmitLimitOrder"

func Register(mux *http.ServeMux, handler OrderHandler) {
	mux.Handle(SubmitLimitOrderProcedure, connect.NewUnaryHandler[
		orderpb.SubmitLimitOrderRequest, orderpb.SubmitLimitOrderResponse,
	](SubmitLimitOrderProcedure, handler.SubmitLimitOrder))
}
