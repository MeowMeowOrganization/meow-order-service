package connectrpc

import (
	"net/http"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
)

// Procedure must exactly match meow-contracts/proto/meow/exchange/private/order/v1alpha1/order_service.proto.
// The service schema and message types are owned by meow-contracts, not this repository.
const (
	SubmitLimitOrderProcedure = "/meow.exchange.private.order.v1alpha1.OrderService/SubmitLimitOrder"
	GetOrderProcedure         = "/meow.exchange.private.order.v1alpha1.OrderService/GetOrder"
	GetOpenOrdersProcedure    = "/meow.exchange.private.order.v1alpha1.OrderService/GetOpenOrders"
	GetOrderHistoryProcedure  = "/meow.exchange.private.order.v1alpha1.OrderService/GetOrderHistory"
	GetOrderTradesProcedure   = "/meow.exchange.private.order.v1alpha1.OrderService/GetOrderTrades"
	GetTradeHistoryProcedure  = "/meow.exchange.private.order.v1alpha1.OrderService/GetTradeHistory"
	CancelOrderProcedure      = "/meow.exchange.private.order.v1alpha1.OrderService/CancelOrder"
)

func Register(mux *http.ServeMux, handler OrderHandler) {
	mux.Handle(SubmitLimitOrderProcedure, connect.NewUnaryHandler[
		orderpb.SubmitLimitOrderRequest, orderpb.SubmitLimitOrderResponse,
	](SubmitLimitOrderProcedure, handler.SubmitLimitOrder))
	mux.Handle(CancelOrderProcedure, connect.NewUnaryHandler[
		orderpb.CancelOrderRequest, orderpb.CancelOrderResponse,
	](CancelOrderProcedure, handler.CancelOrder))
	mux.Handle(GetOrderProcedure, connect.NewUnaryHandler[
		orderpb.GetOrderRequest, orderpb.GetOrderResponse,
	](GetOrderProcedure, handler.GetOrder))
	mux.Handle(GetOpenOrdersProcedure, connect.NewUnaryHandler[
		orderpb.ListOrdersRequest, orderpb.ListOrdersResponse,
	](GetOpenOrdersProcedure, handler.GetOpenOrders))
	mux.Handle(GetOrderHistoryProcedure, connect.NewUnaryHandler[
		orderpb.ListOrdersRequest, orderpb.ListOrdersResponse,
	](GetOrderHistoryProcedure, handler.GetOrderHistory))
	mux.Handle(GetOrderTradesProcedure, connect.NewUnaryHandler[
		orderpb.GetOrderTradesRequest, orderpb.ListTradesResponse,
	](GetOrderTradesProcedure, handler.GetOrderTrades))
	mux.Handle(GetTradeHistoryProcedure, connect.NewUnaryHandler[
		orderpb.GetTradeHistoryRequest, orderpb.ListTradesResponse,
	](GetTradeHistoryProcedure, handler.GetTradeHistory))
}
