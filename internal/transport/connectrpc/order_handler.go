package connectrpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
	cancelapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
	queryapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
)

// OrderHandler is intentionally unimplemented until the CEX order-admission milestone.
// Never fabricate an accepted order or call accounting/matching from this handler.
type OrderHandler struct {
	canceler *cancelapp.Service
	query    *queryapp.Service
}

// WithPrivateQueries installs the read-only query service independently of
// cancellation worker activation.
func (h OrderHandler) WithPrivateQueries(q *queryapp.Service) OrderHandler { h.query = q; return h }

func NewOrderHandler(canceler *cancelapp.Service) OrderHandler {
	return OrderHandler{canceler: canceler}
}

func (OrderHandler) SubmitLimitOrder(
	_ context.Context,
	_ *connect.Request[orderpb.SubmitLimitOrderRequest],
) (*connect.Response[orderpb.SubmitLimitOrderResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("order admission is not implemented"))
}
