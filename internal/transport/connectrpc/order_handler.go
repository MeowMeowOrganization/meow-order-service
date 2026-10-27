package connectrpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
)

// OrderHandler is intentionally unimplemented until the CEX order-admission milestone.
// Never fabricate an accepted order or call accounting/matching from this handler.
type OrderHandler struct{}

func (OrderHandler) SubmitLimitOrder(
	_ context.Context,
	_ *connect.Request[orderpb.SubmitLimitOrderRequest],
) (*connect.Response[orderpb.SubmitLimitOrderResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("order admission is not implemented"))
}
