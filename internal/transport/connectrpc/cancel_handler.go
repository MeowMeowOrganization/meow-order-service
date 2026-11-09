package connectrpc

import (
	"context"
	"errors"
	"strconv"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	privatepb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
	cancelapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
)

// CancelOrder is the authenticated private boundary. The caller must be a
// trusted Gateway; never expose its account_id field as a public client input.
func (h OrderHandler) CancelOrder(ctx context.Context, req *connect.Request[privatepb.CancelOrderRequest]) (*connect.Response[privatepb.CancelOrderResponse], error) {
	if h.canceler == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("order cancellation not configured"))
	}
	if req == nil || req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, cancelapp.ErrInvalidRequest)
	}
	result, err := h.canceler.Cancel(ctx, req.Msg.GetAccountId(), req.Msg.GetOrderId())
	if err != nil {
		switch {
		case errors.Is(err, cancelapp.ErrInvalidRequest):
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		case errors.Is(err, cancelapp.ErrNotFound):
			return nil, connect.NewError(connect.CodeNotFound, err)
		case errors.Is(err, cancelapp.ErrAdmissionPending):
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		case errors.Is(err, cancelapp.ErrProvenance):
			return nil, connect.NewError(connect.CodeInternal, err)
		default:
			// Including commit-unknown: never manufacture a successful request.
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
	}
	status := privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_UNSPECIFIED
	switch result.Status {
	case cancelapp.Pending:
		status = privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_PENDING
	case cancelapp.Confirmed:
		status = privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_CONFIRMED
	case cancelapp.Rejected:
		status = privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_REJECTED
	case cancelapp.NotCancellable:
		status = privatepb.CancelRequestStatus_CANCEL_REQUEST_STATUS_NOT_CANCELLABLE
	default:
		return nil, connect.NewError(connect.CodeInternal, errors.New("unknown cancellation status"))
	}
	life := orderpb.OrderStatus_ORDER_STATUS_UNSPECIFIED
	switch result.LifecycleStatus {
	case "ACCEPTED":
		life = orderpb.OrderStatus_ORDER_STATUS_ACCEPTED
	case "OPEN":
		life = orderpb.OrderStatus_ORDER_STATUS_OPEN
	case "PARTIALLY_FILLED":
		life = orderpb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
	case "FILLED":
		life = orderpb.OrderStatus_ORDER_STATUS_FILLED
	case "CANCELED":
		life = orderpb.OrderStatus_ORDER_STATUS_CANCELED
	case "REJECTED":
		life = orderpb.OrderStatus_ORDER_STATUS_REJECTED
	default:
		return nil, connect.NewError(connect.CodeInternal, errors.New("unknown order lifecycle"))
	}
	return connect.NewResponse(&privatepb.CancelOrderResponse{
		OrderId: strconv.FormatInt(result.OrderID, 10), Status: status,
		LifecycleStatus: life, RejectionReason: result.RejectionReason,
	}), nil
}
