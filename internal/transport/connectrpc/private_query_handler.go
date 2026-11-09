package connectrpc

import (
	"context"
	"errors"
	"strconv"
	"time"

	"connectrpc.com/connect"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	pb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/order/v1alpha1"
	query "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func queryError(err error) error {
	switch {
	case errors.Is(err, query.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, query.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	default:
		return connect.NewError(connect.CodeUnavailable, errors.New("private order query unavailable"))
	}
}
func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }
func optTS(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return ts(*t)
}
func privateOrder(o query.Order) *pb.PrivateOrder {
	status := map[string]pb.PrivateOrderStatus{
		"ADMISSION_PENDING":  pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_ADMISSION_PENDING,
		"ADMISSION_REJECTED": pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_ADMISSION_REJECTED,
		"ACCEPTED":           pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_ACCEPTED,
		"OPEN":               pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_OPEN,
		"PARTIALLY_FILLED":   pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_PARTIALLY_FILLED,
		"FILLED":             pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_FILLED,
		"CANCELED":           pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_CANCELED,
		"REJECTED":           pb.PrivateOrderStatus_PRIVATE_ORDER_STATUS_MATCHER_REJECTED,
	}[o.Status]
	cancelStatus := map[string]pb.CancelRequestStatus{
		"PENDING":   pb.CancelRequestStatus_CANCEL_REQUEST_STATUS_PENDING,
		"CONFIRMED": pb.CancelRequestStatus_CANCEL_REQUEST_STATUS_CONFIRMED,
		"REJECTED":  pb.CancelRequestStatus_CANCEL_REQUEST_STATUS_REJECTED,
	}[o.CancelStatus]
	return &pb.PrivateOrder{
		OrderId: strconv.FormatInt(o.ID, 10), AccountId: o.AccountID,
		ClientOrderId: o.ClientOrderID, MarketId: o.MarketID,
		Side: orderpb.OrderSide(o.Side), TimeInForce: orderpb.TimeInForce(o.TimeInForce),
		PriceDecimal: o.PriceDecimal, QuantityDecimal: o.QuantityDecimal,
		PriceUnits: o.PriceUnits, QuantityBaseAtoms: o.QuantityBaseAtoms,
		FilledBaseAtoms: o.FilledBaseAtoms, CancelledBaseAtoms: o.CancelledBaseAtoms,
		RemainingBaseAtoms: o.RemainingBaseAtoms,
		TotalLots:          o.TotalLots, FilledLots: o.FilledLots, CancelledLots: o.CancelledLots,
		Status: status, AdmissionStatus: o.AdmissionStatus,
		AdmissionRejectionCode: o.AdmissionRejection,
		MatcherRejectionReason: o.MatcherRejection,
		CancelRequestStatus:    cancelStatus, CancelRejectionReason: o.CancelRejection,
		LifecycleVersion: uint64(o.Version), LastMarketSequence: o.MarketSequence,
		CreatedAt: ts(o.CreatedAt), UpdatedAt: ts(o.UpdatedAt),
		OpenedAt: optTS(o.OpenedAt), TerminalAt: optTS(o.TerminalAt),
	}
}
func privateTrade(t query.Trade) *pb.PrivateTrade {
	role := pb.LiquidityRole_LIQUIDITY_ROLE_UNSPECIFIED
	if t.Role == "MAKER" {
		role = pb.LiquidityRole_LIQUIDITY_ROLE_MAKER
	}
	if t.Role == "TAKER" {
		role = pb.LiquidityRole_LIQUIDITY_ROLE_TAKER
	}
	return &pb.PrivateTrade{
		OrderId: strconv.FormatInt(t.OrderID, 10), MarketId: t.MarketID,
		MarketNumericId: t.NumericMarketID, MarketSequence: t.MarketSequence,
		EventOrdinal: t.EventOrdinal, LiquidityRole: role, Side: orderpb.OrderSide(t.Side),
		PriceTicks: t.PriceTicks, QuantityLots: t.QuantityLots,
		PriceUnits: t.PriceUnits, QuantityBaseAtoms: t.QuantityBaseAtoms,
		AppliedAt: ts(t.AppliedAt),
	}
}
func (h OrderHandler) GetOrder(ctx context.Context, req *connect.Request[pb.GetOrderRequest]) (*connect.Response[pb.GetOrderResponse], error) {
	if h.query == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("private queries not configured"))
	}
	if req == nil || req.Msg == nil {
		return nil, queryError(query.ErrInvalid)
	}
	o, err := h.query.GetOrder(ctx, req.Msg.GetAccountId(), req.Msg.GetOrderId())
	if err != nil {
		return nil, queryError(err)
	}
	return connect.NewResponse(&pb.GetOrderResponse{Order: privateOrder(o)}), nil
}
func (h OrderHandler) listOrders(ctx context.Context, req *connect.Request[pb.ListOrdersRequest], openOnly bool) (*connect.Response[pb.ListOrdersResponse], error) {
	if h.query == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("private queries not configured"))
	}
	if req == nil || req.Msg == nil {
		return nil, queryError(query.ErrInvalid)
	}
	page, err := h.query.ListOrders(ctx, req.Msg.GetAccountId(), req.Msg.GetMarketId(), req.Msg.GetPageSize(), req.Msg.GetPageToken(), openOnly)
	if err != nil {
		return nil, queryError(err)
	}
	resp := &pb.ListOrdersResponse{NextPageToken: page.Next}
	for _, o := range page.Items {
		resp.Orders = append(resp.Orders, privateOrder(o))
	}
	return connect.NewResponse(resp), nil
}
func (h OrderHandler) GetOpenOrders(ctx context.Context, req *connect.Request[pb.ListOrdersRequest]) (*connect.Response[pb.ListOrdersResponse], error) {
	return h.listOrders(ctx, req, true)
}
func (h OrderHandler) GetOrderHistory(ctx context.Context, req *connect.Request[pb.ListOrdersRequest]) (*connect.Response[pb.ListOrdersResponse], error) {
	return h.listOrders(ctx, req, false)
}
func (h OrderHandler) GetOrderTrades(ctx context.Context, req *connect.Request[pb.GetOrderTradesRequest]) (*connect.Response[pb.ListTradesResponse], error) {
	if h.query == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("private queries not configured"))
	}
	if req == nil || req.Msg == nil {
		return nil, queryError(query.ErrInvalid)
	}
	if req.Msg.GetOrderId() == "" {
		return nil, queryError(query.ErrInvalid)
	}
	page, err := h.query.ListTrades(ctx, req.Msg.GetAccountId(), "", req.Msg.GetOrderId(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, queryError(err)
	}
	return connect.NewResponse(tradePage(page)), nil
}
func (h OrderHandler) GetTradeHistory(ctx context.Context, req *connect.Request[pb.GetTradeHistoryRequest]) (*connect.Response[pb.ListTradesResponse], error) {
	if h.query == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("private queries not configured"))
	}
	if req == nil || req.Msg == nil {
		return nil, queryError(query.ErrInvalid)
	}
	page, err := h.query.ListTrades(ctx, req.Msg.GetAccountId(), req.Msg.GetMarketId(), "", req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, queryError(err)
	}
	return connect.NewResponse(tradePage(page)), nil
}
func tradePage(page query.Page[query.Trade]) *pb.ListTradesResponse {
	resp := &pb.ListTradesResponse{NextPageToken: page.Next}
	for _, tr := range page.Items {
		resp.Trades = append(resp.Trades, privateTrade(tr))
	}
	return resp
}
