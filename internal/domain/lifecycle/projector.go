// Package lifecycle is the pure deterministic projection of committed matching
// results. It has no dependency on PostgreSQL, Kafka, or protobuf.
package lifecycle

import (
	"errors"
	"fmt"
	"math"
)

var ErrInvariant = errors.New("matching lifecycle invariant violated")

type Status string

const (
	Accepted        Status = "ACCEPTED"
	Open            Status = "OPEN"
	PartiallyFilled Status = "PARTIALLY_FILLED"
	Filled          Status = "FILLED"
	Canceled        Status = "CANCELED"
	Rejected        Status = "REJECTED"
)

type Order struct {
	ID         int64
	Market     uint32
	Side       int32 // protobuf BUY=1, SELL=2; domain-independent numeric contract
	PriceTicks int64
	// Frozen market units must agree between maker and taker for each execution.
	PriceTickUnits, QuantityStepAtoms string // exact decimal uint64 snapshots
	BaseScale, QuoteScale, PriceScale int16
	TotalLots                         int64
	FilledLots                        int64
	CancelledLots                     int64
	Status                            Status
	RejectionReason                   string
}

type Kind string

const (
	Execution      Kind = "EXECUTION"
	OrderRejected  Kind = "ORDER_REJECTED"
	OrderCancelled Kind = "ORDER_CANCELLED"
	CancelRejected Kind = "CANCEL_REJECTED"
)

type Event struct {
	Kind                     Kind
	Ordinal                  uint64
	MakerID, TakerID         int64
	TakerSide                int32
	PriceTicks, QuantityLots int64
	OrderID                  int64
	Reason                   string
}

type Result struct {
	Market  uint32
	Place   bool
	OrderID int64
	Events  []Event
}

type Trade struct {
	Ordinal                  uint64
	MakerID, TakerID         int64
	TakerSide                int32
	PriceTicks, QuantityLots int64
}

type Projection struct {
	Orders []Order // only orders changed by this result, ascending id
	Trades []Trade
}

func fail(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvariant, fmt.Sprintf(format, args...))
}

// Apply stages all state changes in memory; a single invariant failure returns
// no projection. The caller must persist the complete projection in one TX.
func Apply(result Result, known map[int64]Order) (Projection, error) {
	var out Projection
	if result.Market == 0 || result.OrderID <= 0 {
		return out, fail("invalid command identity")
	}
	current := make(map[int64]Order, len(known))
	for id, order := range known {
		if id <= 0 || id != order.ID || order.Market != result.Market ||
			(order.Side != 1 && order.Side != 2) || order.TotalLots <= 0 || order.PriceTicks <= 0 ||
			order.FilledLots < 0 || order.CancelledLots < 0 ||
			order.FilledLots > order.TotalLots || order.CancelledLots > order.TotalLots-order.FilledLots {
			return out, fail("invalid frozen order %d", id)
		}
		current[id] = order
	}
	initiating, ok := current[result.OrderID]
	if !ok {
		return out, fail("initiating order missing")
	}
	if result.Place && initiating.Status != Accepted {
		return out, fail("placement not awaiting matcher: %d", result.OrderID)
	}
	if !result.Place && len(result.Events) != 1 {
		return out, fail("cancel must produce exactly one outcome")
	}
	touched := make(map[int64]bool)
	seenRejected, seenCancelled, seenCancelRejected := false, false, false
	for i, event := range result.Events {
		if event.Ordinal != uint64(i) {
			return Projection{}, fail("event ordinals not contiguous")
		}
		switch event.Kind {
		case Execution:
			if !result.Place || seenRejected {
				return Projection{}, fail("execution in invalid command/outcome")
			}
			if event.TakerID != result.OrderID || event.MakerID <= 0 || event.TakerID <= 0 || event.MakerID == event.TakerID ||
				event.QuantityLots <= 0 || event.PriceTicks <= 0 || (event.TakerSide != 1 && event.TakerSide != 2) {
				return Projection{}, fail("invalid execution")
			}
			maker, mok := current[event.MakerID]
			taker, tok := current[event.TakerID]
			if !mok || !tok || maker.Side == taker.Side || taker.Side != event.TakerSide ||
				maker.PriceTicks != event.PriceTicks ||
				maker.PriceTickUnits != taker.PriceTickUnits || maker.QuantityStepAtoms != taker.QuantityStepAtoms ||
				maker.BaseScale != taker.BaseScale || maker.QuoteScale != taker.QuoteScale || maker.PriceScale != taker.PriceScale ||
				(taker.Side == 1 && event.PriceTicks > taker.PriceTicks) ||
				(taker.Side == 2 && event.PriceTicks < taker.PriceTicks) ||
				(maker.Status != Open && maker.Status != PartiallyFilled) ||
				(taker.Status != Accepted && taker.Status != PartiallyFilled) {
				return Projection{}, fail("execution incompatible with orders")
			}
			if event.QuantityLots > maker.TotalLots-maker.FilledLots-maker.CancelledLots ||
				event.QuantityLots > taker.TotalLots-taker.FilledLots-taker.CancelledLots {
				return Projection{}, fail("execution overfills an order")
			}
			maker.FilledLots += event.QuantityLots
			taker.FilledLots += event.QuantityLots
			maker.Status = fillStatus(maker)
			taker.Status = fillStatus(taker)
			current[maker.ID], current[taker.ID] = maker, taker
			touched[maker.ID], touched[taker.ID] = true, true
			out.Trades = append(out.Trades, Trade{Ordinal: event.Ordinal, MakerID: maker.ID, TakerID: taker.ID,
				TakerSide: event.TakerSide, PriceTicks: event.PriceTicks, QuantityLots: event.QuantityLots})
		case OrderRejected:
			if !result.Place || len(result.Events) != 1 || event.OrderID != result.OrderID || event.Reason == "" {
				return Projection{}, fail("invalid placement rejection")
			}
			initiating.Status, initiating.RejectionReason = Rejected, event.Reason
			current[initiating.ID], touched[initiating.ID] = initiating, true
			seenRejected = true
		case OrderCancelled:
			if result.Place || len(result.Events) != 1 || event.OrderID != result.OrderID || event.QuantityLots <= 0 || seenCancelled {
				return Projection{}, fail("invalid cancellation outcome")
			}
			remaining := initiating.TotalLots - initiating.FilledLots - initiating.CancelledLots
			if (initiating.Status != Open && initiating.Status != PartiallyFilled) || remaining != event.QuantityLots {
				return Projection{}, fail("cancel quantity or order state mismatch")
			}
			initiating.CancelledLots += remaining
			initiating.Status = Canceled
			current[initiating.ID], touched[initiating.ID] = initiating, true
			seenCancelled = true
		case CancelRejected:
			if result.Place || len(result.Events) != 1 || event.OrderID != result.OrderID || event.Reason == "" || seenCancelRejected {
				return Projection{}, fail("invalid cancel rejection")
			}
			// A rejection is a command outcome, NOT a lifecycle transition.
			// It is valid only when its reason agrees with the canonical state.
			valid := (event.Reason == "ALREADY_FILLED" && initiating.Status == Filled) ||
				(event.Reason == "ALREADY_CANCELLED" && initiating.Status == Canceled) ||
				(event.Reason == "ALREADY_REJECTED" && initiating.Status == Rejected) ||
				// Cancel may be enqueued while admission is ACCEPTED but the
				// placement later fails at the matcher. UNKNOWN_ORDER then
				// agrees with the committed REJECTED placement provenance.
				// Do not accept UNKNOWN_ORDER for an apparently resting order.
				(event.Reason == "UNKNOWN_ORDER" && initiating.Status == Rejected && initiating.RejectionReason != "")
			if !valid {
				return Projection{}, fail("unreconciled cancel rejection: %s", event.Reason)
			}
			seenCancelRejected = true
		default:
			return Projection{}, fail("unknown event kind %q", event.Kind)
		}
	}
	if result.Place && !seenRejected {
		placed := current[result.OrderID]
		if placed.Status == Accepted {
			placed.Status = Open
			current[placed.ID] = placed
		}
		touched[result.OrderID] = true
	}
	if !result.Place && !seenCancelled && !seenCancelRejected {
		return Projection{}, fail("missing cancellation outcome")
	}
	ids := make([]int64, 0, len(touched))
	for id := range touched {
		ids = append(ids, id)
	}
	// No external sorting/ordering randomness in the output.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	for _, id := range ids {
		o := current[id]
		if o.FilledLots < 0 || o.FilledLots > math.MaxInt64-o.CancelledLots || o.FilledLots+o.CancelledLots > o.TotalLots {
			return Projection{}, fail("quantity overflow")
		}
		out.Orders = append(out.Orders, o)
	}
	return out, nil
}

func fillStatus(o Order) Status {
	if o.FilledLots == o.TotalLots {
		return Filled
	}
	return PartiallyFilled
}
