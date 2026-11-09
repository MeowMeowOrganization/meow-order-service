package lifecycle

import (
	"errors"
	"reflect"
	"testing"
)

func fixture() map[int64]Order {
	return map[int64]Order{
		1: {ID: 1, Market: 7, Side: 2, PriceTicks: 90, TotalLots: 4, Status: Open},
		2: {ID: 2, Market: 7, Side: 1, PriceTicks: 100, TotalLots: 10, Status: Accepted},
		3: {ID: 3, Market: 7, Side: 2, PriceTicks: 95, TotalLots: 6, Status: Open},
	}
}
func TestZeroEventPlaceOpens(t *testing.T) {
	o, err := Apply(Result{Market: 7, Place: true, OrderID: 2}, fixture())
	if err != nil || len(o.Orders) != 1 || o.Orders[0].Status != Open {
		t.Fatalf("%+v %v", o, err)
	}
}
func TestMultiMakerAtomicAndImmutableInput(t *testing.T) {
	before := fixture()
	result := Result{Market: 7, Place: true, OrderID: 2, Events: []Event{
		{Kind: Execution, Ordinal: 0, MakerID: 1, TakerID: 2, TakerSide: 1, PriceTicks: 90, QuantityLots: 4},
		{Kind: Execution, Ordinal: 1, MakerID: 3, TakerID: 2, TakerSide: 1, PriceTicks: 95, QuantityLots: 6},
	}}
	out, err := Apply(result, before)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Orders) != 3 || len(out.Trades) != 2 || out.Orders[0].Status != Filled || out.Orders[2].Status != Filled {
		t.Fatalf("%+v", out)
	}
	if !reflect.DeepEqual(before, fixture()) {
		t.Fatal("mutated input")
	}
	result.Events[1].QuantityLots = 7
	bad, err := Apply(result, before)
	if !errors.Is(err, ErrInvariant) || len(bad.Orders) != 0 || len(bad.Trades) != 0 {
		t.Fatalf("not atomic: %+v %v", bad, err)
	}
}
func TestPartialThenCancel(t *testing.T) {
	current := fixture()
	current[2] = Order{ID: 2, Market: 7, Side: 1, PriceTicks: 100, TotalLots: 10, FilledLots: 4, Status: PartiallyFilled}
	r, err := Apply(Result{Market: 7, OrderID: 2, Events: []Event{{Kind: OrderCancelled, OrderID: 2, QuantityLots: 6}}}, current)
	if err != nil || len(r.Orders) != 1 || r.Orders[0].Status != Canceled || r.Orders[0].FilledLots != 4 || r.Orders[0].CancelledLots != 6 {
		t.Fatalf("%+v %v", r, err)
	}
}
func TestCancelRejectedNoTransition(t *testing.T) {
	current := fixture()
	current[2] = Order{ID: 2, Market: 7, Side: 1, PriceTicks: 100, TotalLots: 10, FilledLots: 10, Status: Filled}
	r, err := Apply(Result{Market: 7, OrderID: 2, Events: []Event{{Kind: CancelRejected, OrderID: 2, Reason: "ALREADY_FILLED"}}}, current)
	if err != nil || len(r.Orders) != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := Apply(Result{Market: 7, OrderID: 2, Events: []Event{{Kind: CancelRejected, OrderID: 2, Reason: "UNKNOWN_ORDER"}}}, current); !errors.Is(err, ErrInvariant) {
		t.Fatalf("expected mismatch: %v", err)
	}
}
func TestRejection(t *testing.T) {
	o, err := Apply(Result{Market: 7, Place: true, OrderID: 2, Events: []Event{{Kind: OrderRejected, OrderID: 2, Reason: "DUPLICATE_ORDER_ID"}}}, fixture())
	if err != nil || o.Orders[0].Status != Rejected || o.Orders[0].RejectionReason != "DUPLICATE_ORDER_ID" {
		t.Fatalf("%+v %v", o, err)
	}
}
func TestUnorderedEventRejected(t *testing.T) {
	_, err := Apply(Result{Market: 7, Place: true, OrderID: 2, Events: []Event{{Kind: Execution, Ordinal: 1, MakerID: 1, TakerID: 2, TakerSide: 1, PriceTicks: 90, QuantityLots: 1}}}, fixture())
	if !errors.Is(err, ErrInvariant) {
		t.Fatalf("%v", err)
	}
}
func TestIncompatibleFrozenMarketRulesRejectExecution(t *testing.T) {
	orders := fixture()
	maker := orders[1]
	maker.PriceTickUnits = "5"
	maker.QuantityStepAtoms = "100000"
	orders[1] = maker
	taker := orders[2]
	taker.PriceTickUnits = "10"
	taker.QuantityStepAtoms = "100000"
	orders[2] = taker
	_, err := Apply(Result{Market: 7, Place: true, OrderID: 2, Events: []Event{
		{Kind: Execution, Ordinal: 0, MakerID: 1, TakerID: 2, TakerSide: 1, PriceTicks: 90, QuantityLots: 1},
	}}, orders)
	if !errors.Is(err, ErrInvariant) {
		t.Fatalf("incompatible lot/tick snapshots accepted: %v", err)
	}
}

func TestCancelUnknownOrderOnlyWhenPlacementWasMatcherRejected(t *testing.T) {
	current := fixture()
	initial := current[2]
	initial.Status = Rejected
	initial.RejectionReason = "DUPLICATE_ORDER_ID"
	current[2] = initial
	out, err := Apply(Result{Market: 7, OrderID: 2, Events: []Event{{Kind: CancelRejected, OrderID: 2, Reason: "UNKNOWN_ORDER"}}}, current)
	if err != nil || len(out.Orders) != 0 {
		t.Fatalf("known matcher rejection should reconcile: %+v %v", out, err)
	}
	initial.Status = Open
	initial.RejectionReason = ""
	current[2] = initial
	if _, err := Apply(Result{Market: 7, OrderID: 2, Events: []Event{{Kind: CancelRejected, OrderID: 2, Reason: "UNKNOWN_ORDER"}}}, current); !errors.Is(err, ErrInvariant) {
		t.Fatalf("unknown resting order must fail closed: %v", err)
	}
}
