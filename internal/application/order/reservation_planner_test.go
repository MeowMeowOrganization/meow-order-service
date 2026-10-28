package order

import (
	"errors"
	"math"
	"testing"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

func plannedLimit(t *testing.T, side domain.Side) domain.CanonicalLimit {
	t.Helper()
	sub, err := domain.NormalizeSubmission(domain.LimitSubmission{
		ClientOrderID: "fee-planner", MarketID: "BTC-USDT", Side: side,
		Price: "60000.10", Quantity: "0.012", TimeInForce: domain.TimeInForceGTC,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := domain.CanonicalizeLimit(sub, domain.MarketRules{
		MarketID: "BTC-USDT", NumericMarketID: 7, ConfigVersion: 3,
		BaseAsset: "BTC", QuoteAsset: "USDT", BaseScale: 8, QuoteScale: 6,
		PriceScale: 2, PriceTickUnits: 5, QuantityStepBaseAtoms: 100000,
		MinQuantityBaseAtoms: 100000, MinNotionalQuoteAtoms: 1, Status: domain.MarketActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFeeReservationPlannerBuyFeeIncludesWorstCasePartialFillRounding(t *testing.T) {
	planner, err := NewFeeReservationPlanner(map[string]FeePolicy{
		"BTC-USDT": {ID: "cex15.maxfee", Version: 3, BuyerMaxFeeBps: 25},
	})
	if err != nil {
		t.Fatal(err)
	}
	buy := plannedLimit(t, domain.SideBuy)
	if buy.NotionalQuoteAtoms != 720001200 || buy.QuantityLots != 12 {
		t.Fatalf("unexpected canonical order %+v", buy)
	}
	got, err := planner.PlanReservation("acct-1", buy)
	if err != nil {
		t.Fatal(err)
	}
	// One lot: 60_000_100 quote atoms; ceil(60_000_100*25/10_000)
	// = 150_001 atoms. Twelve 1-lot executions: 1_800_012 fee atoms.
	if got.Asset != "USDT" || got.Atoms != 721801212 || got.PolicyVersion != 3 {
		t.Fatalf("unexpected buy reserve %+v", got)
	}
	sell, err := planner.PlanReservation("acct-1", plannedLimit(t, domain.SideSell))
	if err != nil || sell.Asset != "BTC" || sell.Atoms != 1200000 {
		t.Fatalf("unexpected sell reserve %+v err %v", sell, err)
	}
}

func TestFeePlannerNoImplicitRatesAndFrozenPolicy(t *testing.T) {
	if _, err := NewFeeReservationPlanner(nil); !errors.Is(err, ErrFeePolicyUnavailable) {
		t.Fatalf("expected no implicit fee rates: %v", err)
	}
	policies := map[string]FeePolicy{"BTC-USDT": {ID: "fee.v4", Version: 4, BuyerMaxFeeBps: 0}}
	planner, err := NewFeeReservationPlanner(policies)
	if err != nil {
		t.Fatal(err)
	}
	policies["BTC-USDT"] = FeePolicy{ID: "fee.v5", Version: 5, BuyerMaxFeeBps: 10000}
	got, err := planner.PlanReservation("acct-1", plannedLimit(t, domain.SideBuy))
	if err != nil || got.Atoms != 720001200 || got.PolicyVersion != 4 {
		t.Fatalf("policy mutated after construction: %+v %v", got, err)
	}
	other := plannedLimit(t, domain.SideBuy)
	other.Market.MarketID = "ETH-USDT"
	if _, err := planner.PlanReservation("acct-1", other); !errors.Is(err, ErrFeePolicyUnavailable) {
		t.Fatalf("expected unsupported-market fail-closed: %v", err)
	}
	for _, invalid := range []FeePolicy{
		{ID: "bad space", Version: 1, BuyerMaxFeeBps: 100},
		{ID: "test", Version: 0, BuyerMaxFeeBps: 100},
		{ID: "test", Version: 1, BuyerMaxFeeBps: 10001},
	} {
		if _, err := NewFeeReservationPlanner(map[string]FeePolicy{"BTC-USDT": invalid}); err == nil {
			t.Errorf("accepted unsafe policy %+v", invalid)
		}
	}
}

func TestFeePlannerRefusesBuyReservationOverflow(t *testing.T) {
	planner, err := NewFeeReservationPlanner(map[string]FeePolicy{
		"BTC-USDT": {ID: "max.fee", Version: 1, BuyerMaxFeeBps: 10000},
	})
	if err != nil {
		t.Fatal(err)
	}
	limit := plannedLimit(t, domain.SideBuy)
	limit.QuantityLots = 1
	limit.NotionalQuoteAtoms = math.MaxUint64
	if _, err := planner.PlanReservation("acct-1", limit); !errors.Is(err, ErrInvalidReservationPlan) {
		t.Fatalf("overflow not rejected: %v", err)
	}
}
