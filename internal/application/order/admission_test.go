package order

import (
	"errors"
	"testing"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

func buyLimit() domain.CanonicalLimit {
	return domain.CanonicalLimit{
		Submission:         domain.NormalizedSubmission{Side: domain.SideBuy},
		Market:             domain.MarketRules{QuoteAsset: "USDT", BaseAsset: "BTC"},
		NotionalQuoteAtoms: 12345, QuantityBaseAtoms: 10,
	}
}

func TestReservationPlanValidation(t *testing.T) {
	t.Parallel()
	buy := buyLimit()
	good := ReservationPlan{Asset: "USDT", Atoms: 12346, PolicyID: "cex15.taker", PolicyVersion: 2}
	if err := ValidateReservationPlan(buy, good); err != nil {
		t.Fatal(err)
	}
	cases := []ReservationPlan{
		{Asset: "BTC", Atoms: 12346, PolicyID: "cex15.taker", PolicyVersion: 2},
		{Asset: "USDT", Atoms: 12344, PolicyID: "cex15.taker", PolicyVersion: 2},
		{Asset: "USDT", Atoms: 12346, PolicyID: "", PolicyVersion: 2},
		{Asset: "USDT", Atoms: 12346, PolicyID: "unsafe space", PolicyVersion: 2},
		{Asset: "USDT", Atoms: 12346, PolicyID: "cex15.taker", PolicyVersion: 0},
	}
	for _, p := range cases {
		if err := ValidateReservationPlan(buy, p); !errors.Is(err, ErrInvalidReservationPlan) {
			t.Fatalf("unsafe buy plan %+v: %v", p, err)
		}
	}
	sell := buy
	sell.Submission.Side = domain.SideSell
	if err := ValidateReservationPlan(sell, ReservationPlan{Asset: "BTC", Atoms: 10, PolicyID: "cex15.taker", PolicyVersion: 2}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReservationPlan(sell, good); !errors.Is(err, ErrInvalidReservationPlan) {
		t.Fatal(err)
	}
}
