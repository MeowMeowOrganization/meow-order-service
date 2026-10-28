package order

import (
	"errors"
	"testing"
)

func testMarket() MarketRules {
	return MarketRules{
		MarketID: "BTC-USDT", NumericMarketID: 7, ConfigVersion: 3,
		BaseAsset: "BTC", QuoteAsset: "USDT",
		BaseScale: 8, QuoteScale: 6, PriceScale: 2,
		PriceTickUnits: 5, QuantityStepBaseAtoms: 100000,
		MinQuantityBaseAtoms: 100000, MinNotionalQuoteAtoms: 1_000_000,
		Status: MarketActive,
	}
}

func normalized(t *testing.T, price, qty string) NormalizedSubmission {
	t.Helper()
	s := validSubmission()
	s.Price, s.Quantity = price, qty
	v, err := NormalizeSubmission(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestCanonicalizeLimitExactTicksLotsAndQuoteAtoms(t *testing.T) {
	t.Parallel()
	v := normalized(t, "60000.10", "0.01200000")
	got, err := CanonicalizeLimit(v, testMarket())
	if err != nil {
		t.Fatal(err)
	}
	if got.PriceUnits != 6_000_010 || got.QuantityBaseAtoms != 1_200_000 ||
		got.PriceTicks != 1_200_002 || got.QuantityLots != 12 ||
		got.NotionalQuoteAtoms != 720_001_200 {
		t.Fatalf("unexpected canonical LIMIT: %+v", got)
	}
}

func TestCanonicalizeLimitRejectsInvalidOrders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		price, qty string
		modify     func(*MarketRules)
		want       error
	}{
		{"zero price", "0.00", "0.012", nil, ErrInvalidPrice},
		{"zero quantity", "60000.10", "0", nil, ErrInvalidQuantity},
		{"price scale", "60000.101", "0.012", nil, ErrPrecision},
		{"quantity scale", "60000.10", "0.012000001", nil, ErrPrecision},
		{"tick", "60000.11", "0.012", nil, ErrPriceOffTick},
		{"lot", "60000.10", "0.01200001", nil, ErrQuantityOffLot},
		{"minimum qty", "60000.10", "0.001", func(m *MarketRules) { m.MinQuantityBaseAtoms = 200000 }, ErrQuantityBelowMinimum},
		{"minimum notional", "60000.10", "0.012", func(m *MarketRules) { m.MinNotionalQuoteAtoms = 800_000_000 }, ErrNotionalBelowMinimum},
		{"halted", "60000.10", "0.012", func(m *MarketRules) { m.Status = MarketHalted }, ErrMarketNotActive},
		{"mismatched market", "60000.10", "0.012", func(m *MarketRules) { m.MarketID = "ETH-USDT" }, ErrMarketMismatch},
		{"overflow price", "18446744073709551616", "0.012", nil, ErrOverflow},
		{"overflow notional", "184467440737095516.10", "100000000.00000000", nil, ErrOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := testMarket()
			if tc.modify != nil {
				tc.modify(&m)
			}
			_, err := CanonicalizeLimit(normalized(t, tc.price, tc.qty), m)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v; got %v", tc.want, err)
			}
		})
	}
}

func TestValidateMarketRulesProtectsPartialFillExactness(t *testing.T) {
	t.Parallel()
	m := testMarket()
	m.QuantityStepBaseAtoms = 1 // One tick times one lot = fractional quote atom.
	if err := ValidateMarketRules(m); !errors.Is(err, ErrInvalidMarketRules) {
		t.Fatal(err)
	}
	m = testMarket()
	m.NumericMarketID = 0
	if err := ValidateMarketRules(m); !errors.Is(err, ErrInvalidMarketRules) {
		t.Fatal(err)
	}
	m = testMarket()
	m.PriceScale = 19
	if err := ValidateMarketRules(m); !errors.Is(err, ErrInvalidMarketRules) {
		t.Fatal(err)
	}
}

func TestTickCountMustFitMatcherInt64(t *testing.T) {
	t.Parallel()
	m := testMarket()
	m.PriceScale = 0
	m.PriceTickUnits = 1
	m.QuoteScale = 8 // Exact notional with price_scale=0 and base_scale=8.
	m.MinNotionalQuoteAtoms = 1
	_, err := CanonicalizeLimit(normalized(t, "9223372036854775808", "0.001"), m)
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected matcher int64 overflow, got %v", err)
	}
}

func TestCanonicalizationRejectsTamperedFingerprint(t *testing.T) {
	t.Parallel()
	in := normalized(t, "60000.10", "0.012")
	in.Fingerprint[0] ^= 0x01
	_, err := CanonicalizeLimit(in, testMarket())
	if !errors.Is(err, ErrInvalidSubmission) {
		t.Fatalf("expected forged snapshot to fail: %v", err)
	}
}
