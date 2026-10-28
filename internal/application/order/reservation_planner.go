package order

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

// FeePolicy is an explicitly supplied, immutable, versioned admission fee cap.
// BuyerMaxFeeBps must bound the rate charged by Accounting for ANY execution
// of this order, including taker/maker changes and fee tiers. There is no
// implicit or hard-coded exchange-wide fee rate.
//
// CEX 15 reserve semantics: BUY reserves quote notional plus the maximum
// buyer fee budget; SELL reserves base quantity (seller fees are taken from
// quote proceeds). This implementation assumes per-execution fees are
// rounded UP to quote atoms. If Accounting uses a different rounding or rate
// policy, reconcile both policies before using this planner in production.
// It intentionally budgets the maximum fee for a fill split into single lots.
// The result is the exact upper bound for any grouping of lots at prices no
// higher than the BUY limit, under that per-execution rounding rule.
type FeePolicy struct {
	ID             string
	Version        int64
	BuyerMaxFeeBps uint32
}

const feeDenominator = uint64(10_000)

var ErrFeePolicyUnavailable = errors.New("fee reservation policy unavailable")

// FeeReservationPlanner is pure and safe to invoke while TX A holds the market
// snapshot lock. Its schedule is copied at construction. An update MUST use a
// new version and a new instance; in-flight orders keep their frozen plan.
type FeeReservationPlanner struct {
	byMarket map[string]FeePolicy
}

var _ ReservationPlanner = (*FeeReservationPlanner)(nil)

func NewFeeReservationPlanner(policies map[string]FeePolicy) (*FeeReservationPlanner, error) {
	if len(policies) == 0 {
		return nil, fmt.Errorf("%w: no fee policies configured", ErrFeePolicyUnavailable)
	}
	copyOf := make(map[string]FeePolicy, len(policies))
	for marketID, p := range policies {
		if p.BuyerMaxFeeBps > uint32(feeDenominator) || p.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid buyer fee cap/version for %q", ErrFeePolicyUnavailable, marketID)
		}
		// Reuse the existing strict policy identifier check, but without
		// assigning a meaningless plan to a real order.
		if len(p.ID) == 0 || len(p.ID) > 64 || !validPolicyID(p.ID) || marketID == "" {
			return nil, fmt.Errorf("%w: invalid policy identity for %q", ErrFeePolicyUnavailable, marketID)
		}
		copyOf[marketID] = p
	}
	return &FeeReservationPlanner{byMarket: copyOf}, nil
}

func validPolicyID(s string) bool {
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || c == '.' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (p *FeeReservationPlanner) PlanReservation(_ string, limit domain.CanonicalLimit) (ReservationPlan, error) {
	if p == nil {
		return ReservationPlan{}, ErrFeePolicyUnavailable
	}
	policy, ok := p.byMarket[limit.Market.MarketID]
	if !ok {
		return ReservationPlan{}, fmt.Errorf("%w: no policy for market %s", ErrFeePolicyUnavailable, limit.Market.MarketID)
	}
	result := ReservationPlan{PolicyID: policy.ID, PolicyVersion: policy.Version}
	switch limit.Submission.Side {
	case domain.SideSell:
		result.Asset = limit.Market.BaseAsset
		result.Atoms = limit.QuantityBaseAtoms
	case domain.SideBuy:
		if limit.QuantityLots <= 0 || limit.NotionalQuoteAtoms == 0 ||
			limit.NotionalQuoteAtoms%uint64(limit.QuantityLots) != 0 {
			return ReservationPlan{}, fmt.Errorf("%w: invalid canonical notional/lot count", ErrInvalidReservationPlan)
		}
		// Each permissible execution consists of one or more whole lots.
		// At the limit price, quote atoms per lot is an integer. Because
		// ceil(x+y) <= ceil(x)+ceil(y), the greatest total fee occurs
		// when the order is split into one-lot executions. math/big avoids
		// all intermediate multiplication overflow.
		perLot := limit.NotionalQuoteAtoms / uint64(limit.QuantityLots)
		maxFeePerLot := ceilRatio(new(big.Int).Mul(new(big.Int).SetUint64(perLot),
			new(big.Int).SetUint64(uint64(policy.BuyerMaxFeeBps))), feeDenominator)
		feeBudget := maxFeePerLot.Mul(maxFeePerLot, big.NewInt(limit.QuantityLots))
		amount := new(big.Int).Add(new(big.Int).SetUint64(limit.NotionalQuoteAtoms), feeBudget)
		if !amount.IsUint64() || amount.Sign() <= 0 || amount.Cmp(new(big.Int).SetUint64(math.MaxUint64)) > 0 {
			return ReservationPlan{}, fmt.Errorf("%w: buyer reserve overflows uint64", ErrInvalidReservationPlan)
		}
		result.Asset = limit.Market.QuoteAsset
		result.Atoms = amount.Uint64()
	default:
		return ReservationPlan{}, ErrInvalidReservationPlan
	}
	if err := ValidateReservationPlan(limit, result); err != nil {
		return ReservationPlan{}, err
	}
	return result, nil
}

func ceilRatio(numerator *big.Int, denominator uint64) *big.Int {
	divisor := new(big.Int).SetUint64(denominator)
	quotient, remainder := new(big.Int).QuoRem(numerator, divisor, new(big.Int))
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}
