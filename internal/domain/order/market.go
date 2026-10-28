package order

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// The market catalog supplies an immutable snapshot for the admission attempt.
// NumericMarketID is the matching engine's uint32 MarketId, not a hash of BTC-USDT.
type MarketStatus uint8

const (
	MarketActive MarketStatus = 1
	MarketHalted MarketStatus = 2
)

type MarketRules struct {
	MarketID              string
	NumericMarketID       uint32
	ConfigVersion         int64
	BaseAsset             string
	QuoteAsset            string
	BaseScale             uint32
	QuoteScale            uint32
	PriceScale            uint32
	PriceTickUnits        uint64
	QuantityStepBaseAtoms uint64
	MinQuantityBaseAtoms  uint64
	MinNotionalQuoteAtoms uint64
	Status                MarketStatus
}

// CanonicalLimit is the validated order data stored in TX A and later reused
// unchanged to construct the matching command in TX B.
type CanonicalLimit struct {
	Submission         NormalizedSubmission
	Market             MarketRules
	PriceUnits         uint64
	QuantityBaseAtoms  uint64
	PriceTicks         int64
	QuantityLots       int64
	NotionalQuoteAtoms uint64
}

var (
	ErrInvalidMarketRules   = errors.New("invalid market rules")
	ErrMarketNotActive      = errors.New("market not active")
	ErrMarketMismatch       = errors.New("market mismatch")
	ErrInvalidPrice         = errors.New("invalid price")
	ErrInvalidQuantity      = errors.New("invalid quantity")
	ErrPrecision            = errors.New("precision exceeds asset or price scale")
	ErrPriceOffTick         = errors.New("price is not a multiple of tick")
	ErrQuantityOffLot       = errors.New("quantity is not a multiple of lot")
	ErrQuantityBelowMinimum = errors.New("quantity below market minimum")
	ErrNotionalBelowMinimum = errors.New("notional below market minimum")
	ErrOverflow             = errors.New("canonical integer exceeds supported range")
)

// ValidateMarketRules checks independent market invariants. In particular a
// single configured price-tick x quantity-lot MUST produce integral quote
// atoms, so every possible partial execution is exact for settlement.
func ValidateMarketRules(m MarketRules) error {
	if !validMarketID(m.MarketID) || m.NumericMarketID == 0 || m.ConfigVersion <= 0 ||
		!validAsset(m.BaseAsset) || !validAsset(m.QuoteAsset) || m.BaseAsset == m.QuoteAsset ||
		m.BaseScale > 18 || m.QuoteScale > 18 || m.PriceScale > 18 ||
		m.PriceTickUnits == 0 || m.QuantityStepBaseAtoms == 0 ||
		m.MinQuantityBaseAtoms == 0 || m.MinNotionalQuoteAtoms == 0 ||
		(m.Status != MarketActive && m.Status != MarketHalted) {
		return fmt.Errorf("%w: invalid identity, version, scales, or limits", ErrInvalidMarketRules)
	}
	perLot := new(big.Int).Mul(new(big.Int).SetUint64(m.PriceTickUnits), new(big.Int).SetUint64(m.QuantityStepBaseAtoms))
	perLot.Mul(perLot, powerOf10(m.QuoteScale))
	denom := powerOf10(m.PriceScale + m.BaseScale)
	if new(big.Int).Mod(perLot, denom).Sign() != 0 {
		return fmt.Errorf("%w: a tick and lot cannot settle in integral quote atoms", ErrInvalidMarketRules)
	}
	return nil
}

// CanonicalizeLimit performs exact decimal -> uint64 atoms -> int64 ticks/lots
// conversions. It never rounds, and the returned notional fits uint64.
func CanonicalizeLimit(in NormalizedSubmission, rules MarketRules) (CanonicalLimit, error) {
	// Verify the normalized input and its fingerprint were not changed after
	// parsing. This also catches manually constructed invalid domain values.
	verified, err := NormalizeSubmission(LimitSubmission{
		ClientOrderID: in.ClientOrderID, MarketID: in.MarketID, Side: in.Side,
		Price: in.Price, Quantity: in.Quantity, TimeInForce: in.TimeInForce,
	})
	if err != nil || verified != in {
		return CanonicalLimit{}, ErrInvalidSubmission
	}
	if err := ValidateMarketRules(rules); err != nil {
		return CanonicalLimit{}, err
	}
	if in.MarketID != rules.MarketID {
		return CanonicalLimit{}, ErrMarketMismatch
	}
	if rules.Status != MarketActive {
		return CanonicalLimit{}, ErrMarketNotActive
	}
	price, err := decimalUnits(in.Price, rules.PriceScale)
	if err != nil {
		return CanonicalLimit{}, fmt.Errorf("price: %w", err)
	}
	if price == 0 {
		return CanonicalLimit{}, ErrInvalidPrice
	}
	quantity, err := decimalUnits(in.Quantity, rules.BaseScale)
	if err != nil {
		return CanonicalLimit{}, fmt.Errorf("quantity: %w", err)
	}
	if quantity == 0 {
		return CanonicalLimit{}, ErrInvalidQuantity
	}
	if price%rules.PriceTickUnits != 0 {
		return CanonicalLimit{}, ErrPriceOffTick
	}
	if quantity%rules.QuantityStepBaseAtoms != 0 {
		return CanonicalLimit{}, ErrQuantityOffLot
	}
	if quantity < rules.MinQuantityBaseAtoms {
		return CanonicalLimit{}, ErrQuantityBelowMinimum
	}
	priceTicks := price / rules.PriceTickUnits
	quantityLots := quantity / rules.QuantityStepBaseAtoms
	if priceTicks > math.MaxInt64 || quantityLots > math.MaxInt64 {
		return CanonicalLimit{}, ErrOverflow
	}
	notional := new(big.Int).Mul(new(big.Int).SetUint64(price), new(big.Int).SetUint64(quantity))
	notional.Mul(notional, powerOf10(rules.QuoteScale))
	denom := powerOf10(rules.PriceScale + rules.BaseScale)
	quotient, remainder := new(big.Int).QuoRem(notional, denom, new(big.Int))
	if remainder.Sign() != 0 {
		// Should be impossible after ValidateMarketRules and tick/lot checks.
		return CanonicalLimit{}, fmt.Errorf("%w: inexact quote notional", ErrInvalidMarketRules)
	}
	if !quotient.IsUint64() {
		return CanonicalLimit{}, ErrOverflow
	}
	atoms := quotient.Uint64()
	if atoms < rules.MinNotionalQuoteAtoms {
		return CanonicalLimit{}, ErrNotionalBelowMinimum
	}
	return CanonicalLimit{
		Submission: in, Market: rules, PriceUnits: price,
		QuantityBaseAtoms: quantity, PriceTicks: int64(priceTicks),
		QuantityLots: int64(quantityLots), NotionalQuoteAtoms: atoms,
	}, nil
}

func decimalUnits(canonical string, scale uint32) (uint64, error) {
	if scale > 18 {
		return 0, ErrInvalidMarketRules
	}
	// Do not trust exported NormalizedSubmission fields to remain unmodified.
	rechecked, err := CanonicalDecimal(canonical)
	if err != nil || rechecked != canonical {
		return 0, ErrInvalidDecimal
	}
	parts := strings.SplitN(canonical, ".", 2)
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if uint32(len(fraction)) > scale {
		return 0, ErrPrecision
	}
	units, ok := new(big.Int).SetString(parts[0]+fraction+strings.Repeat("0", int(scale)-len(fraction)), 10)
	if !ok || !units.IsUint64() {
		return 0, ErrOverflow
	}
	return units.Uint64(), nil
}

func powerOf10(exp uint32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), new(big.Int).SetUint64(uint64(exp)), nil)
}

func validAsset(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for _, c := range s {
		if !('A' <= c && c <= 'Z' || '0' <= c && c <= '9') {
			return false
		}
	}
	return true
}
