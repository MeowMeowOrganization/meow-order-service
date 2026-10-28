package order

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	FingerprintVersion = uint16(1)
	maxAccountIDBytes  = 128
	maxClientIDBytes   = 128
	maxMarketIDBytes   = 64
	maxDecimalBytes    = 128
)

var (
	ErrInvalidSubmission = errors.New("invalid limit submission")
	ErrInvalidDecimal    = errors.New("invalid decimal")
)

type Side uint8

const (
	SideBuy  Side = 1
	SideSell Side = 2
)

type TimeInForce uint8

const TimeInForceGTC TimeInForce = 1

// LimitSubmission is transport-agnostic: the adapter maps protobuf enum values
// into these domain types in a later slice.
type LimitSubmission struct {
	ClientOrderID string
	MarketID      string
	Side          Side
	Price         string
	Quantity      string
	TimeInForce   TimeInForce
}

// NormalizedSubmission holds exact, canonical ASCII decimal strings. Fingerprint
// is computed BEFORE market-specific validation; changing market rules or fee
// policy must not change whether a request is a retry.
type NormalizedSubmission struct {
	ClientOrderID string
	MarketID      string
	Side          Side
	Price         string
	Quantity      string
	TimeInForce   TimeInForce
	Fingerprint   [sha256.Size]byte
}

// ValidateAccountID checks a trusted Gateway identity, never derives one from
// client-supplied order fields.
func ValidateAccountID(id string) error {
	if !visibleASCII(id, maxAccountIDBytes) {
		return fmt.Errorf("%w: account ID must be 1..%d visible ASCII bytes", ErrInvalidSubmission, maxAccountIDBytes)
	}
	return nil
}

func NormalizeSubmission(in LimitSubmission) (NormalizedSubmission, error) {
	if !visibleASCII(in.ClientOrderID, maxClientIDBytes) {
		return NormalizedSubmission{}, fmt.Errorf("%w: invalid client_order_id", ErrInvalidSubmission)
	}
	if !validMarketID(in.MarketID) {
		return NormalizedSubmission{}, fmt.Errorf("%w: invalid market_id", ErrInvalidSubmission)
	}
	if in.Side != SideBuy && in.Side != SideSell {
		return NormalizedSubmission{}, fmt.Errorf("%w: unsupported order side", ErrInvalidSubmission)
	}
	if in.TimeInForce != TimeInForceGTC {
		return NormalizedSubmission{}, fmt.Errorf("%w: only GTC is supported", ErrInvalidSubmission)
	}
	price, err := CanonicalDecimal(in.Price)
	if err != nil {
		return NormalizedSubmission{}, fmt.Errorf("price: %w", err)
	}
	quantity, err := CanonicalDecimal(in.Quantity)
	if err != nil {
		return NormalizedSubmission{}, fmt.Errorf("quantity: %w", err)
	}
	out := NormalizedSubmission{
		ClientOrderID: in.ClientOrderID,
		MarketID:      in.MarketID,
		Side:          in.Side,
		Price:         price,
		Quantity:      quantity,
		TimeInForce:   in.TimeInForce,
	}
	out.Fingerprint = fingerprint(out)
	return out, nil
}

// CanonicalDecimal accepts only unsigned ordinary ASCII base-10 notation with
// at least one digit before and, if present, after the decimal point. It does
// not round and never passes through a floating-point representation.
func CanonicalDecimal(raw string) (string, error) {
	if len(raw) == 0 || len(raw) > maxDecimalBytes {
		return "", fmt.Errorf("%w: decimal length out of range", ErrInvalidDecimal)
	}
	seenDot := false
	for _, c := range []byte(raw) {
		switch {
		case c >= '0' && c <= '9':
		case c == '.' && !seenDot:
			seenDot = true
		default:
			return "", fmt.Errorf("%w: only unsigned base-10 notation is allowed", ErrInvalidDecimal)
		}
	}
	parts := strings.SplitN(raw, ".", 2)
	if len(parts[0]) == 0 || (seenDot && len(parts[1]) == 0) {
		return "", fmt.Errorf("%w: missing digits", ErrInvalidDecimal)
	}
	whole := strings.TrimLeft(parts[0], "0")
	if whole == "" {
		whole = "0"
	}
	if !seenDot {
		return whole, nil
	}
	fraction := strings.TrimRight(parts[1], "0")
	if fraction == "" {
		return whole, nil
	}
	return whole + "." + fraction, nil
}

func visibleASCII(s string, maxBytes int) bool {
	if len(s) == 0 || len(s) > maxBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '!' || s[i] > '~' {
			return false
		}
	}
	return true
}

func validMarketID(s string) bool {
	if len(s) == 0 || len(s) > maxMarketIDBytes {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// fingerprint hashes a versioned, unambiguous length-prefixed encoding. The
// client_order_id is deliberately omitted: that is the uniqueness KEY, while
// the fingerprint describes the business request bound to that key.
func fingerprint(in NormalizedSubmission) [sha256.Size]byte {
	buf := make([]byte, 0, 128)
	buf = append(buf, "meow.limit-order.submission.v1\x00"...)
	appendString := func(s string) {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
		buf = append(buf, s...)
	}
	appendString(in.MarketID)
	buf = append(buf, byte(in.Side))
	appendString(in.Price)
	appendString(in.Quantity)
	buf = append(buf, byte(in.TimeInForce))
	return sha256.Sum256(buf)
}
