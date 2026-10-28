package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

var (
	ErrIdempotencyConflict    = errors.New("client_order_id already bound to a different submission")
	ErrInvalidReservationPlan = errors.New("unsafe reservation plan")
)

// ReservationPlan must come from a TRUSTED deterministic, versioned policy
// using the canonical order, not from a client-supplied amount. Its atoms are
// the complete reserve (including the buyer's fee budget where applicable).
// A concrete CEX 15-compatible fee policy is intentionally NOT invented here.
type ReservationPlan struct {
	Asset         string
	Atoms         uint64
	PolicyID      string
	PolicyVersion int64
}

// ReservationPlanner must perform no RPCs, IO or other side effects inside
// TX A. Policy changes must result in a new version; a prior claim never
// recalculates the reservation on retry.
type ReservationPlanner interface {
	PlanReservation(accountID string, limit domain.CanonicalLimit) (ReservationPlan, error)
}

func ValidateReservationPlan(limit domain.CanonicalLimit, p ReservationPlan) error {
	if p.PolicyVersion <= 0 || len(p.PolicyID) == 0 || len(p.PolicyID) > 64 || p.Atoms == 0 {
		return ErrInvalidReservationPlan
	}
	for _, c := range p.PolicyID {
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' ||
			'0' <= c && c <= '9' || c == '.' || c == '-' || c == '_') {
			return ErrInvalidReservationPlan
		}
	}
	switch limit.Submission.Side {
	case domain.SideBuy:
		if p.Asset != limit.Market.QuoteAsset || p.Atoms < limit.NotionalQuoteAtoms {
			return ErrInvalidReservationPlan
		}
	case domain.SideSell:
		// CEX 15: sellers reserve base quantity; fees come from quote proceeds.
		if p.Asset != limit.Market.BaseAsset || p.Atoms != limit.QuantityBaseAtoms {
			return ErrInvalidReservationPlan
		}
	default:
		return ErrInvalidReservationPlan
	}
	return nil
}

// AdmissionRecord is the stable durable result of TX A. A REJECTED record has
// no Canonical/Reservation; PENDING and ACCEPTED have their frozen snapshots.
type AdmissionRecord struct {
	OrderID       int64
	AccountID     string
	Submission    domain.NormalizedSubmission
	State         domain.AdmissionState
	RejectionCode string
	Canonical     *domain.CanonicalLimit
	Reservation   *ReservationPlan
}

// Lease is returned ONLY on a freshly claimed PENDING order. Concurrent
// duplicates are read-only and receive no lease token or reservation authority.
type Lease struct {
	Owner      string
	Generation int64
	Until      time.Time
}

type ClaimResult struct {
	Created bool
	Record  AdmissionRecord
	Lease   *Lease
}

// Repository claims a single canonical identity and persists either a fully
// prepared PENDING admission or a durable, deterministic REJECTED result.
type Repository interface {
	Claim(ctx context.Context, accountID string, submission domain.LimitSubmission) (ClaimResult, error)
}

func (r AdmissionRecord) ReservationOperationID() (string, error) {
	if r.State != domain.AdmissionPending && r.State != domain.AdmissionAccepted {
		return "", fmt.Errorf("%w: reservation operation for %s", domain.ErrInvalidSubmission, r.State)
	}
	return domain.ReservationOperationID(r.OrderID)
}
