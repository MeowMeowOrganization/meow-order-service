package order

import (
	"errors"
	"fmt"
)

// AdmissionState describes order admission only; later matcher lifecycle states
// (OPEN, FILLED, etc.) must not overwrite the durable admission outcome.
type AdmissionState string

const (
	AdmissionPending  AdmissionState = "PENDING"
	AdmissionAccepted AdmissionState = "ACCEPTED"
	AdmissionRejected AdmissionState = "REJECTED"
)

var ErrInvalidAdmissionTransition = errors.New("invalid admission transition")

// ValidateAdmissionTransition protects terminal decisions. An unchanged PENDING
// state is permitted for a recoverable retry, but terminal states are immutable.
func ValidateAdmissionTransition(from, to AdmissionState) error {
	switch {
	case from == AdmissionPending && (to == AdmissionPending || to == AdmissionAccepted || to == AdmissionRejected):
		return nil
	case (from == AdmissionAccepted || from == AdmissionRejected) && to == from:
		return nil
	default:
		return fmt.Errorf("%w: %q -> %q", ErrInvalidAdmissionTransition, from, to)
	}
}

// ReservationOperationID derives the stable Accounting idempotency key from
// the canonical PostgreSQL order ID (positive signed BIGINT).
func ReservationOperationID(orderID int64) (string, error) {
	if orderID <= 0 {
		return "", fmt.Errorf("%w: non-positive order ID", ErrInvalidSubmission)
	}
	return fmt.Sprintf("order-reserve:%d", orderID), nil
}

// PlaceCommandID is the immutable logical matching command identity.
func PlaceCommandID(orderID int64) (string, error) {
	if orderID <= 0 {
		return "", fmt.Errorf("%w: non-positive order ID", ErrInvalidSubmission)
	}
	return fmt.Sprintf("place:%d", orderID), nil
}
