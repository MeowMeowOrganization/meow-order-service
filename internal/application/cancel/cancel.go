// Package cancel provides the application-owned account-scoped cancellation
// boundary. Request acceptance is not the same as matching-engine cancellation.
package cancel

import (
	"context"
	"errors"
	"strconv"
)

var (
	ErrInvalidRequest   = errors.New("invalid cancellation request")
	ErrNotFound         = errors.New("order not found")
	ErrAdmissionPending = errors.New("order admission is still pending")
	ErrCommitUnknown    = errors.New("cancellation commit outcome unknown; retry same order ID")
	ErrProvenance       = errors.New("cancellation outbox provenance is inconsistent")
)

type Status string

const (
	Pending        Status = "PENDING"
	Confirmed      Status = "CONFIRMED"
	Rejected       Status = "REJECTED"
	NotCancellable Status = "NOT_CANCELLABLE"
)

type Result struct {
	OrderID         int64
	Status          Status
	LifecycleStatus string
	RejectionReason string
}

type Repository interface {
	RequestCancellation(context.Context, string, int64) (Result, error)
}

type Service struct{ repository Repository }

func New(repository Repository) (*Service, error) {
	if repository == nil {
		return nil, errors.New("cancellation requires a repository")
	}
	return &Service{repository: repository}, nil
}

// Cancel enforces account scope, canonical numeric identifiers, and stable
// idempotency through the repository's single-transaction request/outbox write.
func (s *Service) Cancel(ctx context.Context, accountID, orderID string) (Result, error) {
	if s == nil || s.repository == nil || len(accountID) == 0 || len(accountID) > 128 || len(orderID) == 0 {
		return Result{}, ErrInvalidRequest
	}
	id, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != orderID {
		return Result{}, ErrInvalidRequest
	}
	return s.repository.RequestCancellation(ctx, accountID, id)
}
