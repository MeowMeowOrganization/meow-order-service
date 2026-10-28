package order

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

var (
	ErrReservationOutcomeUnknown  = errors.New("accounting reservation outcome unknown; PENDING must be recovered")
	ErrReservationEvidenceInvalid = errors.New("accounting returned invalid reservation evidence")
	ErrAdmissionLeaseLost         = errors.New("admission lease lost or expired")
	ErrAdmissionLeaseTooShort     = errors.New("admission lease cannot cover reservation RPC")
	ErrAdmissionNotFound          = errors.New("admission not found")
	ErrAdmissionCommitUnknown     = errors.New("admission TX B commit outcome unknown; retry original client_order_id")
)

// ReservationRequest is the application-owned Accounting port; no protobuf or
// transport types are permitted in the application layer.
type ReservationRequest struct {
	OperationID string
	AccountID   string
	ReferenceID string
	Asset       string
	Atoms       uint64
}

type ReservationEvidence struct {
	ReservationID  string
	AccountID      string
	ReferenceID    string
	Asset          string
	OriginalAtoms  uint64
	RemainingAtoms uint64
	Active         bool
}

type FundsReserver interface {
	Reserve(ctx context.Context, req ReservationRequest) (ReservationEvidence, error)
}

// RecoveryRepository adds fenced reclaiming, verification, and TX B on top of TX A.
// Implementations MUST atomically claim expired PENDING rows, increment the
// persisted generation and replace the owner under PostgreSQL row locking.
type RecoveryRepository interface {
	Repository
	ReclaimExpired(ctx context.Context, orderID int64) (ClaimResult, error)
	LeaseValid(ctx context.Context, orderID int64, lease Lease) (bool, error)
	Finalize(ctx context.Context, orderID int64, lease Lease, evidence ReservationEvidence) (AdmissionRecord, error)
}

// PlaceOutcome contains the durable admission result. Confirmed is set ONLY
// after TX B commits ACCEPTED and the matching outbox in PostgreSQL. A timeout
// or failed finalization never returns a misleading ACCEPTED outcome.
type PlaceOutcome struct {
	Record     AdmissionRecord
	Created    bool
	InProgress bool
	Confirmed  *ReservationEvidence
	Lease      *Lease
}

type PlaceOrder struct {
	repo              RecoveryRepository
	accounting        FundsReserver
	rpcTimeout        time.Duration
	leaseSafetyMargin time.Duration
}

func NewPlaceOrder(repo RecoveryRepository, accounting FundsReserver, rpcTimeout, safetyMargin time.Duration) (*PlaceOrder, error) {
	if repo == nil || accounting == nil || rpcTimeout <= 0 || rpcTimeout > time.Minute ||
		safetyMargin <= 0 || safetyMargin > time.Minute {
		return nil, errors.New("place order requires a repository, accounting client, positive RPC timeout and lease margin")
	}
	return &PlaceOrder{repo: repo, accounting: accounting, rpcTimeout: rpcTimeout, leaseSafetyMargin: safetyMargin}, nil
}

// Place claims TX A and reserves funds only if the caller owns the newly
// acquired PENDING lease. Existing PENDING retries do not issue a second RPC.
// A recoverer can later reclaim an expired lease with the SAME operation ID.
func (p *PlaceOrder) Place(ctx context.Context, accountID string, submission domain.LimitSubmission) (PlaceOutcome, error) {
	claim, err := p.repo.Claim(ctx, accountID, submission)
	if err != nil {
		return PlaceOutcome{}, err
	}
	return p.processClaim(ctx, claim)
}

// Recover attempts ONE expired PENDING admission by canonical order ID.
// A scheduler/worker is deliberately deferred; callers may use this method
// after discovering a stale order. Unexpired or terminal rows are read-only.
func (p *PlaceOrder) Recover(ctx context.Context, orderID int64) (PlaceOutcome, error) {
	claim, err := p.repo.ReclaimExpired(ctx, orderID)
	if err != nil {
		return PlaceOutcome{}, err
	}
	return p.processClaim(ctx, claim)
}

func (p *PlaceOrder) processClaim(ctx context.Context, claim ClaimResult) (PlaceOutcome, error) {
	out := PlaceOutcome{Record: claim.Record, Created: claim.Created, Lease: claim.Lease}
	if claim.Record.State != domain.AdmissionPending {
		return out, nil // durable REJECTED or (future) ACCEPTED replay
	}
	if claim.Lease == nil {
		out.InProgress = true
		return out, nil // never reserve on a duplicate without lease authority
	}
	if claim.Record.Canonical == nil || claim.Record.Reservation == nil {
		return out, ErrInvalidReservationPlan
	}
	if err := ValidateReservationPlan(*claim.Record.Canonical, *claim.Record.Reservation); err != nil {
		return out, err
	}
	// A lease must outlive the entire RPC deadline AND a fencing margin.
	// Never start an RPC on a lease that is already too close to expiration.
	if time.Until(claim.Lease.Until) <= p.rpcTimeout+p.leaseSafetyMargin {
		return out, ErrAdmissionLeaseTooShort
	}
	opID, err := claim.Record.ReservationOperationID()
	if err != nil {
		return out, err
	}
	req := ReservationRequest{
		OperationID: opID, AccountID: claim.Record.AccountID,
		ReferenceID: strconv.FormatInt(claim.Record.OrderID, 10),
		Asset:       claim.Record.Reservation.Asset, Atoms: claim.Record.Reservation.Atoms,
	}
	rpcCtx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
	defer cancel()
	evidence, err := p.accounting.Reserve(rpcCtx, req)
	if err != nil {
		// No compensating release: the reservation may have succeeded.
		// TX A remains PENDING, and recovery repeats this SAME operation_id.
		if isAmbiguousReservationError(err) || rpcCtx.Err() != nil {
			return out, fmt.Errorf("%w: %w", ErrReservationOutcomeUnknown, err)
		}
		return out, fmt.Errorf("reserve funds (PENDING preserved): %w", err)
	}
	if err := ValidateReservationEvidence(req, evidence); err != nil {
		return out, err // never assume an invalid reservation is usable
	}
	// Advisory early check only. TX B MUST recheck the lease while holding
	// the order row lock: this call alone is not a fencing guarantee.
	active, err := p.repo.LeaseValid(ctx, claim.Record.OrderID, *claim.Lease)
	if err != nil {
		return out, fmt.Errorf("verify admission lease after reservation: %w", err)
	}
	if !active {
		return out, ErrAdmissionLeaseLost
	}
	// The repository revalidates the stored reservation intent under FOR UPDATE,
	// checks the fencing token, and commits ACCEPTED + serialized outbox together.
	// A returned commit error may mean the DB committed but the ACK was lost.
	accepted, err := p.repo.Finalize(ctx, claim.Record.OrderID, *claim.Lease, evidence)
	if err != nil {
		return out, fmt.Errorf("finalize admission TX B: %w", err)
	}
	out.Record = accepted
	out.Confirmed = &evidence
	out.Lease = nil // committed ACCEPTED rows no longer have a live admission lease
	return out, nil
}

func isAmbiguousReservationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrReservationOutcomeUnknown)
}

// ValidateReservationEvidence is also called at the repository boundary against
// the LOCKED frozen intent; a direct Finalize caller cannot bypass verification.
func ValidateReservationEvidence(req ReservationRequest, got ReservationEvidence) error {
	if !got.Active || len(got.ReservationID) == 0 || len(got.ReservationID) > 128 || got.AccountID != req.AccountID ||
		got.ReferenceID != req.ReferenceID || got.Asset != req.Asset ||
		got.OriginalAtoms != req.Atoms || got.RemainingAtoms != req.Atoms {
		return ErrReservationEvidenceInvalid
	}
	return nil
}
