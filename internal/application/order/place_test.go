package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

type stubRecoveryRepo struct {
	mu                sync.Mutex
	claim             ClaimResult
	reclaim           ClaimResult
	valid             bool
	claims            int
	reclaims          int
	checks            int
	finalizes         int
	finalizeErr       error
	finalizedEvidence ReservationEvidence
}

func (r *stubRecoveryRepo) Claim(context.Context, string, domain.LimitSubmission) (ClaimResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claims++
	return r.claim, nil
}
func (r *stubRecoveryRepo) ReclaimExpired(context.Context, int64) (ClaimResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reclaims++
	return r.reclaim, nil
}
func (r *stubRecoveryRepo) LeaseValid(_ context.Context, id int64, lease Lease) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks++
	return r.valid, nil
}

func (r *stubRecoveryRepo) Finalize(_ context.Context, id int64, lease Lease, evidence ReservationEvidence) (AdmissionRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalizes++
	r.finalizedEvidence = evidence
	if r.finalizeErr != nil {
		return AdmissionRecord{}, r.finalizeErr
	}
	if !r.valid {
		return AdmissionRecord{}, ErrAdmissionLeaseLost
	}
	rec := r.claim.Record
	if id != rec.OrderID || lease.Generation <= 0 {
		return AdmissionRecord{}, ErrAdmissionLeaseLost
	}
	rec.State = domain.AdmissionAccepted
	return rec, nil
}

type stubAccounting struct {
	mu       sync.Mutex
	requests []ReservationRequest
	err      error
	response ReservationEvidence
}

func (a *stubAccounting) Reserve(ctx context.Context, req ReservationRequest) (ReservationEvidence, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, req)
	if a.err != nil {
		return ReservationEvidence{}, a.err
	}
	if a.response.ReservationID != "" {
		return a.response, nil
	}
	return ReservationEvidence{
		ReservationID: "reservation-42", AccountID: req.AccountID, ReferenceID: req.ReferenceID,
		Asset: req.Asset, OriginalAtoms: req.Atoms, RemainingAtoms: req.Atoms, Active: true,
	}, nil
}

func freshPendingRecord(t *testing.T) ClaimResult {
	t.Helper()
	canonical := plannedLimit(t, domain.SideBuy)
	return ClaimResult{Created: true, Record: AdmissionRecord{
		OrderID: 42, AccountID: "acct-1", Submission: canonical.Submission,
		State: domain.AdmissionPending, Canonical: &canonical,
		Reservation: &ReservationPlan{Asset: "USDT", Atoms: 721801212, PolicyID: "fee.v3", PolicyVersion: 3},
	}, Lease: &Lease{Owner: "abcedf00-aaaa-4bbb-8ccc-111122223333", Generation: 1, Until: time.Now().Add(30 * time.Second)}}
}

func placeInput() domain.LimitSubmission {
	return domain.LimitSubmission{ClientOrderID: "fee-planner", MarketID: "BTC-USDT", Side: domain.SideBuy,
		Price: "60000.10", Quantity: "0.012", TimeInForce: domain.TimeInForceGTC}
}

func TestPlaceReservesAndFinalizesAtomically(t *testing.T) {
	claim := freshPendingRecord(t)
	repo := &stubRecoveryRepo{claim: claim, valid: true}
	accounting := &stubAccounting{}
	usecase, err := NewPlaceOrder(repo, accounting, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := usecase.Place(context.Background(), "acct-1", placeInput())
	if err != nil {
		t.Fatal(err)
	}
	if result.Confirmed == nil || result.Record.State != domain.AdmissionAccepted || !result.Created || result.InProgress || result.Lease != nil {
		t.Fatalf("unexpected Place outcome: %+v", result)
	}
	if len(accounting.requests) != 1 || accounting.requests[0] != (ReservationRequest{
		OperationID: "order-reserve:42", AccountID: "acct-1", ReferenceID: "42", Asset: "USDT", Atoms: 721801212,
	}) {
		t.Fatalf("bad Accounting request %+v", accounting.requests)
	}
	if repo.checks != 1 || repo.finalizes != 1 || repo.finalizedEvidence.ReservationID != "reservation-42" {
		t.Fatalf("lease check count %d", repo.checks)
	}
}

func TestPlaceDuplicatePendingAndRejectedNeverReserve(t *testing.T) {
	claim := freshPendingRecord(t)
	claim.Created = false
	claim.Lease = nil
	repo := &stubRecoveryRepo{claim: claim, valid: true}
	accounting := &stubAccounting{}
	usecase, _ := NewPlaceOrder(repo, accounting, time.Second, time.Second)
	res, err := usecase.Place(context.Background(), "acct-1", placeInput())
	if err != nil || !res.InProgress || len(accounting.requests) != 0 {
		t.Fatalf("duplicate issued RPC: %+v %v", res, err)
	}
	repo.claim.Record.State = domain.AdmissionRejected
	repo.claim.Record.Canonical = nil
	repo.claim.Record.Reservation = nil
	res, err = usecase.Place(context.Background(), "acct-1", placeInput())
	if err != nil || res.InProgress || len(accounting.requests) != 0 {
		t.Fatalf("rejection issued RPC: %+v %v", res, err)
	}
}

func TestTimeoutPreservesPendingAndRecoveryReusesExactOperation(t *testing.T) {
	claim := freshPendingRecord(t)
	repo := &stubRecoveryRepo{claim: claim, valid: true}
	accounting := &stubAccounting{err: fmt.Errorf("network: %w", context.DeadlineExceeded)}
	usecase, _ := NewPlaceOrder(repo, accounting, time.Second, time.Second)
	res, err := usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrReservationOutcomeUnknown) || res.Record.State != domain.AdmissionPending || res.Confirmed != nil {
		t.Fatalf("timeout incorrectly completed admission: %+v %v", res, err)
	}
	// Simulate PostgreSQL reclaim after expiry: only generation/owner change.
	claim.Created = false
	claim.Lease = &Lease{Owner: "aaaa0000-aaaa-4bbb-8ccc-111122223333", Generation: 2, Until: time.Now().Add(30 * time.Second)}
	repo.reclaim = claim
	accounting.err = nil
	res, err = usecase.Recover(context.Background(), 42)
	if err != nil || res.Confirmed == nil || res.Record.State != domain.AdmissionAccepted {
		t.Fatalf("recovery failed: %+v %v", res, err)
	}
	if len(accounting.requests) != 2 || accounting.requests[0] != accounting.requests[1] {
		t.Fatalf("recovery changed reservation operation ID: %+v", accounting.requests)
	}
}

func TestLeaseChecksAndInvalidEvidenceFailClosed(t *testing.T) {
	claim := freshPendingRecord(t)
	repo := &stubRecoveryRepo{claim: claim, valid: false}
	accounting := &stubAccounting{}
	usecase, _ := NewPlaceOrder(repo, accounting, time.Second, time.Second)
	_, err := usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrAdmissionLeaseLost) {
		t.Fatalf("stale claimant returned confirmation: %v", err)
	}
	repo.valid = true
	accounting.response = ReservationEvidence{ReservationID: "r", AccountID: "WRONG", Active: true}
	_, err = usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrReservationEvidenceInvalid) {
		t.Fatalf("mismatched evidence accepted: %v", err)
	}
	claim.Lease.Until = time.Now().Add(800 * time.Millisecond)
	repo.claim = claim
	accounting.response = ReservationEvidence{}
	_, err = usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrAdmissionLeaseTooShort) || len(accounting.requests) != 2 {
		t.Fatalf("expired lease allowed RPC: %v calls %d", err, len(accounting.requests))
	}
}

func TestTXBFailureNeverReturnsConfirmedAcceptance(t *testing.T) {
	claim := freshPendingRecord(t)
	repo := &stubRecoveryRepo{claim: claim, valid: true, finalizeErr: ErrAdmissionLeaseLost}
	accounting := &stubAccounting{}
	usecase, _ := NewPlaceOrder(repo, accounting, time.Second, time.Second)
	got, err := usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrAdmissionLeaseLost) || got.Confirmed != nil ||
		got.Record.State != domain.AdmissionPending || repo.finalizes != 1 {
		t.Fatalf("fenced TX B falsely accepted: %+v %v", got, err)
	}
	repo.finalizeErr = ErrAdmissionCommitUnknown
	got, err = usecase.Place(context.Background(), "acct-1", placeInput())
	if !errors.Is(err, ErrAdmissionCommitUnknown) || got.Confirmed != nil ||
		got.Record.State != domain.AdmissionPending {
		t.Fatalf("ambiguous TX B falsely accepted: %+v %v", got, err)
	}
}
