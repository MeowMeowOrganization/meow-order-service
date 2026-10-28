//go:build integration

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
)

func TestCEX183RecoveryFencesStaleLeaseWithoutChangingFrozenPlan(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx := context.Background()
	first, err := repo.Claim(ctx, "acct-1", newSubmission("recovery-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Lease == nil {
		t.Fatal("missing initial lease")
	}
	no, err := repo.ReclaimExpired(ctx, first.Record.OrderID)
	if err != nil || no.Lease != nil {
		t.Fatalf("unexpired lease reclaimed: %+v %v", no, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE order_service.orders SET admission_lease_until=clock_timestamp()-interval '1 second'
        WHERE order_id=$1`, first.Record.OrderID); err != nil {
		t.Fatal(err)
	}

	const contenders = 10
	start := make(chan struct{})
	results := make(chan app.ClaimResult, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := repo.ReclaimExpired(ctx, first.Record.OrderID)
			if err != nil {
				errs <- err
			} else {
				results <- result
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("reclaim: %v", err)
	}
	var winner *app.Lease
	for r := range results {
		if r.Record.OrderID != first.Record.OrderID || r.Record.State != domain.AdmissionPending ||
			r.Record.Reservation.Atoms != first.Record.Reservation.Atoms ||
			r.Record.Canonical.Market.ConfigVersion != first.Record.Canonical.Market.ConfigVersion {
			t.Fatalf("recovery modified canonical order: %+v", r)
		}
		if r.Lease != nil {
			if winner != nil {
				t.Fatal("multiple recovery lease owners")
			}
			winner = r.Lease
		}
	}
	if winner == nil || winner.Generation != first.Lease.Generation+1 || winner.Owner == first.Lease.Owner {
		t.Fatalf("wrong recovered lease: %+v", winner)
	}
	oldValid, err := repo.LeaseValid(ctx, first.Record.OrderID, *first.Lease)
	if err != nil || oldValid {
		t.Fatalf("stale owner not fenced: %v %v", oldValid, err)
	}
	newValid, err := repo.LeaseValid(ctx, first.Record.OrderID, *winner)
	if err != nil || !newValid {
		t.Fatalf("new owner invalid: %v %v", newValid, err)
	}
	var status, reservationID string
	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT admission_status,coalesce(reservation_id,'') FROM order_service.orders WHERE order_id=$1`, first.Record.OrderID).Scan(&status, &reservationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || reservationID != "" || outboxCount != 0 {
		t.Fatalf("CEX18.3 wrote TX B: %s %s %d", status, reservationID, outboxCount)
	}
}

func TestCEX183ReclaimDoesNotChangeRejectedOrMissingAdmission(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rejected := newSubmission("rejected-recovery")
	rejected.Price = "0"
	claim, err := repo.Claim(ctx, "acct-1", rejected)
	if err != nil || claim.Record.State != domain.AdmissionRejected {
		t.Fatalf("bad reject %+v %v", claim, err)
	}
	got, err := repo.ReclaimExpired(ctx, claim.Record.OrderID)
	if err != nil || got.Lease != nil || got.Record.State != domain.AdmissionRejected {
		t.Fatalf("reclaimed rejected order: %+v %v", got, err)
	}
	_, err = repo.ReclaimExpired(ctx, 999999999)
	if err != app.ErrAdmissionNotFound {
		t.Fatalf("missing order: %v", err)
	}
}
