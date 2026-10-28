//go:build integration

package postgres

import (
	"context"
	"errors"
	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The CEX 18.1 integration harness uses exclusively a randomly named local
// disposable PostgreSQL DATABASE and drops only the database it created.
func newAdmissionRepositoryTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := newMigratedTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000002_admission_repository.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(raw), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply 000002: %v", err)
	}
	return pool
}

const seedMarketSQL = `INSERT INTO order_service.market_catalog (
    market_id,numeric_market_id,config_version,base_asset,quote_asset,
    base_scale,quote_scale,price_scale,price_tick_units,quantity_step_base_atoms,
    min_quantity_base_atoms,min_notional_quote_atoms,status
) VALUES ('BTC-USDT',7,3,'BTC','USDT',8,6,2,5,100000,100000,1000000,'ACTIVE')`

func seedMarket(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), seedMarketSQL); err != nil {
		t.Fatal(err)
	}
}

type testReservationPlanner struct {
	calls   atomic.Int64
	failure error
	invalid bool
}

func (p *testReservationPlanner) PlanReservation(_ string, c domain.CanonicalLimit) (app.ReservationPlan, error) {
	p.calls.Add(1)
	if p.failure != nil {
		return app.ReservationPlan{}, p.failure
	}
	switch c.Submission.Side {
	case domain.SideSell:
		return app.ReservationPlan{Asset: c.Market.BaseAsset, Atoms: c.QuantityBaseAtoms, PolicyID: "test.cex15", PolicyVersion: 3}, nil
	default:
		if p.invalid {
			return app.ReservationPlan{Asset: c.Market.QuoteAsset, Atoms: c.NotionalQuoteAtoms - 1, PolicyID: "test.cex15", PolicyVersion: 3}, nil
		}
		return app.ReservationPlan{Asset: c.Market.QuoteAsset, Atoms: c.NotionalQuoteAtoms + 17, PolicyID: "test.cex15", PolicyVersion: 3}, nil
	}
}

func repoForTest(t *testing.T, pool *pgxpool.Pool, planner app.ReservationPlanner) *AdmissionRepository {
	t.Helper()
	repo, err := NewAdmissionRepository(pool, planner, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}
func newSubmission(key string) domain.LimitSubmission {
	return domain.LimitSubmission{ClientOrderID: key, MarketID: "BTC-USDT", Side: domain.SideBuy,
		Price: "060000.100", Quantity: "0.01200000", TimeInForce: domain.TimeInForceGTC}
}
func rowCount(t *testing.T, pool *pgxpool.Pool, table, key string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM order_service."+table+" WHERE client_order_id=$1", key).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTXACommitAndIdempotentReplayUsesFrozenSnapshot(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	planner := &testReservationPlanner{}
	repo := repoForTest(t, pool, planner)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := repo.Claim(ctx, "acct-1", newSubmission("txa-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Lease == nil || first.Lease.Generation != 1 ||
		first.Lease.Owner == "" || !first.Lease.Until.After(time.Now()) ||
		first.Record.State != domain.AdmissionPending {
		t.Fatalf("invalid new claim: %+v", first)
	}
	if first.Record.Canonical == nil || first.Record.Reservation == nil {
		t.Fatal("missing canonical/fee snapshot")
	}
	c := first.Record.Canonical
	if c.PriceTicks != 1200002 || c.QuantityLots != 12 || c.NotionalQuoteAtoms != 720001200 ||
		c.Market.ConfigVersion != 3 || c.Market.MinQuantityBaseAtoms != 100000 ||
		first.Record.Reservation.Atoms != 720001217 || first.Record.Reservation.PolicyVersion != 3 {
		t.Fatalf("bad snapshot: %+v, %+v", c, first.Record.Reservation)
	}
	op, err := first.Record.ReservationOperationID()
	if err != nil || op != "order-reserve:"+strconv.FormatInt(first.Record.OrderID, 10) {
		t.Fatalf("bad operation id %q: %v", op, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("TX A published outbox: %d, %v", count, err)
	}
	var status, reservationID string
	var gen int64
	if err := pool.QueryRow(ctx, `SELECT admission_status,coalesce(reservation_id,''),admission_lease_generation
        FROM order_service.orders WHERE order_id=$1`, first.Record.OrderID).Scan(&status, &reservationID, &gen); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || reservationID != "" || gen != 1 {
		t.Fatalf("invalid TX A state %s %s %d", status, reservationID, gen)
	}

	// The market has now changed: duplicates must NOT run market validation or
	// fee planning a second time and must preserve the original frozen rules.
	if _, err := pool.Exec(ctx, `UPDATE order_service.market_catalog SET status='HALTED',config_version=4 WHERE market_id='BTC-USDT'`); err != nil {
		t.Fatal(err)
	}
	duplicate := newSubmission("txa-1")
	duplicate.Price = "60000.10"
	duplicate.Quantity = "0.012"
	replay, err := repo.Claim(ctx, "acct-1", duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Created || replay.Lease != nil || replay.Record.OrderID != first.Record.OrderID ||
		replay.Record.Canonical.Market.ConfigVersion != 3 || replay.Record.Reservation.Atoms != 720001217 ||
		planner.calls.Load() != 1 {
		t.Fatalf("duplicate changed identity or reservation: %+v calls=%d", replay, planner.calls.Load())
	}

	conflict := newSubmission("txa-1")
	conflict.Price = "59999.95"
	_, err = repo.Claim(ctx, "acct-1", conflict)
	if !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	if rowCount(t, pool, "orders", "txa-1") != 1 {
		t.Fatal("idempotency duplicate persisted")
	}
	_, err = pool.Exec(ctx, `UPDATE order_service.orders SET market_min_notional_quote_atoms=2 WHERE order_id=$1`, first.Record.OrderID)
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(ctx, `UPDATE order_service.orders SET reservation_policy_version=4 WHERE order_id=$1`, first.Record.OrderID)
	assertPgCode(t, err, "23514")
}

func TestTXARejectionsPersistAndAreIdempotent(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	planner := &testReservationPlanner{}
	repo := repoForTest(t, pool, planner)
	ctx := context.Background()
	tests := []struct{ key, price, qty, want string }{
		{"reject-tick", "60000.11", "0.012", "PRICE_OFF_TICK"},
		{"reject-lot", "60000.10", "0.01200001", "QUANTITY_OFF_LOT"},
		{"reject-zero", "0", "0.012", "INVALID_PRICE"},
		{"reject-precision", "60000.101", "0.012", "INVALID_PRECISION"},
	}
	for _, tt := range tests {
		req := newSubmission(tt.key)
		req.Price = tt.price
		req.Quantity = tt.qty
		got, err := repo.Claim(ctx, "acct-1", req)
		if err != nil {
			t.Fatalf("%s: %v", tt.key, err)
		}
		if !got.Created || got.Lease != nil || got.Record.State != domain.AdmissionRejected ||
			got.Record.RejectionCode != tt.want || got.Record.Canonical != nil {
			t.Fatalf("%s: %+v", tt.key, got)
		}
		again, err := repo.Claim(ctx, "acct-1", req)
		if err != nil || again.Created || again.Record.OrderID != got.Record.OrderID || again.Record.RejectionCode != tt.want {
			t.Fatalf("replay rejection %s: %+v err=%v", tt.key, again, err)
		}
	}
	missing := newSubmission("missing-market")
	missing.MarketID = "DOGE-USDT"
	got, err := repo.Claim(ctx, "acct-1", missing)
	if err != nil || got.Record.RejectionCode != "MARKET_NOT_FOUND" {
		t.Fatalf("missing market %+v %v", got, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE order_service.market_catalog SET status='HALTED',config_version=4 WHERE market_id='BTC-USDT'`); err != nil {
		t.Fatal(err)
	}
	halted, err := repo.Claim(ctx, "acct-1", newSubmission("market-halted"))
	if err != nil || halted.Record.RejectionCode != "MARKET_HALTED" {
		t.Fatalf("halted: %+v %v", halted, err)
	}
	if planner.calls.Load() != 0 {
		t.Fatalf("planner called for rejected orders %d", planner.calls.Load())
	}
	var outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox`).Scan(&outbox); err != nil || outbox != 0 {
		t.Fatalf("outbox=%d err=%v", outbox, err)
	}
}

func TestTXAConcurrentClaimSingleIdentityAndSingleLease(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	const workers = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan app.ClaimResult, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			res, err := repo.Claim(ctx, "acct-1", newSubmission("same-key-parallel"))
			if err != nil {
				errs <- err
			} else {
				results <- res
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("parallel claim: %v", err)
	}
	var id int64
	created, leased, total := 0, 0, 0
	for res := range results {
		total++
		if id == 0 {
			id = res.Record.OrderID
		}
		if res.Record.OrderID != id {
			t.Error("multiple order identities")
		}
		if res.Created {
			created++
		}
		if res.Lease != nil {
			leased++
		}
	}
	if total != workers || created != 1 || leased != 1 {
		t.Fatalf("total=%d created=%d leased=%d", total, created, leased)
	}
	if rowCount(t, pool, "orders", "same-key-parallel") != 1 {
		t.Fatal("expected exactly one durable order")
	}
}

func TestTXAConflictRaceNoSecondOrder(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	start := make(chan struct{})
	type outcome struct {
		res app.ClaimResult
		err error
	}
	results := make(chan outcome, 2)
	for _, price := range []string{"60000.10", "59999.95"} {
		price := price
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s := newSubmission("race-different")
			s.Price = price
			r, e := repo.Claim(ctx, "acct-1", s)
			results <- outcome{r, e}
		}()
	}
	close(start)
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		v := <-results
		switch {
		case v.err == nil:
			success++
		case errors.Is(v.err, app.ErrIdempotencyConflict):
			conflict++
		default:
			t.Errorf("unexpected %v", v.err)
		}
	}
	if success != 1 || conflict != 1 || rowCount(t, pool, "orders", "race-different") != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
}

func TestTXARollbackOnPolicyFailureAndPreflight(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	planner := &testReservationPlanner{failure: errors.New("fee schedule unavailable")}
	repo := repoForTest(t, pool, planner)
	ctx := context.Background()
	_, err := repo.Claim(ctx, "acct-1", newSubmission("policy-down"))
	if err == nil || rowCount(t, pool, "orders", "policy-down") != 0 {
		t.Fatal("policy failure was persisted as a rejection/pending")
	}
	planner.failure = nil
	planner.invalid = true
	_, err = repo.Claim(ctx, "acct-1", newSubmission("under-reserve"))
	if !errors.Is(err, app.ErrInvalidReservationPlan) || rowCount(t, pool, "orders", "under-reserve") != 0 {
		t.Fatalf("unsafe quote persisted %v", err)
	}
	invalid := newSubmission("malformed")
	invalid.Quantity = "1e-3"
	if _, err := repo.Claim(ctx, "acct-1", invalid); !errors.Is(err, domain.ErrInvalidDecimal) {
		t.Fatalf("invalid decimal %v", err)
	}
	if rowCount(t, pool, "orders", "malformed") != 0 {
		t.Fatal("malformed input persisted")
	}
}

func TestTXAAccountsAreSeparateAndMarketVersionIsFenced(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	a, err := repo.Claim(context.Background(), "account-A", newSubmission("shared-client-id"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := repo.Claim(context.Background(), "account-B", newSubmission("shared-client-id"))
	if err != nil {
		t.Fatal(err)
	}
	if a.Record.OrderID == b.Record.OrderID || !a.Created || !b.Created {
		t.Fatal("account scoping failed")
	}
	// Catalog revisions must increment monotonically and never reassign the
	// numeric market ID of an existing market.
	_, err = pool.Exec(context.Background(), `UPDATE order_service.market_catalog SET config_version=5 WHERE market_id='BTC-USDT'`)
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(context.Background(), `UPDATE order_service.market_catalog SET config_version=4,numeric_market_id=9 WHERE market_id='BTC-USDT'`)
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(context.Background(), `UPDATE order_service.market_catalog SET config_version=4,price_tick_units=10 WHERE market_id='BTC-USDT'`)
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(context.Background(), `DELETE FROM order_service.market_catalog WHERE market_id='BTC-USDT'`)
	assertPgCode(t, err, "23514")
}

func TestTXARejectsIncompletePendingInsertAtDatabaseBoundary(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	ctx := context.Background()
	// This was a valid 000001-era fixture; after 000002 no NEW PENDING row
	// may omit the versioned market/fee snapshot, even via direct SQL.
	var id int64
	err := pool.QueryRow(ctx, insertPendingOrderSQL, "incomplete-bypass").Scan(&id)
	assertPgCode(t, err, "23514")
}

func TestTXACatalogShareLockBlocksConcurrentRevision(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	reader, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback(context.Background())
	var version int64
	if err := reader.QueryRow(ctx, `SELECT config_version FROM order_service.market_catalog
        WHERE market_id='BTC-USDT' FOR SHARE`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("version=%d", version)
	}

	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background())
	if _, err := writer.Exec(ctx, `SET LOCAL lock_timeout='150ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = writer.Exec(ctx, `UPDATE order_service.market_catalog
        SET status='HALTED',config_version=4 WHERE market_id='BTC-USDT'`)
	assertPgCode(t, err, "55P03")
	if err := writer.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reader.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// The revision can succeed only after the snapshot reader commits.
	if _, err := pool.Exec(ctx, `UPDATE order_service.market_catalog
        SET status='HALTED',config_version=4 WHERE market_id='BTC-USDT'`); err != nil {
		t.Fatal(err)
	}
}

// CEX 18.4: TX B proves the ACCEPTED state and immutable matcher command are
// one PostgreSQL commit. Tests run on disposable databases created above.
func cex184Evidence(claim app.ClaimResult) app.ReservationEvidence {
	return app.ReservationEvidence{
		ReservationID:  "reservation-" + strconv.FormatInt(claim.Record.OrderID, 10),
		AccountID:      claim.Record.AccountID,
		ReferenceID:    strconv.FormatInt(claim.Record.OrderID, 10),
		Asset:          claim.Record.Reservation.Asset,
		OriginalAtoms:  claim.Record.Reservation.Atoms,
		RemainingAtoms: claim.Record.Reservation.Atoms,
		Active:         true,
	}
}

func TestCEX184FinalizeCommitsAcceptedAndExactMatchingCommand(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx := context.Background()
	claim, err := repo.Claim(ctx, "acct-184", newSubmission("finalize-accepted"))
	if err != nil || claim.Lease == nil {
		t.Fatalf("claim: %+v, %v", claim, err)
	}
	evidence := cex184Evidence(claim)
	accepted, err := repo.Finalize(ctx, claim.Record.OrderID, *claim.Lease, evidence)
	if err != nil || accepted.State != domain.AdmissionAccepted || accepted.OrderID != claim.Record.OrderID {
		t.Fatalf("TX B did not accept: %+v, %v", accepted, err)
	}
	var state, reservationID string
	var confirmedAt, acceptedAt time.Time
	var leaseOwner *string
	var leaseUntil *time.Time
	var leaseGeneration int64
	err = pool.QueryRow(ctx, `SELECT admission_status,reservation_id,reservation_confirmed_at,accepted_at,
		admission_lease_owner::text,admission_lease_until,admission_lease_generation
		FROM order_service.orders WHERE order_id=$1`, accepted.OrderID).Scan(
		&state, &reservationID, &confirmedAt, &acceptedAt, &leaseOwner, &leaseUntil, &leaseGeneration)
	if err != nil || state != "ACCEPTED" || reservationID != evidence.ReservationID ||
		confirmedAt.IsZero() || acceptedAt.IsZero() || leaseOwner != nil || leaseUntil != nil || leaseGeneration != 1 {
		t.Fatalf("incomplete ACCEPTED provenance: state=%s reservation=%s owner=%v until=%v gen=%d err=%v", state, reservationID, leaseOwner, leaseUntil, leaseGeneration, err)
	}
	var commandID, topic, key, publication string
	var marketID int64
	var payload []byte
	var count int
	err = pool.QueryRow(ctx, `SELECT command_id,kafka_topic,kafka_key,market_numeric_id,payload,publication_status
		FROM order_service.matching_outbox WHERE order_id=$1`, accepted.OrderID).
		Scan(&commandID, &topic, &key, &marketID, &payload, &publication)
	if err != nil {
		t.Fatal(err)
	}
	if commandID != "place:"+strconv.FormatInt(accepted.OrderID, 10) || topic != DefaultMatchingCommandTopic ||
		key != "7" || marketID != 7 || publication != "PENDING" {
		t.Fatalf("wrong outbox routing: %s %s %s %d %s", commandID, topic, key, marketID, publication)
	}
	var command matchingpb.MatchingCommand
	if err := proto.Unmarshal(payload, &command); err != nil {
		t.Fatal(err)
	}
	place := command.GetPlaceLimitOrder()
	if command.GetCommandId() != commandID || command.GetMarketId() != 7 || place == nil ||
		place.GetOrderId() != uint64(accepted.OrderID) || place.GetSide() != orderpb.OrderSide_ORDER_SIDE_BUY ||
		place.GetPriceTicks() != claim.Record.Canonical.PriceTicks ||
		place.GetQuantityLots() != claim.Record.Canonical.QuantityLots {
		t.Fatalf("outbox payload differs from frozen canonical order: %+v", &command)
	}
	// A duplicate HTTP Place uses the original ACCEPTED order without a lease.
	replay, err := repo.Claim(ctx, "acct-184", newSubmission("finalize-accepted"))
	if err != nil || replay.Created || replay.Lease != nil || replay.Record.State != domain.AdmissionAccepted || replay.Record.OrderID != accepted.OrderID {
		t.Fatalf("idempotent replay: %+v %v", replay, err)
	}
	if _, err := repo.Finalize(ctx, accepted.OrderID, *claim.Lease, evidence); !errors.Is(err, app.ErrAdmissionLeaseLost) {
		t.Fatalf("terminal TX B replay was allowed: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1`, accepted.OrderID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate outbox: %d %v", count, err)
	}
}

func TestCEX184ExpiredAndReclaimedFencesCannotFinalize(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx := context.Background()
	first, err := repo.Claim(ctx, "acct-184", newSubmission("fence-expired"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE order_service.orders SET admission_lease_until=clock_timestamp()-interval '1 second' WHERE order_id=$1`, first.Record.OrderID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Finalize(ctx, first.Record.OrderID, *first.Lease, cex184Evidence(first)); !errors.Is(err, app.ErrAdmissionLeaseLost) {
		t.Fatalf("expired lease finalized: %v", err)
	}
	assertCEX184PendingNoOutbox(t, pool, first.Record.OrderID)
	second, err := repo.ReclaimExpired(ctx, first.Record.OrderID)
	if err != nil || second.Lease == nil || second.Lease.Generation != first.Lease.Generation+1 {
		t.Fatalf("reclaim: %+v %v", second, err)
	}
	if _, err := repo.Finalize(ctx, first.Record.OrderID, *first.Lease, cex184Evidence(first)); !errors.Is(err, app.ErrAdmissionLeaseLost) {
		t.Fatalf("reclaimed old lease finalized: %v", err)
	}
	assertCEX184PendingNoOutbox(t, pool, first.Record.OrderID)
	accepted, err := repo.Finalize(ctx, second.Record.OrderID, *second.Lease, cex184Evidence(second))
	if err != nil || accepted.State != domain.AdmissionAccepted {
		t.Fatalf("new owner not permitted: %+v %v", accepted, err)
	}
}

func TestCEX184OutboxFailureRollsBackAcceptAndReservationEvidence(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx := context.Background()
	claim, err := repo.Claim(ctx, "acct-184", newSubmission("rollback-outbox"))
	if err != nil {
		t.Fatal(err)
	}
	// Only this disposable database receives the deterministic fault injection.
	_, err = pool.Exec(ctx, `CREATE FUNCTION order_service.cex184_fail_outbox() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure' USING ERRCODE='23514'; END; $$;
		CREATE TRIGGER cex184_fail_outbox BEFORE INSERT ON order_service.matching_outbox
		FOR EACH ROW EXECUTE FUNCTION order_service.cex184_fail_outbox();`, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Finalize(ctx, claim.Record.OrderID, *claim.Lease, cex184Evidence(claim))
	if err == nil {
		t.Fatal("outbox insert failure must roll back TX B")
	}
	assertCEX184PendingNoOutbox(t, pool, claim.Record.OrderID)
	// Rollback must leave the same live lease available for a valid retry.
	_, err = pool.Exec(ctx, `DROP TRIGGER cex184_fail_outbox ON order_service.matching_outbox`)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := repo.Finalize(ctx, claim.Record.OrderID, *claim.Lease, cex184Evidence(claim))
	if err != nil || accepted.State != domain.AdmissionAccepted {
		t.Fatalf("retry after SQL rollback: %+v %v", accepted, err)
	}
}

func TestCEX184InvalidEvidenceLeavesPendingAndNoOutbox(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	ctx := context.Background()
	claim, err := repo.Claim(ctx, "acct-184", newSubmission("bad-evidence"))
	if err != nil {
		t.Fatal(err)
	}
	evidence := cex184Evidence(claim)
	evidence.RemainingAtoms--
	_, err = repo.Finalize(ctx, claim.Record.OrderID, *claim.Lease, evidence)
	if !errors.Is(err, app.ErrReservationEvidenceInvalid) {
		t.Fatalf("accepted invalid evidence: %v", err)
	}
	assertCEX184PendingNoOutbox(t, pool, claim.Record.OrderID)
}

func assertCEX184PendingNoOutbox(t *testing.T, pool *pgxpool.Pool, orderID int64) {
	t.Helper()
	var state string
	var reservationID *string
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT admission_status,reservation_id FROM order_service.orders WHERE order_id=$1`, orderID).Scan(&state, &reservationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1`, orderID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if state != "PENDING" || reservationID != nil || count != 0 {
		t.Fatalf("TX B partially committed: state=%s reservation=%v outbox=%d", state, reservationID, count)
	}
}

func TestCEX184ConcurrentFinalizersProduceOneOutbox(t *testing.T) {
	pool := newAdmissionRepositoryTestDB(t)
	seedMarket(t, pool)
	repo := repoForTest(t, pool, &testReservationPlanner{})
	claim, err := repo.Claim(context.Background(), "acct-184", newSubmission("concurrent-finalize"))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := repo.Finalize(context.Background(), claim.Record.OrderID, *claim.Lease, cex184Evidence(claim))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var wins, fenced int
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, app.ErrAdmissionLeaseLost):
			fenced++
		default:
			t.Fatalf("unexpected concurrent finalization error: %v", err)
		}
	}
	if wins != 1 || fenced != 1 {
		t.Fatalf("expected one winner and one fenced writer; wins=%d fenced=%d", wins, fenced)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1`, claim.Record.OrderID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected exactly one durable outbox: %d %v", count, err)
	}
}
