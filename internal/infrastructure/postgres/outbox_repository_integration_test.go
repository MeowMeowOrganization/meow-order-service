//go:build integration

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	outboxapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newOutboxTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// Prior milestone harness creates its own random disposable database.
	db := newAdmissionRepositoryTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	sql, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000003_outbox_publisher.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply 000003: %v", err)
	}
	return db
}

// These outbox fixtures run against migrations 000001, 000002 and 000003.
// Do not reuse insertPendingOrderSQL: it intentionally omits the fields added
// in 000002 and is used elsewhere to assert that incomplete PENDING orders fail.
const insertOutboxFixtureOrderSQL = `
INSERT INTO order_service.orders (
    account_id, client_order_id, request_fingerprint, market_id, side,
    time_in_force, price_decimal, quantity_decimal, admission_status,
    market_numeric_id, market_config_version, base_asset, quote_asset,
    price_scale, base_scale, quote_scale, price_tick_units,
    quantity_step_base_atoms, price_units, quantity_base_atoms,
    notional_quote_atoms, price_ticks, quantity_lots,
    reservation_asset, reservation_atoms,
    market_min_quantity_base_atoms, market_min_notional_quote_atoms,
    market_status, reservation_policy_id, reservation_policy_version
) VALUES (
    'test-account', $1, decode(repeat('ab', 32), 'hex'), $2, 1,
    1, '60000.1', '0.012', 'PENDING',
    $3, 3, $4, 'USDT',
    2, 8, 6, 5,
    100000, 6000010, 1200000,
    720001200, 1200002, 12,
    'USDT', 720001200,
    100000, 1000000,
    'ACTIVE', 'test.outbox.no_fee', 1
) RETURNING order_id`

const insertOutboxFixtureCommandSQL = `
INSERT INTO order_service.matching_outbox
    (order_id, command_id, market_numeric_id, kafka_topic, kafka_key, payload)
VALUES ($1, $2, $3, 'matching.commands.v1', $4, $5)`

func committedOutbox(t *testing.T, db *pgxpool.Pool, key string, market int64) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var marketID, baseAsset string
	switch market {
	case 7:
		marketID, baseAsset = "BTC-USDT", "BTC"
	case 9:
		marketID, baseAsset = "ETH-USDT", "ETH"
	default:
		t.Fatalf("unsupported outbox fixture market: %d", market)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	var orderID int64
	if err := tx.QueryRow(ctx, insertOutboxFixtureOrderSQL, key, marketID, market, baseAsset).Scan(&orderID); err != nil {
		t.Fatalf("insert complete PENDING outbox fixture: %v", err)
	}
	if _, err := tx.Exec(ctx, acceptSQL(), orderID, "reservation-"+key); err != nil {
		t.Fatal(err)
	}
	payload := []byte("original-matching-command-" + key)
	if _, err := tx.Exec(ctx, insertOutboxFixtureCommandSQL,
		orderID, fmt.Sprintf("place:%d", orderID), market, fmt.Sprintf("%d", market), payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return orderID
}

func newOutboxRepo(t *testing.T, db *pgxpool.Pool) *OutboxRepository {
	t.Helper()
	repo, err := NewOutboxRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestOutboxConcurrentClaimsOnePerMarket(t *testing.T) {
	db := newOutboxTestDatabase(t)
	for i := 0; i < 3; i++ {
		committedOutbox(t, db, fmt.Sprintf("btc-%d", i), 7)
	}
	committedOutbox(t, db, "eth-0", 9)
	committedOutbox(t, db, "eth-1", 9)
	repo := newOutboxRepo(t, db)
	const workers = 8
	var wg sync.WaitGroup
	begin := make(chan struct{})
	results := make(chan []outboxapp.Message, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-begin
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := repo.Claim(ctx, 8, 30*time.Second)
			results <- got
			errs <- err
		}()
	}
	close(begin)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	claimed := map[uint32]outboxapp.Message{}
	for rows := range results {
		for _, row := range rows {
			if _, ok := claimed[row.MarketNumericID]; ok {
				t.Fatalf("same market claimed twice concurrently: market=%d", row.MarketNumericID)
			}
			claimed[row.MarketNumericID] = row
		}
	}
	if len(claimed) != 2 {
		t.Fatalf("want one claim for each market, got %v", claimed)
	}
	for market, row := range claimed {
		if row.LeaseGeneration != 1 || row.AttemptCount != 1 || row.LeaseOwner == "" || row.MarketNumericID != market {
			t.Fatalf("invalid claim %v", row)
		}
	}
	// A later BTC command cannot overtake its unacknowledged predecessor.
	ctx := context.Background()
	more, err := repo.Claim(ctx, 8, 30*time.Second)
	if err != nil || len(more) != 0 {
		t.Fatalf("overtook in-flight market head: %v, %v", more, err)
	}
	if err := repo.MarkSent(ctx, claimed[7]); err != nil {
		t.Fatal(err)
	}
	next, err := repo.Claim(ctx, 8, 30*time.Second)
	if err != nil || len(next) != 1 || next[0].MarketNumericID != 7 || next[0].ID <= claimed[7].ID {
		t.Fatalf("incorrect next market head: %v, %v", next, err)
	}
}

func TestOutboxAckLossReplaysSameCommandAndFencesOldOwner(t *testing.T) {
	db := newOutboxTestDatabase(t)
	committedOutbox(t, db, "ack-lost", 7)
	repo := newOutboxRepo(t, db)
	ctx := context.Background()
	first, err := repo.Claim(ctx, 1, 10*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v %v", first, err)
	}
	// Simulate a crash after Kafka accepted the command but before SENT update.
	// We advance the lease ONLY inside the disposable test DB.
	if _, err := db.Exec(ctx, `UPDATE order_service.matching_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE outbox_id=$1`, first[0].ID); err != nil {
		t.Fatal(err)
	}
	second, err := repo.Claim(ctx, 1, 10*time.Second)
	if err != nil || len(second) != 1 {
		t.Fatalf("reclaim: %v %v", second, err)
	}
	if second[0].ID != first[0].ID || second[0].CommandID != first[0].CommandID || !bytes.Equal(second[0].Payload, first[0].Payload) ||
		second[0].LeaseGeneration != first[0].LeaseGeneration+1 || second[0].LeaseOwner == first[0].LeaseOwner || second[0].AttemptCount != 2 {
		t.Fatalf("redelivery mutated command or reused fence: first=%+v second=%+v", first[0], second[0])
	}
	if err := repo.MarkSent(ctx, first[0]); !errors.Is(err, outboxapp.ErrLeaseLost) {
		t.Fatalf("stale ACK succeeded: %v", err)
	}
	if err := repo.Retry(ctx, first[0], 0); !errors.Is(err, outboxapp.ErrLeaseLost) {
		t.Fatalf("stale retry succeeded: %v", err)
	}
	if err := repo.MarkSent(ctx, second[0]); err != nil {
		t.Fatal(err)
	}
	again, err := repo.Claim(ctx, 1, 10*time.Second)
	if err != nil || len(again) != 0 {
		t.Fatalf("SENT re-claimed: %v, %v", again, err)
	}
	var status string
	var published *time.Time
	if err := db.QueryRow(ctx, `SELECT publication_status, published_at FROM order_service.matching_outbox WHERE outbox_id=$1`, second[0].ID).Scan(&status, &published); err != nil {
		t.Fatal(err)
	}
	if status != "SENT" || published == nil {
		t.Fatalf("not durably SENT: %q %v", status, published)
	}
}

func TestOutboxFailureSchedulesRetryWithoutOvertaking(t *testing.T) {
	db := newOutboxTestDatabase(t)
	committedOutbox(t, db, "fail-head", 7)
	committedOutbox(t, db, "later-head", 7)
	repo := newOutboxRepo(t, db)
	ctx := context.Background()
	first, err := repo.Claim(ctx, 10, 15*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v %v", first, err)
	}
	if err := repo.Retry(ctx, first[0], 30*time.Second); err != nil {
		t.Fatal(err)
	}
	next, err := repo.Claim(ctx, 10, 15*time.Second)
	if err != nil || len(next) != 0 {
		t.Fatalf("newer market command overtook retry: %v %v", next, err)
	}
	if _, err := db.Exec(ctx, `UPDATE order_service.matching_outbox SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE outbox_id=$1`, first[0].ID); err != nil {
		t.Fatal(err)
	}
	retry, err := repo.Claim(ctx, 10, 15*time.Second)
	if err != nil || len(retry) != 1 || retry[0].ID != first[0].ID || retry[0].AttemptCount != 2 {
		t.Fatalf("retry skipped original command: %v %v", retry, err)
	}
}

func TestOutboxSkipLockedDoesNotBlockOtherMarkets(t *testing.T) {
	db := newOutboxTestDatabase(t)
	firstID := committedOutbox(t, db, "locked", 7)
	committedOutbox(t, db, "not-locked", 9)
	repo := newOutboxRepo(t, db)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT outbox_id FROM order_service.matching_outbox WHERE order_id=$1 FOR UPDATE`, firstID); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	found, err := repo.Claim(deadline, 8, 15*time.Second)
	if err != nil || len(found) != 1 || found[0].MarketNumericID != 9 {
		t.Fatalf("SKIP LOCKED blocked independent market: %v %v", found, err)
	}
}
