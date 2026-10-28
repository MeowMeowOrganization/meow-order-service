//go:build integration

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const insertPendingOrderSQL = `
INSERT INTO order_service.orders (
    account_id, client_order_id, request_fingerprint, market_id, side,
    time_in_force, price_decimal, quantity_decimal, admission_status,
    market_numeric_id, market_config_version, base_asset, quote_asset,
    price_scale, base_scale, quote_scale, price_tick_units,
    quantity_step_base_atoms, price_units, quantity_base_atoms,
    notional_quote_atoms, price_ticks, quantity_lots,
    reservation_asset, reservation_atoms
) VALUES (
    'test-account', $1, decode(repeat('ab', 32), 'hex'), 'BTC-USDT', 1,
    1, '60000.1', '0.012', 'PENDING',
    7, 3, 'BTC', 'USDT',
    2, 8, 6, 5,
    100000, 6000010, 1200000,
    720001200, 1200002, 12,
    'USDT', 720001200
) RETURNING order_id`

func newMigratedTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ORDER_SERVICE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("ORDER_SERVICE_TEST_DATABASE_URL must point to a LOCAL disposable PostgreSQL instance")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid local test DSN")
	}
	if (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") ||
		strings.TrimPrefix(parsed.Path, "/") != "meow_order_service" {
		t.Fatal("refusing integration test unless DSN names local meow_order_service database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("connect to local PostgreSQL:", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		t.Fatal("ping local PostgreSQL:", err)
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	name := "cex18_it_" + hex.EncodeToString(token[:])
	// The DB name is generated exclusively from hex characters, not user input.
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatal("create disposable test DB:", err)
	}
	parsed.Path = "/" + name
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		admin.Close()
		t.Fatal("connect to disposable DB:", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanCancel()
		if _, err := admin.Exec(cleanCtx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop disposable test DB: %v", err)
		}
		admin.Close()
	})
	bytes, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000001_order_admission.sql"))
	if err != nil {
		t.Fatal(err)
	}
	// PostgreSQL extended protocol forbids multi-command prepared statements.
	// This executes the migration BEGIN..COMMIT as a simple-protocol batch.
	if _, err := pool.Exec(ctx, string(bytes), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply admission migration: %v", err)
	}
	return pool
}

func insertPending(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx, insertPendingOrderSQL, key).Scan(&id); err != nil {
		t.Fatal("insert PENDING:", err)
	}
	return id
}

func outboxInsertSQL() string {
	return `INSERT INTO order_service.matching_outbox
	    (order_id, command_id, market_numeric_id, kafka_topic, kafka_key, payload)
	    VALUES ($1, $2, 7, 'matching.commands.v1', '7', $3)`
}

func acceptSQL() string {
	return `UPDATE order_service.orders
	    SET admission_status = 'ACCEPTED',
	        reservation_id = $2, reservation_confirmed_at = now(),
	        accepted_at = now(), updated_at = now()
	    WHERE order_id = $1 AND admission_status = 'PENDING'`
}

func assertPgCode(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		t.Fatalf("expected PostgreSQL SQLSTATE %s, got %v", code, err)
	}
}

func TestAdmissionMigrationDuplicateKeyAndImmutableSnapshot(t *testing.T) {
	pool := newMigratedTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := insertPending(t, ctx, pool, "same-key")
	if id <= 0 {
		t.Fatal("expected positive canonical order ID")
	}
	_, err := pool.Exec(ctx, `INSERT INTO order_service.orders
	    (account_id, client_order_id, request_fingerprint, market_id, side,
	     time_in_force, price_decimal, quantity_decimal, admission_status,
	     rejected_at, rejection_code)
	    VALUES ('test-account', 'same-key', decode(repeat('cd', 32),'hex'),
	     'BTC-USDT', 1, 1, '10', '1', 'REJECTED', now(), 'INVALID_TICK')`)
	assertPgCode(t, err, "23505")
	// Same client ID belongs to a different account idempotency scope.
	_, err = pool.Exec(ctx, `INSERT INTO order_service.orders
	    (account_id, client_order_id, request_fingerprint, market_id, side,
	     time_in_force, price_decimal, quantity_decimal, admission_status,
	     rejected_at, rejection_code)
	    VALUES ('another-account', 'same-key', decode(repeat('cd', 32),'hex'),
	     'BTC-USDT', 1, 1, '10', '1', 'REJECTED', now(), 'INVALID_TICK')`)
	if err != nil {
		t.Fatalf("separate account must have independent client key: %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE order_service.orders SET price_decimal='99' WHERE order_id=$1`, id)
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(ctx, `DELETE FROM order_service.orders WHERE order_id=$1`, id)
	assertPgCode(t, err, "23514")
}

func TestDuplicateConcurrentAdmissionClaimsOnlyOneOrder(t *testing.T) {
	pool := newMigratedTestDatabase(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var id int64
			results <- pool.QueryRow(ctx, insertPendingOrderSQL, "concurrent-key").Scan(&id)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			conflicted++
		} else {
			t.Fatalf("unexpected concurrent admission error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("want 1 committed and 1 unique violation: got %d and %d", succeeded, conflicted)
	}
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM order_service.orders WHERE client_order_id='concurrent-key'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("persisted count=%d err=%v", count, err)
	}
}

func TestAcceptedOrderRequiresOutboxAtCommit(t *testing.T) {
	pool := newMigratedTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := insertPending(t, ctx, pool, "requires-outbox")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, acceptSQL(), id, "reservation-1"); err != nil {
		t.Fatal(err)
	}
	// Deliberately omit the outbox record. The DEFERRABLE CONSTRAINT
	// TRIGGER must reject this at commit, not merely by application policy.
	assertPgCode(t, tx.Commit(ctx), "23514")
	var status string
	if err := pool.QueryRow(ctx, `SELECT admission_status FROM order_service.orders WHERE order_id=$1`, id).Scan(&status); err != nil || status != "PENDING" {
		t.Fatalf("expected rolled-back PENDING, status=%q err=%v", status, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("outbox unexpectedly persisted: count=%d err=%v", count, err)
	}
}

func TestRollbackAndCommitAreAtomicWithOutbox(t *testing.T) {
	pool := newMigratedTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := insertPending(t, ctx, pool, "rollback-boundary")
	commandID := fmt.Sprintf("place:%d", id)
	payload := []byte{0x08, 0x01}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, acceptSQL(), id, "reservation-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, outboxInsertSQL(), id, commandID, payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	var count int
	if err := pool.QueryRow(ctx, `SELECT admission_status FROM order_service.orders WHERE order_id=$1`, id).Scan(&status); err != nil || status != "PENDING" {
		t.Fatalf("after rollback status=%q err=%v", status, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("after rollback outbox count=%d err=%v", count, err)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, acceptSQL(), id, "reservation-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, outboxInsertSQL(), id, commandID, payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT admission_status FROM order_service.orders WHERE order_id=$1`, id).Scan(&status); err != nil || status != "ACCEPTED" {
		t.Fatalf("after commit status=%q err=%v", status, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1 AND command_id=$2`, id, commandID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("after commit outbox count=%d err=%v", count, err)
	}
	_, err = pool.Exec(ctx, `UPDATE order_service.matching_outbox SET payload=$2 WHERE order_id=$1`, id, []byte{0x08, 0x02})
	assertPgCode(t, err, "23514")
	_, err = pool.Exec(ctx, `UPDATE order_service.orders SET admission_status='REJECTED', rejected_at=now(), rejection_code='X', accepted_at=NULL WHERE order_id=$1`, id)
	assertPgCode(t, err, "23514")
}

func TestOutboxCannotCommitWithoutAcceptedParent(t *testing.T) {
	pool := newMigratedTestDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := insertPending(t, ctx, pool, "pending-parent")
	_, err := pool.Exec(ctx, outboxInsertSQL(), id, fmt.Sprintf("place:%d", id), []byte{0x08, 0x01})
	assertPgCode(t, err, "23514")
}
