//go:build integration

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func lifecycleDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := newOutboxTestDatabase(t) // isolated local DB, migrations 000001..000003
	sql, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000004_order_lifecycle.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(context.Background(), string(sql), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply 000004: %v", err)
	}
	return db
}

const lifecycleFixtureSQL = `INSERT INTO order_service.orders (
    account_id,client_order_id,request_fingerprint,market_id,side,time_in_force,
    price_decimal,quantity_decimal,admission_status,market_numeric_id,market_config_version,
    base_asset,quote_asset,price_scale,base_scale,quote_scale,price_tick_units,
    quantity_step_base_atoms,price_units,quantity_base_atoms,notional_quote_atoms,
    price_ticks,quantity_lots,reservation_asset,reservation_atoms,
    market_min_quantity_base_atoms,market_min_notional_quote_atoms,market_status,
    reservation_policy_id,reservation_policy_version
) VALUES (
    'test-lifecycle-account',$1,decode(repeat('ab',32),'hex'),$2,$3,1,
    '60000.1',$4,'PENDING',$5,3,$6,'USDT',2,8,6,5,
    100000,6000010,$7,$8,1200002,$9,$10,$11,
    100000,1000000,'ACTIVE','test.lifecycle.no_fee',1
) RETURNING order_id`

func lifecycleOrder(t *testing.T, db *pgxpool.Pool, market uint32, side int32, lots int64, name string) (int64, *matchingpb.MatchingCommand) {
	t.Helper()
	if lots <= 0 || lots > 999 {
		t.Fatal("invalid test lots")
	}
	marketName, base := "BTC-USDT", "BTC"
	if market == 9 {
		marketName, base = "ETH-USDT", "ETH"
	} else if market != 7 {
		t.Fatal("unsupported fixture market")
	}
	quantity := strings.TrimRight(fmt.Sprintf("0.%03d", lots), "0")
	baseAtoms, notional := lots*100000, lots*60000100
	asset, reserved := "USDT", notional
	if side == 2 {
		asset, reserved = base, baseAtoms
	}
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var id int64
	err = tx.QueryRow(ctx, lifecycleFixtureSQL, name, marketName, side, quantity, market, base,
		baseAtoms, notional, lots, asset, reserved).Scan(&id)
	if err != nil {
		t.Fatalf("insert complete lifecycle fixture: %v", err)
	}
	cmd := &matchingpb.MatchingCommand{CommandId: fmt.Sprintf("place:%d", id), MarketId: market,
		Command: &matchingpb.MatchingCommand_PlaceLimitOrder{PlaceLimitOrder: &matchingpb.PlaceLimitOrder{
			OrderId: uint64(id), Side: orderpb.OrderSide(side), PriceTicks: 1200002, QuantityLots: lots,
		}}}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, acceptSQL(), id, "reservation-"+name); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, insertOutboxFixtureCommandSQL, id, cmd.GetCommandId(), int64(market), fmt.Sprintf("%d", market), encoded); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return id, cmd
}

func lifecycleResult(cmd *matchingpb.MatchingCommand, seq uint64, events ...*matchingpb.SequencedMatchingEvent) *matchingpb.MatchingResult {
	return &matchingpb.MatchingResult{Command: cmd, MarketSequence: seq, Events: events,
		StateHashAfter: &matchingpb.MatchingStateHash{EncodingVersion: 1, Sha256: bytes.Repeat([]byte{byte(seq)}, 32)}}
}
func execEvent(market uint32, seq uint64, ord uint64, maker, taker int64, lots int64) *matchingpb.SequencedMatchingEvent {
	return &matchingpb.SequencedMatchingEvent{
		EventId: &matchingpb.MatchingEventId{MarketId: market, MarketSequence: seq, EventOrdinal: ord},
		Event: &matchingpb.SequencedMatchingEvent_Exectuion{Exectuion: &matchingpb.Execution{
			MakerOrderId: uint64(maker), TakerOrderId: uint64(taker), TakerSide: orderpb.OrderSide(1),
			PriceTicks: 1200002, QuantityLots: lots,
		}},
	}
}
func lifecycleState(t *testing.T, db *pgxpool.Pool, id int64) (string, int64, int64, int64) {
	t.Helper()
	var status string
	var filled, canceled, version int64
	if err := db.QueryRow(context.Background(), `SELECT status,filled_lots,cancelled_lots,version FROM order_service.order_lifecycle WHERE order_id=$1`, id).Scan(&status, &filled, &canceled, &version); err != nil {
		t.Fatal(err)
	}
	return status, filled, canceled, version
}
func requireResult(t *testing.T, r *LifecycleRepository, msg *matchingpb.MatchingResult, want ApplyOutcome) {
	t.Helper()
	got, err := r.ApplyMatchingResult(context.Background(), msg)
	if err != nil || got != want {
		t.Fatalf("ApplyMatchingResult: outcome=%q err=%v want=%q", got, err, want)
	}
}
func TestLifecycleMigrationInitializesAcceptedInTXB(t *testing.T) {
	db := lifecycleDatabase(t)
	id, _ := lifecycleOrder(t, db, 7, 1, 12, "txb")
	status, filled, canceled, version := lifecycleState(t, db, id)
	if status != "ACCEPTED" || filled != 0 || canceled != 0 || version != 0 {
		t.Fatalf("bad initialization: %s %d %d %d", status, filled, canceled, version)
	}
}
func TestLifecycleApplyOpenReplayAndConflictingDuplicate(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	id, cmd := lifecycleOrder(t, db, 7, 1, 12, "open")
	msg := lifecycleResult(cmd, 1)
	requireResult(t, repo, msg, ResultApplied)
	requireResult(t, repo, msg, ResultDuplicate)
	status, filled, canceled, version := lifecycleState(t, db, id)
	if status != "OPEN" || filled != 0 || canceled != 0 || version != 1 {
		t.Fatalf("non-idempotent replay: %s %d %d %d", status, filled, canceled, version)
	}
	changed := proto.Clone(msg).(*matchingpb.MatchingResult)
	changed.StateHashAfter.Sha256 = bytes.Repeat([]byte{99}, 32)
	if _, err := repo.ApplyMatchingResult(context.Background(), changed); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
}
func TestLifecycleMultiMakerAllOrNothing(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	maker1, c1 := lifecycleOrder(t, db, 7, 2, 12, "maker-1")
	maker2, c2 := lifecycleOrder(t, db, 7, 2, 12, "maker-2")
	taker, c3 := lifecycleOrder(t, db, 7, 1, 24, "taker")
	requireResult(t, repo, lifecycleResult(c1, 1), ResultApplied)
	requireResult(t, repo, lifecycleResult(c2, 2), ResultApplied)
	bad := lifecycleResult(c3, 3, execEvent(7, 3, 0, maker1, taker, 12), execEvent(7, 3, 1, maker2, taker, 13))
	if _, err := repo.ApplyMatchingResult(context.Background(), bad); err == nil {
		t.Fatal("overfill must fail")
	}
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM order_service.private_executions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rolled back partial executions %d %v", n, err)
	}
	if status, _, _, _ := lifecycleState(t, db, maker1); status != "OPEN" {
		t.Fatalf("rolled back maker mutated: %s", status)
	}
	good := lifecycleResult(c3, 3, execEvent(7, 3, 0, maker1, taker, 12), execEvent(7, 3, 1, maker2, taker, 12))
	requireResult(t, repo, good, ResultApplied)
	requireResult(t, repo, good, ResultDuplicate)
	for _, id := range []int64{maker1, maker2, taker} {
		status, filled, _, version := lifecycleState(t, db, id)
		wantVersion := int64(2)
		if id == taker {
			wantVersion = 1
		}
		if status != "FILLED" || filled == 0 || version != wantVersion {
			t.Fatalf("order %d: %s %d %d", id, status, filled, version)
		}
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM order_service.private_executions`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("private executions=%d err=%v", n, err)
	}
}
func TestLifecycleGapAndUntrustedResultDoNotMutate(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	id, cmd := lifecycleOrder(t, db, 7, 1, 12, "gap")
	if _, err := repo.ApplyMatchingResult(context.Background(), lifecycleResult(cmd, 2)); !errors.Is(err, ErrResultGap) {
		t.Fatalf("expected gap: %v", err)
	}
	changed := proto.Clone(cmd).(*matchingpb.MatchingCommand)
	changed.GetPlaceLimitOrder().QuantityLots = 11
	if _, err := repo.ApplyMatchingResult(context.Background(), lifecycleResult(changed, 1)); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("expected outbox mismatch: %v", err)
	}
	malformed := lifecycleResult(cmd, 1, execEvent(7, 1, 1, id, id, 1))
	if _, err := repo.ApplyMatchingResult(context.Background(), malformed); !errors.Is(err, ErrResultInvalid) {
		t.Fatalf("expected invalid event identity: %v", err)
	}
	status, _, _, v := lifecycleState(t, db, id)
	if status != "ACCEPTED" || v != 0 {
		t.Fatalf("mutation despite errors: %s %d", status, v)
	}
	requireResult(t, repo, lifecycleResult(cmd, 1), ResultApplied)
}
func TestLifecycleConcurrentReplaySerializes(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	id, cmd := lifecycleOrder(t, db, 7, 1, 12, "race")
	msg := lifecycleResult(cmd, 1)
	const count = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan ApplyOutcome, count)
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			outcome, err := repo.ApplyMatchingResult(ctx, msg)
			results <- outcome
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied, dup := 0, 0
	for r := range results {
		if r == ResultApplied {
			applied++
		} else if r == ResultDuplicate {
			dup++
		}
	}
	if applied != 1 || dup != count-1 {
		t.Fatalf("concurrent results applied=%d duplicate=%d", applied, dup)
	}
	_, _, _, version := lifecycleState(t, db, id)
	if version != 1 {
		t.Fatal("duplicate lifecycle update")
	}
}
func TestLifecycleMigrationBackfillsAcceptedWithoutInventingHistory(t *testing.T) {
	db := newOutboxTestDatabase(t)
	id := committedOutbox(t, db, "pre-migration", 7)
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000004_order_lifecycle.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(context.Background(), string(raw), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	status, _, _, v := lifecycleState(t, db, id)
	if status != "ACCEPTED" || v != 0 {
		t.Fatalf("bad backfill: %s %d", status, v)
	}
}

func TestLifecycleMatcherRejectPreservesAdmission(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	id, cmd := lifecycleOrder(t, db, 7, 1, 12, "matcher-rejected")
	rejected := &matchingpb.SequencedMatchingEvent{
		EventId: &matchingpb.MatchingEventId{MarketId: 7, MarketSequence: 1, EventOrdinal: 0},
		Event: &matchingpb.SequencedMatchingEvent_OrderRejected{OrderRejected: &matchingpb.OrderRejected{
			OrderId: uint64(id), Reason: matchingpb.MatchingOrderRejectionReason(1),
		}},
	}
	requireResult(t, repo, lifecycleResult(cmd, 1, rejected), ResultApplied)
	status, filled, canceled, _ := lifecycleState(t, db, id)
	if status != "REJECTED" || filled != 0 || canceled != 0 {
		t.Fatalf("matcher rejection: %s %d %d", status, filled, canceled)
	}
	var admission string
	if err := db.QueryRow(context.Background(), `SELECT admission_status FROM order_service.orders WHERE order_id=$1`, id).Scan(&admission); err != nil || admission != "ACCEPTED" {
		t.Fatalf("admission was mutated: %s %v", admission, err)
	}
}
func TestLifecycleCrossMarketExecutionFailsClosed(t *testing.T) {
	db := lifecycleDatabase(t)
	repo, _ := NewLifecycleRepository(db)
	maker, mc := lifecycleOrder(t, db, 9, 2, 12, "foreign-maker")
	taker, tc := lifecycleOrder(t, db, 7, 1, 12, "local-taker")
	requireResult(t, repo, lifecycleResult(mc, 1), ResultApplied)
	event := execEvent(7, 1, 0, maker, taker, 12)
	if _, err := repo.ApplyMatchingResult(context.Background(), lifecycleResult(tc, 1, event)); !errors.Is(err, ErrResultConflict) {
		t.Fatalf("expected market mismatch: %v", err)
	}
	status, _, _, _ := lifecycleState(t, db, taker)
	if status != "ACCEPTED" {
		t.Fatalf("cross-market mutation: %s", status)
	}
}

func TestLifecycleDatabaseRejectsSnapshotRewrite(t *testing.T) {
	db := lifecycleDatabase(t)
	id, _ := lifecycleOrder(t, db, 7, 1, 12, "immutable-lots")
	_, err := db.Exec(context.Background(), `UPDATE order_service.order_lifecycle SET total_lots=100,version=version+1,last_market_sequence=1 WHERE order_id=$1`, id)
	assertPgCode(t, err, "23514")
	status, _, _, version := lifecycleState(t, db, id)
	if status != "ACCEPTED" || version != 0 {
		t.Fatalf("mutated immutable lifecycle: %s %d", status, version)
	}
}
