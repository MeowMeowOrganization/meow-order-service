//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	query "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func privateQueryDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := cancellationDatabase(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000006_private_queries.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), string(data), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply 000006: %v", err)
	}
	return db
}
func queryServices(t *testing.T, db *pgxpool.Pool) (*query.Service, *LifecycleRepository, *CancelRepository) {
	t.Helper()
	repo, err := NewPrivateQueryRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := query.New(repo)
	if err != nil {
		t.Fatal(err)
	}
	lc, err := NewLifecycleRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	cancel, err := NewCancelRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return svc, lc, cancel
}
func otherAccountOrder(t *testing.T, db *pgxpool.Pool, name string, lots int64, side int32) (int64, *matchingpb.MatchingCommand) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// Reuse the complete frozen admission snapshot; change only account ownership.
	sql := strings.Replace(lifecycleFixtureSQL, "'test-lifecycle-account'", "'other-account'", 1)
	qty := strings.TrimRight(fmt.Sprintf("0.%03d", lots), "0")
	baseAtoms, notional := lots*100000, lots*60000100
	asset, reserved := "USDT", notional
	if side == 2 {
		asset, reserved = "BTC", baseAtoms
	}
	var id int64
	if err = tx.QueryRow(ctx, sql, name, "BTC-USDT", side, qty, 7, "BTC", baseAtoms, notional, lots, asset, reserved).Scan(&id); err != nil {
		t.Fatal(err)
	}
	cmd := &matchingpb.MatchingCommand{CommandId: fmt.Sprintf("place:%d", id), MarketId: 7,
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
	if _, err = tx.Exec(ctx, insertOutboxFixtureCommandSQL, id, cmd.GetCommandId(), int64(7), "7", encoded); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return id, cmd
}

func TestPrivateQueryAdmissionAndOpenOrders(t *testing.T) {
	db := privateQueryDB(t)
	svc, lc, _ := queryServices(t, db)
	ctx := context.Background()
	openID, place := lifecycleOrder(t, db, 7, 2, 12, "query-open")
	// An admitted order is not yet a resting order until the matcher commits it.
	before, err := svc.GetOrder(ctx, "test-lifecycle-account", strconv.FormatInt(openID, 10))
	if err != nil || before.Status != "ACCEPTED" || before.QuantityBaseAtoms != "1200000" || before.FilledBaseAtoms != "0" {
		t.Fatalf("accepted order %+v err=%v", before, err)
	}
	opens, err := svc.ListOrders(ctx, "test-lifecycle-account", "", 10, "", true)
	if err != nil || len(opens.Items) != 0 {
		t.Fatalf("pre-placement opens: %+v %v", opens, err)
	}
	// Admission PENDING remains distinguishable from matcher ACCEPTED.
	var pendingID int64
	if err = db.QueryRow(ctx, lifecycleFixtureSQL, "query-pending", "BTC-USDT", int32(1), "0.003", uint32(7), "BTC", int64(300000), int64(180000300), int64(3), "USDT", int64(180000300)).Scan(&pendingID); err != nil {
		t.Fatal(err)
	}
	pending, err := svc.GetOrder(ctx, "test-lifecycle-account", strconv.FormatInt(pendingID, 10))
	if err != nil || pending.Status != "ADMISSION_PENDING" || pending.MarketSequence != "" {
		t.Fatalf("pending %+v %v", pending, err)
	}
	requireResult(t, lc, lifecycleResult(place, 1), ResultApplied)
	opens, err = svc.ListOrders(ctx, "test-lifecycle-account", "BTC-USDT", 10, "", true)
	if err != nil || len(opens.Items) != 1 || opens.Items[0].ID != openID || opens.Items[0].Status != "OPEN" {
		t.Fatalf("opens %+v %v", opens, err)
	}
	if _, err = svc.GetOrder(ctx, "other-account", strconv.FormatInt(openID, 10)); !errors.Is(err, query.ErrNotFound) {
		t.Fatalf("foreign get %v", err)
	}
	foreign, err := svc.ListOrders(ctx, "other-account", "", 10, "", false)
	if err != nil || len(foreign.Items) != 0 {
		t.Fatalf("foreign history %+v %v", foreign, err)
	}
	// Keyset history includes admission pending and accepted; never silently
	// substitutes an offset-based page when records share a timestamp.
	history, err := svc.ListOrders(ctx, "test-lifecycle-account", "", 1, "", false)
	if err != nil || len(history.Items) != 1 || history.Next == "" {
		t.Fatalf("history page 1 %+v %v", history, err)
	}
	after, err := svc.ListOrders(ctx, "test-lifecycle-account", "", 1, history.Next, false)
	if err != nil || len(after.Items) != 1 || after.Items[0].ID == history.Items[0].ID || after.Next != "" {
		t.Fatalf("history page 2 %+v %v", after, err)
	}
	if _, err = svc.ListOrders(ctx, "other-account", "", 1, history.Next, false); !errors.Is(err, query.ErrInvalid) {
		t.Fatalf("cross account cursor %v", err)
	}
	if _, err = svc.ListOrders(ctx, "test-lifecycle-account", "", 1, history.Next, true); !errors.Is(err, query.ErrInvalid) {
		t.Fatalf("history/open cursor %v", err)
	}
}

func TestPrivateQueryTradeAndCancelReplay(t *testing.T) {
	db := privateQueryDB(t)
	svc, lc, cancels := queryServices(t, db)
	ctx := context.Background()
	maker, makerCmd := lifecycleOrder(t, db, 7, 2, 12, "private-maker")
	taker, takerCmd := lifecycleOrder(t, db, 7, 1, 4, "private-taker")
	requireResult(t, lc, lifecycleResult(makerCmd, 1), ResultApplied)
	second := lifecycleResult(takerCmd, 2, execEvent(7, 2, 0, maker, taker, 4))
	requireResult(t, lc, second, ResultApplied)
	requireResult(t, lc, second, ResultDuplicate)
	other, otherCmd := otherAccountOrder(t, db, "private-foreign", 2, 1)
	third := lifecycleResult(otherCmd, 3, execEvent(7, 3, 0, maker, other, 2))
	requireResult(t, lc, third, ResultApplied)
	requireResult(t, lc, third, ResultDuplicate)
	// 2 executions produce 3 private legs for this account: maker twice,
	// taker once. The other account's taker leg is never visible here.
	trades, err := svc.ListTrades(ctx, "test-lifecycle-account", "", "", 10, "")
	if err != nil || len(trades.Items) != 3 {
		t.Fatalf("private account trades %+v %v", trades, err)
	}
	for _, tr := range trades.Items {
		if tr.OrderID == other || tr.PriceUnits != "6000010" || tr.QuantityBaseAtoms == "" || tr.NumericMarketID != 7 {
			t.Fatalf("private trade provenance %+v", tr)
		}
	}
	otherTr, err := svc.ListTrades(ctx, "other-account", "", "", 10, "")
	if err != nil || len(otherTr.Items) != 1 || otherTr.Items[0].OrderID != other || otherTr.Items[0].Role != "TAKER" {
		t.Fatalf("foreign trade %+v %v", otherTr, err)
	}
	makerTr, err := svc.ListTrades(ctx, "test-lifecycle-account", "", strconv.FormatInt(maker, 10), 10, "")
	if err != nil || len(makerTr.Items) != 2 || makerTr.Items[0].Role != "MAKER" {
		t.Fatalf("maker trades %+v %v", makerTr, err)
	}
	if _, err = svc.ListTrades(ctx, "test-lifecycle-account", "", strconv.FormatInt(other, 10), 10, ""); !errors.Is(err, query.ErrNotFound) {
		t.Fatalf("foreign trade lookup %v", err)
	}
	one, err := svc.ListTrades(ctx, "test-lifecycle-account", "", "", 1, "")
	if err != nil || len(one.Items) != 1 || one.Next == "" {
		t.Fatalf("first trade page %+v %v", one, err)
	}
	two, err := svc.ListTrades(ctx, "test-lifecycle-account", "", "", 1, one.Next)
	if err != nil || len(two.Items) != 1 || two.Items[0].OrderID == one.Items[0].OrderID && two.Items[0].MarketSequence == one.Items[0].MarketSequence && two.Items[0].Role == one.Items[0].Role {
		t.Fatalf("second trade page %+v %v", two, err)
	}
	three, err := svc.ListTrades(ctx, "test-lifecycle-account", "", "", 1, two.Next)
	if err != nil || len(three.Items) != 1 || three.Next != "" {
		t.Fatalf("third trade page %+v %v", three, err)
	}
	if _, err = svc.ListTrades(ctx, "other-account", "", "", 1, one.Next); !errors.Is(err, query.ErrInvalid) {
		t.Fatalf("cross-account trade cursor %v", err)
	}
	// CEX 19.4 pending -> confirmed status, with remaining lots cancelled.
	if _, err = cancels.RequestCancellation(ctx, "test-lifecycle-account", maker); err != nil {
		t.Fatal(err)
	}
	pending, err := svc.GetOrder(ctx, "test-lifecycle-account", strconv.FormatInt(maker, 10))
	if err != nil || pending.CancelStatus != "PENDING" || pending.Status != "PARTIALLY_FILLED" {
		t.Fatalf("pending cancellation %+v %v", pending, err)
	}
	cancelCommand := queuedCancel(t, db, maker)
	cancelResult := lifecycleResult(cancelCommand, 4, cancelEvent(maker, 4, 6))
	requireResult(t, lc, cancelResult, ResultApplied)
	requireResult(t, lc, cancelResult, ResultDuplicate)
	done, err := svc.GetOrder(ctx, "test-lifecycle-account", strconv.FormatInt(maker, 10))
	if err != nil || done.Status != "CANCELED" || done.CancelStatus != "CONFIRMED" ||
		done.FilledLots != 6 || done.CancelledLots != 6 || done.FilledBaseAtoms != "600000" || done.CancelledBaseAtoms != "600000" || done.RemainingBaseAtoms != "0" {
		t.Fatalf("confirmed cancellation %+v %v", done, err)
	}
	opens, err := svc.ListOrders(ctx, "test-lifecycle-account", "", 10, "", true)
	if err != nil || len(opens.Items) != 0 {
		t.Fatalf("terminal order leaked into opens %+v %v", opens, err)
	}
	same, err := svc.ListTrades(ctx, "test-lifecycle-account", "", "", 10, "")
	if err != nil || len(same.Items) != 3 {
		t.Fatalf("replay duplicated trade legs %+v %v", same, err)
	}
}

func TestPrivateQueryPostgresKeysetTimestampTie(t *testing.T) {
	db := privateQueryDB(t)
	svc, _, _ := queryServices(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var a, b int64
	insert := func(name string, id *int64) {
		t.Helper()
		if err := tx.QueryRow(ctx, lifecycleFixtureSQL, name, "BTC-USDT", int32(1), "0.003", uint32(7), "BTC",
			int64(300000), int64(180000300), int64(3), "USDT", int64(180000300)).Scan(id); err != nil {
			t.Fatal(err)
		}
	}
	insert("same-timestamp-a", &a)
	insert("same-timestamp-b", &b)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var same bool
	if err := db.QueryRow(ctx, `SELECT x.created_at=y.created_at FROM order_service.orders x,order_service.orders y
        WHERE x.order_id=$1 AND y.order_id=$2`, a, b).Scan(&same); err != nil || !same {
		t.Fatalf("timestamp tie: %v %v", same, err)
	}
	first, err := svc.ListOrders(ctx, "test-lifecycle-account", "BTC-USDT", 1, "", false)
	if err != nil || len(first.Items) != 1 || first.Items[0].ID != b || first.Next == "" {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := svc.ListOrders(ctx, "test-lifecycle-account", "BTC-USDT", 1, first.Next, false)
	if err != nil || len(second.Items) != 1 || second.Items[0].ID != a || second.Next != "" {
		t.Fatalf("second: %+v %v", second, err)
	}
}

func TestPrivateQueryRejectedAdmissionIsNotMatcherRejection(t *testing.T) {
	db := privateQueryDB(t)
	svc, _, _ := queryServices(t, db)
	ctx := context.Background()
	var id int64
	if err := db.QueryRow(ctx, lifecycleFixtureSQL, "query-admission-rejected", "BTC-USDT", int32(1), "0.003", uint32(7), "BTC",
		int64(300000), int64(180000300), int64(3), "USDT", int64(180000300)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec(ctx, `UPDATE order_service.orders SET admission_status='REJECTED', rejection_code='MARKET_HALTED',
        rejected_at=clock_timestamp() WHERE order_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetOrder(ctx, "test-lifecycle-account", strconv.FormatInt(id, 10))
	if err != nil || got.Status != "ADMISSION_REJECTED" || got.AdmissionRejection != "MARKET_HALTED" ||
		got.MatcherRejection != "" || got.MarketSequence != "" || got.TotalLots != 0 {
		t.Fatalf("rejected admission: %+v %v", got, err)
	}
	opens, err := svc.ListOrders(ctx, "test-lifecycle-account", "", 10, "", true)
	if err != nil || len(opens.Items) != 0 {
		t.Fatalf("rejected order in open orders: %+v %v", opens, err)
	}
}
