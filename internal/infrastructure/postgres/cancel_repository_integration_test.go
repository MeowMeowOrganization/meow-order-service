//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
	outboxapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
	"github.com/MeowMeowOrganization/meow-order-service/internal/domain/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

func cancellationDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db := lifecycleDatabase(t)
	migration, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "000005_cancellation_workflow.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(context.Background(), string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("apply 000005: %v", err)
	}
	return db
}
func cancelRepos(t *testing.T, db *pgxpool.Pool) (*CancelRepository, *LifecycleRepository) {
	t.Helper()
	c, err := NewCancelRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLifecycleRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	return c, l
}
func queuedCancel(t *testing.T, db *pgxpool.Pool, orderID int64) *matchingpb.MatchingCommand {
	t.Helper()
	var blob []byte
	var typ, key, topic string
	err := db.QueryRow(context.Background(), `SELECT command_type,kafka_key,kafka_topic,payload FROM order_service.matching_outbox
        WHERE order_id=$1 AND command_type='CANCEL_ORDER'`, orderID).Scan(&typ, &key, &topic, &blob)
	if err != nil {
		t.Fatal(err)
	}
	var msg matchingpb.MatchingCommand
	if err := proto.Unmarshal(blob, &msg); err != nil {
		t.Fatal(err)
	}
	if typ != "CANCEL_ORDER" || topic != "matching.commands.v1" || key != "7" || msg.GetCommandId() != fmt.Sprintf("cancel:%d", orderID) ||
		msg.GetMarketId() != 7 || msg.GetCancelOrder() == nil || msg.GetCancelOrder().GetOrderId() != uint64(orderID) {
		t.Fatalf("invalid cancel outbox command: %+v %q %q %q", msg, typ, topic, key)
	}
	return &msg
}
func cancelEvent(orderID int64, seq uint64, qty int64) *matchingpb.SequencedMatchingEvent {
	return &matchingpb.SequencedMatchingEvent{
		EventId: &matchingpb.MatchingEventId{MarketId: 7, MarketSequence: seq, EventOrdinal: 0},
		Event: &matchingpb.SequencedMatchingEvent_OrderCancelled{OrderCancelled: &matchingpb.OrderCancelled{
			OrderId: uint64(orderID), CancelledQuantityLots: qty,
		}},
	}
}
func cancelRejectEvent(orderID int64, seq uint64, reason matchingpb.MatchingCancelRejectionReason) *matchingpb.SequencedMatchingEvent {
	return &matchingpb.SequencedMatchingEvent{
		EventId: &matchingpb.MatchingEventId{MarketId: 7, MarketSequence: seq, EventOrdinal: 0},
		Event: &matchingpb.SequencedMatchingEvent_CancelRejected{CancelRejected: &matchingpb.CancelRejected{
			OrderId: uint64(orderID), Reason: reason,
		}},
	}
}
func cancellationCounts(t *testing.T, db *pgxpool.Pool, orderID int64, wantIntent, wantOutbox, wantRelease int) {
	t.Helper()
	ctx := context.Background()
	var i, o, r int
	err := db.QueryRow(ctx, `SELECT count(*) FROM order_service.cancel_requests WHERE order_id=$1`, orderID).Scan(&i)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(ctx, `SELECT count(*) FROM order_service.matching_outbox WHERE order_id=$1 AND command_type='CANCEL_ORDER'`, orderID).Scan(&o)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(ctx, `SELECT count(*) FROM order_service.reservation_release_intents WHERE order_id=$1`, orderID).Scan(&r)
	if err != nil {
		t.Fatal(err)
	}
	if i != wantIntent || o != wantOutbox || r != wantRelease {
		t.Fatalf("intent=%d outbox=%d release=%d want=%d,%d,%d", i, o, r, wantIntent, wantOutbox, wantRelease)
	}
}
func TestCancelAcceptedBeforePlacementResultAndReplay(t *testing.T) {
	db := cancellationDatabase(t)
	c, l := cancelRepos(t, db)
	id, place := lifecycleOrder(t, db, 7, 1, 12, "cancel-before-placement")
	ctx := context.Background()
	one, err := c.RequestCancellation(ctx, "test-lifecycle-account", id)
	if err != nil || one.Status != app.Pending || one.LifecycleStatus != "ACCEPTED" {
		t.Fatalf("request: %+v %v", one, err)
	}
	again, err := c.RequestCancellation(ctx, "test-lifecycle-account", id)
	if err != nil || again.Status != app.Pending {
		t.Fatalf("duplicate: %+v %v", again, err)
	}
	cancellationCounts(t, db, id, 1, 1, 0)
	cancel := queuedCancel(t, db, id)
	// Even when cancellation is queued before the placement result, the
	// outbox must publish PLACE_LIMIT before CANCEL_ORDER for that market.
	outbox, _ := NewOutboxRepository(db)
	first, err := outbox.Claim(ctx, 1, 30*time.Second)
	if err != nil || len(first) != 1 || first[0].CommandID != place.GetCommandId() {
		t.Fatalf("first claim: %v %v", first, err)
	}
	if err := outbox.MarkSent(ctx, first[0]); err != nil {
		t.Fatal(err)
	}
	second, err := outbox.Claim(ctx, 1, 30*time.Second)
	if err != nil || len(second) != 1 || second[0].CommandID != cancel.GetCommandId() {
		t.Fatalf("second claim: %v %v", second, err)
	}
	requireResult(t, l, lifecycleResult(place, 1), ResultApplied)
	msg := lifecycleResult(cancel, 2, cancelEvent(id, 2, 12))
	requireResult(t, l, msg, ResultApplied)
	requireResult(t, l, msg, ResultDuplicate)
	status, filled, cancelled, _ := lifecycleState(t, db, id)
	if status != "CANCELED" || filled != 0 || cancelled != 12 {
		t.Fatalf("lifecycle: %s %d %d", status, filled, cancelled)
	}
	after, err := c.RequestCancellation(ctx, "test-lifecycle-account", id)
	if err != nil || after.Status != app.Confirmed || after.LifecycleStatus != "CANCELED" {
		t.Fatalf("confirmed: %+v %v", after, err)
	}
	cancellationCounts(t, db, id, 1, 1, 1)
	var rid, op, state string
	err = db.QueryRow(ctx, `SELECT reservation_id,operation_id,state FROM order_service.reservation_release_intents WHERE order_id=$1`, id).Scan(&rid, &op, &state)
	if err != nil || rid != "reservation-cancel-before-placement" || op != "release:"+strconv.FormatInt(id, 10) || state != "AWAITING_SETTLEMENT" {
		t.Fatalf("release intent: rid=%q op=%q state=%q err=%v", rid, op, state, err)
	}
}
func TestCancelWrongAccountAndTerminalCannotEnqueue(t *testing.T) {
	db := cancellationDatabase(t)
	c, l := cancelRepos(t, db)
	id, place := lifecycleOrder(t, db, 7, 2, 5, "terminal")
	if _, err := c.RequestCancellation(context.Background(), "other-account", id); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("foreign order: %v", err)
	}
	if _, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", id+2000); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("unknown order: %v", err)
	}
	requireResult(t, l, lifecycleResult(place, 1), ResultApplied)
	// A valid matcher rejection is a terminal order with zero fills.
	// This test uses a directly applied FILLED order via maker/taker execution.
	takerID, takerPlace := lifecycleOrder(t, db, 7, 1, 5, "terminal-taker")
	requireResult(t, l, lifecycleResult(takerPlace, 2, execEvent(7, 2, 0, id, takerID, 5)), ResultApplied)
	res, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", id)
	if err != nil || res.Status != app.NotCancellable || res.LifecycleStatus != "FILLED" {
		t.Fatalf("terminal: %+v %v", res, err)
	}
	cancellationCounts(t, db, id, 0, 0, 0)
}
func TestCancelPartialFillPreservesExecutionAndRejectsBadQuantity(t *testing.T) {
	db := cancellationDatabase(t)
	c, l := cancelRepos(t, db)
	makerID, maker := lifecycleOrder(t, db, 7, 2, 12, "partial-maker")
	takerID, taker := lifecycleOrder(t, db, 7, 1, 4, "partial-taker")
	requireResult(t, l, lifecycleResult(maker, 1), ResultApplied)
	requireResult(t, l, lifecycleResult(taker, 2, execEvent(7, 2, 0, makerID, takerID, 4)), ResultApplied)
	req, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", makerID)
	if err != nil || req.Status != app.Pending {
		t.Fatalf("request: %+v %v", req, err)
	}
	command := queuedCancel(t, db, makerID)
	bad := lifecycleResult(command, 3, cancelEvent(makerID, 3, 9))
	if _, err := l.ApplyMatchingResult(context.Background(), bad); !errors.Is(err, lifecycle.ErrInvariant) {
		t.Fatalf("should reject bad remaining: %v", err)
	}
	cancellationCounts(t, db, makerID, 1, 1, 0)
	requireResult(t, l, lifecycleResult(command, 3, cancelEvent(makerID, 3, 8)), ResultApplied)
	status, filled, canceled, _ := lifecycleState(t, db, makerID)
	if status != "CANCELED" || filled != 4 || canceled != 8 {
		t.Fatalf("partial cancel %s filled=%d canceled=%d", status, filled, canceled)
	}
	cancellationCounts(t, db, makerID, 1, 1, 1)
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM order_service.private_executions WHERE maker_order_id=$1`, makerID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("execution history count=%d err=%v", n, err)
	}
}
func TestCancelRejectedAfterFillDoesNotRelease(t *testing.T) {
	db := cancellationDatabase(t)
	c, l := cancelRepos(t, db)
	makerID, maker := lifecycleOrder(t, db, 7, 2, 6, "reject-maker")
	takerID, taker := lifecycleOrder(t, db, 7, 1, 6, "reject-taker")
	requireResult(t, l, lifecycleResult(maker, 1), ResultApplied)
	_, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", makerID)
	if err != nil {
		t.Fatal(err)
	}
	cancel := queuedCancel(t, db, makerID)
	requireResult(t, l, lifecycleResult(taker, 2, execEvent(7, 2, 0, makerID, takerID, 6)), ResultApplied)
	reject := lifecycleResult(cancel, 3, cancelRejectEvent(makerID, 3, matchingpb.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_ALREADY_FILLED))
	requireResult(t, l, reject, ResultApplied)
	requireResult(t, l, reject, ResultDuplicate)
	result, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", makerID)
	if err != nil || result.Status != app.Rejected || result.LifecycleStatus != "FILLED" || result.RejectionReason != "ALREADY_FILLED" {
		t.Fatalf("rejection %+v %v", result, err)
	}
	cancellationCounts(t, db, makerID, 1, 1, 0)
}

func TestCancelUnknownOrderAfterMatcherRejectedPlacement(t *testing.T) {
	db := cancellationDatabase(t)
	c, l := cancelRepos(t, db)
	id, place := lifecycleOrder(t, db, 7, 1, 12, "placement-rejected-cancel")
	req, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", id)
	if err != nil || req.Status != app.Pending {
		t.Fatalf("request %+v %v", req, err)
	}
	// The cancel request was queued before the matcher processed place.
	// Committed placement rejection makes UNKNOWN_ORDER an expected outcome.
	placeRej := &matchingpb.SequencedMatchingEvent{
		EventId: &matchingpb.MatchingEventId{MarketId: 7, MarketSequence: 1, EventOrdinal: 0},
		Event: &matchingpb.SequencedMatchingEvent_OrderRejected{OrderRejected: &matchingpb.OrderRejected{
			OrderId: uint64(id), Reason: matchingpb.MatchingOrderRejectionReason_MATCHING_ORDER_REJECTION_REASON_DUPLICATE_ORDER_ID,
		}},
	}
	requireResult(t, l, lifecycleResult(place, 1, placeRej), ResultApplied)
	cancel := queuedCancel(t, db, id)
	msg := lifecycleResult(cancel, 2, cancelRejectEvent(id, 2, matchingpb.MatchingCancelRejectionReason_MATCHING_CANCEL_REJECTION_REASON_UNKNOWN_ORDER))
	requireResult(t, l, msg, ResultApplied)
	requireResult(t, l, msg, ResultDuplicate)
	result, err := c.RequestCancellation(context.Background(), "test-lifecycle-account", id)
	if err != nil || result.Status != app.Rejected || result.LifecycleStatus != "REJECTED" || result.RejectionReason != "UNKNOWN_ORDER" {
		t.Fatalf("outcome %+v %v", result, err)
	}
	cancellationCounts(t, db, id, 1, 1, 0)
}

func TestCancelConcurrentRequestsSingleOutbox(t *testing.T) {
	db := cancellationDatabase(t)
	c, _ := cancelRepos(t, db)
	id, _ := lifecycleOrder(t, db, 7, 1, 12, "concurrent-cancel")
	const workers = 7
	var wg sync.WaitGroup
	begin := make(chan struct{})
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-begin
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			r, err := c.RequestCancellation(ctx, "test-lifecycle-account", id)
			if err == nil && r.Status != app.Pending {
				err = fmt.Errorf("unexpected status %q", r.Status)
			}
			results <- err
		}()
	}
	close(begin)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	cancellationCounts(t, db, id, 1, 1, 0)
}

var _ outboxapp.Repository = (*OutboxRepository)(nil)
