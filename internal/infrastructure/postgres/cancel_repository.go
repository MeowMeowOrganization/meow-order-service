package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

type CancelRepository struct{ db *pgxpool.Pool }

var _ app.Repository = (*CancelRepository)(nil)

func NewCancelRepository(db *pgxpool.Pool) (*CancelRepository, error) {
	if db == nil {
		return nil, errors.New("cancellation requires PostgreSQL")
	}
	return &CancelRepository{db: db}, nil
}

// RequestCancellation atomically persists ONE cancellation intent and ONE
// immutable matching command. The lifecycle row lock serializes requests with
// matching result projection. PENDING means queued, never already canceled.
func (r *CancelRepository) RequestCancellation(ctx context.Context, accountID string, orderID int64) (app.Result, error) {
	if r == nil || r.db == nil || accountID == "" || len(accountID) > 128 || orderID <= 0 {
		return app.Result{}, app.ErrInvalidRequest
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.Result{}, fmt.Errorf("begin cancel request: %w", err)
	}
	defer tx.Rollback(context.Background())

	var admission, lifecycleStatus string
	var market int64
	// Admission identity and accepted fields are immutable; no order-row lock
	// is needed. The lifecycle row is the serialization point.
	err = tx.QueryRow(ctx, `SELECT admission_status,COALESCE(market_numeric_id,0)
        FROM order_service.orders WHERE order_id=$1 AND account_id=$2`, orderID, accountID).
		Scan(&admission, &market)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return app.Result{}, fmt.Errorf("inspect cancellation admission: %w", err)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Result{}, app.ErrNotFound
	}
	if admission != "ACCEPTED" {
		if admission == "PENDING" {
			return app.Result{}, app.ErrAdmissionPending
		}
		return app.Result{OrderID: orderID, Status: app.NotCancellable, LifecycleStatus: "REJECTED"}, nil
	}
	// We lock the lifecycle row under the same transaction as the initial
	// ownership read. It is the only row that matcher projection updates.
	err = tx.QueryRow(ctx, `SELECT l.status FROM order_service.order_lifecycle l
        WHERE l.order_id=$1 FOR UPDATE`, orderID).Scan(&lifecycleStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Result{}, fmt.Errorf("%w: accepted order missing lifecycle", app.ErrProvenance)
	}
	if err != nil {
		return app.Result{}, fmt.Errorf("lock cancellation lifecycle: %w", err)
	}

	var status string
	var reason sql.NullString
	err = tx.QueryRow(ctx, `SELECT status,matcher_rejection_reason FROM order_service.cancel_requests
        WHERE order_id=$1`, orderID).Scan(&status, &reason)
	if err == nil {
		out := app.Result{OrderID: orderID, Status: app.Status(status), LifecycleStatus: lifecycleStatus}
		if reason.Valid {
			out.RejectionReason = reason.String
		}
		if out.Status != app.Pending && out.Status != app.Confirmed && out.Status != app.Rejected {
			return app.Result{}, fmt.Errorf("%w: unexpected cancellation status", app.ErrProvenance)
		}
		if err := tx.Commit(ctx); err != nil {
			return app.Result{}, fmt.Errorf("%w: %w", app.ErrCommitUnknown, err)
		}
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return app.Result{}, fmt.Errorf("read existing cancellation: %w", err)
	}
	switch lifecycleStatus {
	case "FILLED", "CANCELED", "REJECTED":
		if err := tx.Commit(ctx); err != nil {
			return app.Result{}, fmt.Errorf("%w: %w", app.ErrCommitUnknown, err)
		}
		return app.Result{OrderID: orderID, Status: app.NotCancellable, LifecycleStatus: lifecycleStatus}, nil
	case "ACCEPTED", "OPEN", "PARTIALLY_FILLED":
	default:
		return app.Result{}, fmt.Errorf("%w: unknown lifecycle status", app.ErrProvenance)
	}

	var topic, key string
	var placeMarket int64
	err = tx.QueryRow(ctx, `SELECT kafka_topic,kafka_key,market_numeric_id
        FROM order_service.matching_outbox
        WHERE order_id=$1 AND command_type='PLACE_LIMIT'`, orderID).Scan(&topic, &key, &placeMarket)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.Result{}, fmt.Errorf("%w: missing placement outbox", app.ErrProvenance)
	}
	if err != nil {
		return app.Result{}, fmt.Errorf("read placement outbox: %w", err)
	}
	if market <= 0 || market > 4294967295 || market != placeMarket || key != fmt.Sprintf("%d", market) || topic == "" {
		return app.Result{}, fmt.Errorf("%w: inconsistent placement partition", app.ErrProvenance)
	}
	commandID := fmt.Sprintf("cancel:%d", orderID)
	msg := &matchingpb.MatchingCommand{CommandId: commandID, MarketId: uint32(market),
		Command: &matchingpb.MatchingCommand_CancelOrder{CancelOrder: &matchingpb.CancelOrder{OrderId: uint64(orderID)}}}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return app.Result{}, fmt.Errorf("marshal cancellation command: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO order_service.cancel_requests(order_id,command_id) VALUES($1,$2)`, orderID, commandID)
	if err != nil {
		return app.Result{}, fmt.Errorf("persist cancellation intent: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO order_service.matching_outbox
        (order_id,command_id,command_type,market_numeric_id,kafka_topic,kafka_key,payload)
        VALUES($1,$2,'CANCEL_ORDER',$3,$4,$5,$6)`, orderID, commandID, market, topic, key, payload)
	if err != nil {
		return app.Result{}, fmt.Errorf("persist cancellation outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return app.Result{}, fmt.Errorf("%w: %w", app.ErrCommitUnknown, err)
	}
	return app.Result{OrderID: orderID, Status: app.Pending, LifecycleStatus: lifecycleStatus}, nil
}
