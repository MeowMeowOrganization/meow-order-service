package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/lifecycle"
	"github.com/MeowMeowOrganization/meow-order-service/internal/domain/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

var (
	ErrResultGap           = app.ErrResultGap
	ErrResultConflict      = app.ErrResultConflict
	ErrResultInvalid       = app.ErrResultInvalid
	ErrResultCommitUnknown = app.ErrResultCommitUnknown
)

type ApplyOutcome = app.ApplyOutcome

const (
	ResultApplied   ApplyOutcome = app.Applied
	ResultDuplicate ApplyOutcome = app.Duplicate
)

type LifecycleRepository struct{ db *pgxpool.Pool }

func NewLifecycleRepository(db *pgxpool.Pool) (*LifecycleRepository, error) {
	if db == nil {
		return nil, errors.New("nil PostgreSQL pool")
	}
	return &LifecycleRepository{db: db}, nil
}

type parsedResult struct {
	result                 lifecycle.Result
	commandID, commandType string
	fingerprint            [32]byte
	stateHash              []byte
	stateVersion           uint32
	sequence               uint64
}

func parseMatchingResult(msg *matchingpb.MatchingResult) (parsedResult, error) {
	var parsed parsedResult
	if msg == nil || msg.GetCommand() == nil || msg.GetMarketSequence() == 0 || msg.GetCommand().GetMarketId() == 0 ||
		msg.GetCommand().GetCommandId() == "" || len(msg.GetCommand().GetCommandId()) > 128 ||
		msg.GetStateHashAfter() == nil || msg.GetStateHashAfter().GetEncodingVersion() == 0 ||
		len(msg.GetStateHashAfter().GetSha256()) != sha256.Size {
		return parsed, fmt.Errorf("%w: missing command, sequence or valid state hash", ErrResultInvalid)
	}
	cmd := msg.GetCommand()
	parsed.result.Market = cmd.GetMarketId()
	parsed.commandID = cmd.GetCommandId()
	parsed.sequence = msg.GetMarketSequence()
	parsed.stateHash = msg.GetStateHashAfter().GetSha256()
	parsed.stateVersion = msg.GetStateHashAfter().GetEncodingVersion()
	switch {
	case cmd.GetPlaceLimitOrder() != nil:
		place := cmd.GetPlaceLimitOrder()
		if place.GetOrderId() == 0 || place.GetOrderId() > math.MaxInt64 ||
			place.GetPriceTicks() <= 0 || place.GetQuantityLots() <= 0 ||
			(int32(place.GetSide()) != 1 && int32(place.GetSide()) != 2) ||
			parsed.commandID != fmt.Sprintf("place:%d", place.GetOrderId()) {
			return parsed, fmt.Errorf("%w: invalid placement command", ErrResultInvalid)
		}
		parsed.result.Place, parsed.result.OrderID = true, int64(place.GetOrderId())
		parsed.commandType = "PLACE_LIMIT"
	case cmd.GetCancelOrder() != nil:
		cancel := cmd.GetCancelOrder()
		if cancel.GetOrderId() == 0 || cancel.GetOrderId() > math.MaxInt64 ||
			parsed.commandID != fmt.Sprintf("cancel:%d", cancel.GetOrderId()) {
			return parsed, fmt.Errorf("%w: invalid cancellation command", ErrResultInvalid)
		}
		parsed.result.OrderID = int64(cancel.GetOrderId())
		parsed.commandType = "CANCEL_ORDER"
	default:
		return parsed, fmt.Errorf("%w: unknown command", ErrResultInvalid)
	}
	for i, ev := range msg.GetEvents() {
		if ev == nil || ev.GetEventId() == nil || ev.GetEventId().GetMarketId() != parsed.result.Market ||
			ev.GetEventId().GetMarketSequence() != parsed.sequence || ev.GetEventId().GetEventOrdinal() != uint64(i) {
			return parsed, fmt.Errorf("%w: mismatched event identity at ordinal %d", ErrResultInvalid, i)
		}
		event := lifecycle.Event{Ordinal: uint64(i)}
		switch {
		case ev.GetExectuion() != nil: // upstream proto deliberately spells this 'exectuion'
			e := ev.GetExectuion()
			if e.GetMakerOrderId() == 0 || e.GetTakerOrderId() == 0 || e.GetMakerOrderId() > math.MaxInt64 || e.GetTakerOrderId() > math.MaxInt64 {
				return parsed, fmt.Errorf("%w: execution order ID out of range", ErrResultInvalid)
			}
			event.Kind = lifecycle.Execution
			event.MakerID, event.TakerID = int64(e.GetMakerOrderId()), int64(e.GetTakerOrderId())
			event.PriceTicks, event.QuantityLots, event.TakerSide = e.GetPriceTicks(), e.GetQuantityLots(), int32(e.GetTakerSide())
		case ev.GetOrderRejected() != nil:
			e := ev.GetOrderRejected()
			if e.GetOrderId() == 0 || e.GetOrderId() > math.MaxInt64 {
				return parsed, fmt.Errorf("%w: invalid rejected order", ErrResultInvalid)
			}
			event.Kind, event.OrderID = lifecycle.OrderRejected, int64(e.GetOrderId())
			switch int32(e.GetReason()) {
			case 1:
				event.Reason = "DUPLICATE_ORDER_ID"
			case 2:
				event.Reason = "RESTING_AGGREGATE_QUANTITY_OUT_OF_RANGE"
			default:
				return parsed, fmt.Errorf("%w: unknown placement rejection", ErrResultInvalid)
			}
		case ev.GetOrderCancelled() != nil:
			e := ev.GetOrderCancelled()
			if e.GetOrderId() == 0 || e.GetOrderId() > math.MaxInt64 {
				return parsed, fmt.Errorf("%w: invalid canceled order", ErrResultInvalid)
			}
			event.Kind, event.OrderID, event.QuantityLots = lifecycle.OrderCancelled, int64(e.GetOrderId()), e.GetCancelledQuantityLots()
		case ev.GetCancelRejected() != nil:
			e := ev.GetCancelRejected()
			if e.GetOrderId() == 0 || e.GetOrderId() > math.MaxInt64 {
				return parsed, fmt.Errorf("%w: invalid cancel rejection", ErrResultInvalid)
			}
			event.Kind, event.OrderID = lifecycle.CancelRejected, int64(e.GetOrderId())
			switch int32(e.GetReason()) {
			case 1:
				event.Reason = "UNKNOWN_ORDER"
			case 2:
				event.Reason = "ALREADY_FILLED"
			case 3:
				event.Reason = "ALREADY_CANCELLED"
			case 4:
				event.Reason = "ALREADY_REJECTED"
			default:
				return parsed, fmt.Errorf("%w: unknown cancel rejection", ErrResultInvalid)
			}
		default:
			return parsed, fmt.Errorf("%w: unrecognized matching event", ErrResultInvalid)
		}
		parsed.result.Events = append(parsed.result.Events, event)
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return parsed, fmt.Errorf("%w: serialize result: %v", ErrResultInvalid, err)
	}
	parsed.fingerprint = sha256.Sum256(b)
	return parsed, nil
}

// ApplyMatchingResult checks command provenance, locks the per-market cursor,
// projects ALL events, then commits lifecycle, executions and journal together.
// The caller must acknowledge Kafka ONLY on APPLIED or DUPLICATE.
func (r *LifecycleRepository) ApplyMatchingResult(ctx context.Context, msg *matchingpb.MatchingResult) (ApplyOutcome, error) {
	var none ApplyOutcome
	parsed, err := parseMatchingResult(msg)
	if err != nil {
		return none, err
	}
	seq := strconv.FormatUint(parsed.sequence, 10) // PostgreSQL NUMERIC(20,0): exact uint64
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return none, fmt.Errorf("begin lifecycle transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO order_service.market_result_cursors(market_numeric_id)
        VALUES($1) ON CONFLICT DO NOTHING`, int64(parsed.result.Market))
	if err != nil {
		return none, fmt.Errorf("initialize market cursor: %w", err)
	}
	var lastText string
	err = tx.QueryRow(ctx, `SELECT last_sequence::text FROM order_service.market_result_cursors
        WHERE market_numeric_id=$1 FOR UPDATE`, int64(parsed.result.Market)).Scan(&lastText)
	if err != nil {
		return none, fmt.Errorf("lock market cursor: %w", err)
	}
	last, err := strconv.ParseUint(lastText, 10, 64)
	if err != nil {
		return none, fmt.Errorf("corrupt market cursor: %w", err)
	}
	if parsed.sequence <= last {
		var oldDigest []byte
		err = tx.QueryRow(ctx, `SELECT payload_sha256 FROM order_service.applied_matching_results
            WHERE market_numeric_id=$1 AND market_sequence=$2::numeric`, int64(parsed.result.Market), seq).Scan(&oldDigest)
		if errors.Is(err, pgx.ErrNoRows) {
			return none, fmt.Errorf("%w: cursor ahead of journal", ErrResultConflict)
		}
		if err != nil {
			return none, err
		}
		if !bytes.Equal(oldDigest, parsed.fingerprint[:]) {
			return none, fmt.Errorf("%w: conflicting duplicate result", ErrResultConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return none, fmt.Errorf("%w: %w", ErrResultCommitUnknown, err)
		}
		return ResultDuplicate, nil
	}
	if last == math.MaxUint64 || parsed.sequence != last+1 {
		return none, fmt.Errorf("%w: market=%d last=%d incoming=%d", ErrResultGap, parsed.result.Market, last, parsed.sequence)
	}
	var storedPayload []byte
	var storedType string
	err = tx.QueryRow(ctx, `SELECT payload, command_type FROM order_service.matching_outbox
        WHERE command_id=$1 AND market_numeric_id=$2 AND order_id=$3`, parsed.commandID, int64(parsed.result.Market), parsed.result.OrderID).
		Scan(&storedPayload, &storedType)
	if errors.Is(err, pgx.ErrNoRows) {
		return none, fmt.Errorf("%w: committed command not found in outbox", ErrResultConflict)
	}
	if err != nil {
		return none, fmt.Errorf("read immutable command provenance: %w", err)
	}
	var original matchingpb.MatchingCommand
	if storedType != parsed.commandType || proto.Unmarshal(storedPayload, &original) != nil || !proto.Equal(&original, msg.GetCommand()) {
		return none, fmt.Errorf("%w: matching command differs from frozen outbox", ErrResultConflict)
	}
	// Unique command_id is guaranteed by the journal; detect reused IDs
	// explicitly instead of relying on a unique-constraint error at commit.
	var oldSeq string
	err = tx.QueryRow(ctx, `SELECT market_sequence::text FROM order_service.applied_matching_results
        WHERE command_id=$1`, parsed.commandID).Scan(&oldSeq)
	if err == nil {
		return none, fmt.Errorf("%w: command %q already committed at sequence %s", ErrResultConflict, parsed.commandID, oldSeq)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return none, err
	}
	ids := map[int64]struct{}{parsed.result.OrderID: {}}
	for _, e := range parsed.result.Events {
		switch e.Kind {
		case lifecycle.Execution:
			ids[e.MakerID] = struct{}{}
			ids[e.TakerID] = struct{}{}
		case lifecycle.OrderRejected, lifecycle.OrderCancelled, lifecycle.CancelRejected:
			ids[e.OrderID] = struct{}{}
		}
	}
	ordered := make([]int64, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	// Ascending row-lock order prevents cycles with other lifecycle actors.
	for i := 1; i < len(ordered); i++ {
		for j := i; j > 0 && ordered[j] < ordered[j-1]; j-- {
			ordered[j], ordered[j-1] = ordered[j-1], ordered[j]
		}
	}
	known := make(map[int64]lifecycle.Order, len(ordered))
	for _, id := range ordered {
		var o lifecycle.Order
		var status string
		var market int64
		var side int16
		var admission string
		var reason sql.NullString
		var totalFromAdmission int64
		err = tx.QueryRow(ctx, `SELECT o.market_numeric_id,o.side,o.price_ticks,o.quantity_lots,o.admission_status,
            o.price_tick_units::text,o.quantity_step_base_atoms::text,o.base_scale,o.quote_scale,o.price_scale,
            l.total_lots,l.status,l.filled_lots,l.cancelled_lots,l.matcher_rejection_reason
            FROM order_service.orders o JOIN order_service.order_lifecycle l ON l.order_id=o.order_id
            WHERE o.order_id=$1 FOR UPDATE OF l`, id).Scan(&market, &side, &o.PriceTicks, &totalFromAdmission, &admission,
			&o.PriceTickUnits, &o.QuantityStepAtoms, &o.BaseScale, &o.QuoteScale, &o.PriceScale,
			&o.TotalLots, &status, &o.FilledLots, &o.CancelledLots, &reason)
		if errors.Is(err, pgx.ErrNoRows) {
			return none, fmt.Errorf("%w: missing accepted order lifecycle %d", ErrResultConflict, id)
		}
		if err != nil {
			return none, fmt.Errorf("lock order %d: %w", id, err)
		}
		if admission != "ACCEPTED" || market != int64(parsed.result.Market) || o.TotalLots != totalFromAdmission {
			return none, fmt.Errorf("%w: order admission/market mismatch %d", ErrResultConflict, id)
		}
		o.ID, o.Market, o.Side, o.Status = id, uint32(market), int32(side), lifecycle.Status(status)
		if reason.Valid {
			o.RejectionReason = reason.String
		}
		known[id] = o
	}
	if parsed.result.Place {
		place := msg.GetCommand().GetPlaceLimitOrder()
		order := known[parsed.result.OrderID]
		if order.Side != int32(place.GetSide()) || order.PriceTicks != place.GetPriceTicks() || order.TotalLots != place.GetQuantityLots() {
			return none, fmt.Errorf("%w: placement contradicts admission snapshot", ErrResultConflict)
		}
	}
	projected, err := lifecycle.Apply(parsed.result, known)
	if err != nil {
		return none, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO order_service.applied_matching_results
        (market_numeric_id,market_sequence,command_id,command_type,payload_sha256,hash_encoding_version,state_sha256)
        VALUES($1,$2::numeric,$3,$4,$5,$6,$7)`, int64(parsed.result.Market), seq, parsed.commandID, parsed.commandType, parsed.fingerprint[:], int64(parsed.stateVersion), parsed.stateHash)
	if err != nil {
		return none, fmt.Errorf("record applied result: %w", err)
	}
	for _, o := range projected.Orders {
		tag, err := tx.Exec(ctx, `UPDATE order_service.order_lifecycle SET
            status=$2::text,filled_lots=$3,cancelled_lots=$4,matcher_rejection_reason=NULLIF($5::text,''),
            last_market_sequence=$6::numeric,version=version+1,updated_at=clock_timestamp(),
            opened_at=CASE WHEN $2::text IN ('OPEN','PARTIALLY_FILLED') AND opened_at IS NULL THEN clock_timestamp() ELSE opened_at END,
            terminal_at=CASE WHEN $2::text IN ('FILLED','CANCELED','REJECTED') AND terminal_at IS NULL THEN clock_timestamp() ELSE terminal_at END
            WHERE order_id=$1 AND last_market_sequence<$6::numeric`, o.ID, string(o.Status), o.FilledLots, o.CancelledLots, o.RejectionReason, seq)
		if err != nil {
			return none, fmt.Errorf("update lifecycle %d: %w", o.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return none, fmt.Errorf("%w: stale lifecycle sequence for %d", ErrResultConflict, o.ID)
		}
	}
	// A cancellation command has exactly one committed outcome. Persist it in
	// the SAME transaction as lifecycle changes and market-sequence advance.
	// The intent must exist: never allow an unsolicited matcher cancellation
	// to create a synthetic client request or bypass local provenance.
	if parsed.commandType == "CANCEL_ORDER" {
		var intentStatus string
		err = tx.QueryRow(ctx, `SELECT status FROM order_service.cancel_requests
            WHERE order_id=$1 AND command_id=$2 FOR UPDATE`, parsed.result.OrderID, parsed.commandID).Scan(&intentStatus)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && intentStatus != "PENDING") {
			return none, fmt.Errorf("%w: cancellation intent missing or already completed", ErrResultConflict)
		}
		if err != nil {
			return none, fmt.Errorf("read cancellation intent: %w", err)
		}
		// Pure projector verifies that there is exactly one outcome for a
		// cancellation and that the reason agrees with the order state.
		outcome := parsed.result.Events[0]
		var status, reason string
		switch outcome.Kind {
		case lifecycle.OrderCancelled:
			status = "CONFIRMED"
		case lifecycle.CancelRejected:
			status, reason = "REJECTED", outcome.Reason
		default:
			return none, fmt.Errorf("%w: invalid cancellation outcome", ErrResultInvalid)
		}
		tag, updateErr := tx.Exec(ctx, `UPDATE order_service.cancel_requests
            SET status=$2::text,matcher_rejection_reason=NULLIF($3::text,''),
                completed_market_sequence=$4::numeric,completed_at=clock_timestamp()
            WHERE order_id=$1 AND status='PENDING'`, parsed.result.OrderID, status, reason, seq)
		if updateErr != nil {
			return none, fmt.Errorf("commit cancellation outcome: %w", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return none, fmt.Errorf("%w: cancellation intent was not pending", ErrResultConflict)
		}
		if status == "CONFIRMED" {
			// A confirmed cancellation creates a durable, *unexecuted* release
			// obligation. The separate Accounting integration must prove all
			// earlier executions settled before calling ReleaseReservation.
			tag, updateErr = tx.Exec(ctx, `INSERT INTO order_service.reservation_release_intents
                (order_id,reservation_id,operation_id)
                SELECT order_id,reservation_id,'release:' || order_id::text
                FROM order_service.orders
                WHERE order_id=$1 AND admission_status='ACCEPTED' AND reservation_id IS NOT NULL`, parsed.result.OrderID)
			if updateErr != nil {
				return none, fmt.Errorf("record settlement-fenced release intent: %w", updateErr)
			}
			if tag.RowsAffected() != 1 {
				return none, fmt.Errorf("%w: reservation provenance missing for canceled order", ErrResultConflict)
			}
		}
	}
	for _, trade := range projected.Trades {
		_, err = tx.Exec(ctx, `INSERT INTO order_service.private_executions
            (market_numeric_id,market_sequence,event_ordinal,maker_order_id,taker_order_id,price_ticks,quantity_lots,taker_side)
            VALUES($1,$2::numeric,$3::numeric,$4,$5,$6,$7,$8)`, int64(parsed.result.Market), seq,
			strconv.FormatUint(trade.Ordinal, 10), trade.MakerID, trade.TakerID, trade.PriceTicks, trade.QuantityLots, trade.TakerSide)
		if err != nil {
			return none, fmt.Errorf("insert private execution: %w", err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE order_service.market_result_cursors SET last_sequence=$2::numeric,
        last_state_hash=$3,last_hash_version=$4,updated_at=clock_timestamp() WHERE market_numeric_id=$1`,
		int64(parsed.result.Market), seq, parsed.stateHash, int64(parsed.stateVersion))
	if err != nil {
		return none, fmt.Errorf("advance contiguous cursor: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return none, fmt.Errorf("%w: %w", ErrResultCommitUnknown, err)
	}
	return ResultApplied, nil
}
