package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	orderpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/order/v1alpha1"
	"google.golang.org/protobuf/proto"
	"strconv"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	minAdmissionLease = time.Second
	maxAdmissionLease = 5 * time.Minute
)

// AdmissionRepository implements TX A and TX B. Neither Accounting nor Kafka is
// reachable from this package. Only pure Protobuf encoding happens inside TX B.
type AdmissionRepository struct {
	db            *pgxpool.Pool
	planner       app.ReservationPlanner
	leaseTTL      time.Duration
	matchingTopic string
}

var _ app.Repository = (*AdmissionRepository)(nil)

const DefaultMatchingCommandTopic = "matching.commands.v1"

func NewAdmissionRepository(db *pgxpool.Pool, planner app.ReservationPlanner, leaseTTL time.Duration) (*AdmissionRepository, error) {
	return NewAdmissionRepositoryWithTopic(db, planner, leaseTTL, DefaultMatchingCommandTopic)
}

// NewAdmissionRepositoryWithTopic freezes the intended Kafka topic in the
// immutable outbox row. No network producer is involved in this milestone.
func NewAdmissionRepositoryWithTopic(db *pgxpool.Pool, planner app.ReservationPlanner, leaseTTL time.Duration, topic string) (*AdmissionRepository, error) {
	if db == nil || planner == nil || leaseTTL < minAdmissionLease ||
		leaseTTL > maxAdmissionLease || leaseTTL%time.Millisecond != 0 ||
		!validMatchingTopic(topic) {
		return nil, errors.New("admission repository requires a database, a trusted planner, and a 1s..5m whole-millisecond lease")
	}
	return &AdmissionRepository{db: db, planner: planner, leaseTTL: leaseTTL, matchingTopic: topic}, nil
}

func validMatchingTopic(topic string) bool {
	if len(topic) == 0 || len(topic) > 128 || topic == "." || topic == ".." {
		return false
	}
	for _, b := range []byte(topic) {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' ||
			b >= '0' && b <= '9' || b == '.' || b == '_' || b == '-') {
			return false
		}
	}
	return true
}

func (r *AdmissionRepository) Claim(ctx context.Context, accountID string, submission domain.LimitSubmission) (app.ClaimResult, error) {
	if err := domain.ValidateAccountID(accountID); err != nil {
		return app.ClaimResult{}, err
	}
	normalized, err := domain.NormalizeSubmission(submission)
	if err != nil {
		// Invalid syntax has no canonical identity and is not persisted.
		return app.ClaimResult{}, err
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.ClaimResult{}, fmt.Errorf("begin admission: %w", err)
	}
	defer tx.Rollback(context.Background())

	// An existing idempotency identity wins even if current market rules have
	// been modified or the market has subsequently been halted/removed.
	existing, found, err := readAdmission(ctx, tx, accountID, normalized.ClientOrderID)
	if err != nil {
		return app.ClaimResult{}, err
	}
	if found {
		if existing.Submission.Fingerprint != normalized.Fingerprint {
			return app.ClaimResult{}, app.ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return app.ClaimResult{}, fmt.Errorf("commit duplicate lookup: %w", err)
		}
		return app.ClaimResult{Record: existing}, nil
	}

	rules, present, err := readMarketRules(ctx, tx, normalized.MarketID)
	if err != nil {
		return app.ClaimResult{}, err
	}
	var (
		code      string
		canonical domain.CanonicalLimit
		plan      app.ReservationPlan
	)
	if !present {
		code = "MARKET_NOT_FOUND"
	} else {
		// Invalid catalog configuration is an operational failure, NOT a
		// permanent client-order rejection.
		if err = domain.ValidateMarketRules(rules); err != nil {
			return app.ClaimResult{}, fmt.Errorf("unsafe market configuration: %w", err)
		}
		canonical, err = domain.CanonicalizeLimit(normalized, rules)
		if err != nil {
			var permanent bool
			code, permanent = classifyAdmissionRejection(err)
			if !permanent {
				return app.ClaimResult{}, fmt.Errorf("canonicalize admission: %w", err)
			}
		} else {
			// This is a pure in-process policy calculation; no Accounting
			// calls, HTTP, caches, or fee-policy RPCs inside the transaction.
			plan, err = r.planner.PlanReservation(accountID, canonical)
			if err != nil {
				return app.ClaimResult{}, fmt.Errorf("reservation policy unavailable: %w", err)
			}
			if err := app.ValidateReservationPlan(canonical, plan); err != nil {
				return app.ClaimResult{}, err
			}
		}
	}

	var id int64
	var lease *app.Lease
	if code != "" {
		id, err = insertRejected(ctx, tx, accountID, normalized, code)
	} else {
		var owner string
		owner, err = newLeaseOwner()
		if err != nil {
			return app.ClaimResult{}, err
		}
		var expiry time.Time
		id, expiry, err = insertPendingClaim(ctx, tx, accountID, canonical, plan, owner, r.leaseTTL)
		if err == nil && id > 0 {
			lease = &app.Lease{Owner: owner, Generation: 1, Until: expiry}
		}
	}
	if err != nil {
		return app.ClaimResult{}, err
	}

	// INSERT ON CONFLICT DO NOTHING waits for a concurrent uncommitted winner.
	// At READ COMMITTED this *new statement* observes the winner's committed
	// row. No unique-violation leaks to the caller and no second order exists.
	record, found, err := readAdmission(ctx, tx, accountID, normalized.ClientOrderID)
	if err != nil {
		return app.ClaimResult{}, err
	}
	if !found {
		return app.ClaimResult{}, errors.New("idempotency winner not visible after insert")
	}
	if record.Submission.Fingerprint != normalized.Fingerprint {
		return app.ClaimResult{}, app.ErrIdempotencyConflict
	}
	if err := tx.Commit(ctx); err != nil {
		// Outcome can be ambiguous if the COMMIT acknowledgement was lost.
		// Retry the SAME (account_id, client_order_id); never create a new key.
		return app.ClaimResult{}, fmt.Errorf("commit admission (retry original client_order_id if outcome unknown): %w", err)
	}
	created := id != 0
	if !created {
		lease = nil // loser must never receive a reservation-capable lease
	}
	return app.ClaimResult{Created: created, Record: record, Lease: lease}, nil
}

func classifyAdmissionRejection(err error) (string, bool) {
	switch {
	case errors.Is(err, domain.ErrMarketNotActive):
		return "MARKET_HALTED", true
	case errors.Is(err, domain.ErrInvalidPrice):
		return "INVALID_PRICE", true
	case errors.Is(err, domain.ErrInvalidQuantity):
		return "INVALID_QUANTITY", true
	case errors.Is(err, domain.ErrPrecision):
		return "INVALID_PRECISION", true
	case errors.Is(err, domain.ErrPriceOffTick):
		return "PRICE_OFF_TICK", true
	case errors.Is(err, domain.ErrQuantityOffLot):
		return "QUANTITY_OFF_LOT", true
	case errors.Is(err, domain.ErrQuantityBelowMinimum):
		return "QUANTITY_BELOW_MIN", true
	case errors.Is(err, domain.ErrNotionalBelowMinimum):
		return "NOTIONAL_BELOW_MIN", true
	case errors.Is(err, domain.ErrOverflow):
		return "ORDER_OUT_OF_RANGE", true
	default:
		return "", false
	}
}

func newLeaseOwner() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate lease owner: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 v4
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

func readMarketRules(ctx context.Context, tx pgx.Tx, marketID string) (domain.MarketRules, bool, error) {
	const query = `SELECT market_id, numeric_market_id, config_version,
        base_asset, quote_asset, base_scale, quote_scale, price_scale,
        price_tick_units::text, quantity_step_base_atoms::text,
        min_quantity_base_atoms::text, min_notional_quote_atoms::text, status
        FROM order_service.market_catalog WHERE market_id=$1 FOR SHARE`
	var m domain.MarketRules
	var marketNumeric int64
	var baseScale, quoteScale, priceScale int16
	var tick, step, minQty, minNotional, status string
	err := tx.QueryRow(ctx, query, marketID).Scan(
		&m.MarketID, &marketNumeric, &m.ConfigVersion, &m.BaseAsset, &m.QuoteAsset,
		&baseScale, &quoteScale, &priceScale, &tick, &step, &minQty, &minNotional, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MarketRules{}, false, nil
	}
	if err != nil {
		return domain.MarketRules{}, false, fmt.Errorf("read market catalog: %w", err)
	}
	if marketNumeric <= 0 || marketNumeric > math.MaxUint32 {
		return domain.MarketRules{}, false, errors.New("invalid numeric market ID in catalog")
	}
	m.NumericMarketID = uint32(marketNumeric)
	m.BaseScale, m.QuoteScale, m.PriceScale = uint32(baseScale), uint32(quoteScale), uint32(priceScale)
	if m.PriceTickUnits, err = strconv.ParseUint(tick, 10, 64); err != nil {
		return m, false, err
	}
	if m.QuantityStepBaseAtoms, err = strconv.ParseUint(step, 10, 64); err != nil {
		return m, false, err
	}
	if m.MinQuantityBaseAtoms, err = strconv.ParseUint(minQty, 10, 64); err != nil {
		return m, false, err
	}
	if m.MinNotionalQuoteAtoms, err = strconv.ParseUint(minNotional, 10, 64); err != nil {
		return m, false, err
	}
	switch status {
	case "ACTIVE":
		m.Status = domain.MarketActive
	case "HALTED":
		m.Status = domain.MarketHalted
	default:
		return m, false, errors.New("unrecognized catalog market status")
	}
	return m, true, nil
}

const insertRejectedSQL = `
INSERT INTO order_service.orders (
    account_id, client_order_id, request_fingerprint, market_id,
    side, time_in_force, price_decimal, quantity_decimal,
    admission_status, rejection_code, rejected_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'REJECTED',$9,transaction_timestamp())
ON CONFLICT (account_id, client_order_id) DO NOTHING RETURNING order_id`

func insertRejected(ctx context.Context, tx pgx.Tx, accountID string, s domain.NormalizedSubmission, code string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, insertRejectedSQL, accountID, s.ClientOrderID, s.Fingerprint[:],
		s.MarketID, int16(s.Side), int16(s.TimeInForce), s.Price, s.Quantity, code).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("insert rejected admission: %w", err)
	}
	return id, nil
}

const insertPendingSQL = `
INSERT INTO order_service.orders (
    account_id, client_order_id, request_fingerprint, market_id,
    side, time_in_force, price_decimal, quantity_decimal, admission_status,
    market_numeric_id, market_config_version, base_asset, quote_asset,
    price_scale, base_scale, quote_scale, price_tick_units,
    quantity_step_base_atoms, market_min_quantity_base_atoms,
    market_min_notional_quote_atoms, market_status,
    price_units, quantity_base_atoms, notional_quote_atoms, price_ticks, quantity_lots,
    reservation_asset, reservation_atoms, reservation_policy_id, reservation_policy_version,
    admission_lease_generation, admission_lease_owner, admission_lease_until
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,'PENDING',
    $9,$10,$11,$12,$13,$14,$15,$16::text::numeric,$17::text::numeric,$18::text::numeric,$19::text::numeric,'ACTIVE',
    $20::text::numeric,$21::text::numeric,$22::text::numeric,$23,$24,$25,$26::text::numeric,$27,$28,
    1,$29::text::uuid,transaction_timestamp()+($30::bigint * interval '1 millisecond')
)
ON CONFLICT (account_id, client_order_id) DO NOTHING
RETURNING order_id, admission_lease_until`

func insertPendingClaim(ctx context.Context, tx pgx.Tx, accountID string, c domain.CanonicalLimit, p app.ReservationPlan, owner string, ttl time.Duration) (int64, time.Time, error) {
	var id int64
	var until time.Time
	err := tx.QueryRow(ctx, insertPendingSQL,
		accountID, c.Submission.ClientOrderID, c.Submission.Fingerprint[:], c.Submission.MarketID,
		int16(c.Submission.Side), int16(c.Submission.TimeInForce), c.Submission.Price, c.Submission.Quantity,
		int64(c.Market.NumericMarketID), c.Market.ConfigVersion, c.Market.BaseAsset, c.Market.QuoteAsset,
		int16(c.Market.PriceScale), int16(c.Market.BaseScale), int16(c.Market.QuoteScale),
		strconv.FormatUint(c.Market.PriceTickUnits, 10), strconv.FormatUint(c.Market.QuantityStepBaseAtoms, 10),
		strconv.FormatUint(c.Market.MinQuantityBaseAtoms, 10), strconv.FormatUint(c.Market.MinNotionalQuoteAtoms, 10),
		strconv.FormatUint(c.PriceUnits, 10), strconv.FormatUint(c.QuantityBaseAtoms, 10),
		strconv.FormatUint(c.NotionalQuoteAtoms, 10), c.PriceTicks, c.QuantityLots,
		p.Asset, strconv.FormatUint(p.Atoms, 10), p.PolicyID, p.PolicyVersion,
		owner, ttl.Milliseconds()).Scan(&id, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, nil
	}
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("insert pending admission: %w", err)
	}
	return id, until, nil
}

// Finalize performs TX B. The row lock serializes with lease recovery, and the
// conditional UPDATE *rechecks* the token and DB clock after all pure work.
// ACCEPTED and the immutable serialized MatchingCommand are committed together.
// No Accounting RPC and no Kafka publication occurs within this transaction.
func (r *AdmissionRepository) Finalize(ctx context.Context, orderID int64, lease app.Lease, evidence app.ReservationEvidence) (app.AdmissionRecord, error) {
	if orderID <= 0 || lease.Owner == "" || lease.Generation <= 0 {
		return app.AdmissionRecord{}, app.ErrAdmissionLeaseLost
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("begin TX B: %w", err)
	}
	defer tx.Rollback(context.Background())

	var accountID, clientOrderID, state string
	// Do not trust any client-supplied price, side, market, reservation atoms,
	// or Kafka payload. The locked row and its frozen snapshot are authoritative.
	err = tx.QueryRow(ctx, `SELECT account_id, client_order_id, admission_status
		FROM order_service.orders WHERE order_id=$1 FOR UPDATE`, orderID).
		Scan(&accountID, &clientOrderID, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.AdmissionRecord{}, app.ErrAdmissionNotFound
	}
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("lock admission: %w", err)
	}
	if state != "PENDING" {
		return app.AdmissionRecord{}, app.ErrAdmissionLeaseLost // never replay a terminal write
	}
	record, found, err := readAdmission(ctx, tx, accountID, clientOrderID)
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("read frozen TX B admission snapshot: %w", err)
	}
	if !found || record.OrderID != orderID || record.Canonical == nil || record.Reservation == nil {
		return app.AdmissionRecord{}, errors.New("invalid frozen TX B admission snapshot")
	}
	expected := app.ReservationRequest{
		OperationID: "order-reserve:" + strconv.FormatInt(orderID, 10),
		AccountID:   record.AccountID, ReferenceID: strconv.FormatInt(orderID, 10),
		Asset: record.Reservation.Asset, Atoms: record.Reservation.Atoms,
	}
	if err = app.ValidateReservationEvidence(expected, evidence); err != nil {
		return app.AdmissionRecord{}, err
	}
	commandID, err := domain.PlaceCommandID(orderID)
	if err != nil {
		return app.AdmissionRecord{}, err
	}
	c := record.Canonical
	command := &matchingpb.MatchingCommand{
		CommandId: commandID, MarketId: c.Market.NumericMarketID,
		Command: &matchingpb.MatchingCommand_PlaceLimitOrder{
			PlaceLimitOrder: &matchingpb.PlaceLimitOrder{
				OrderId: uint64(orderID), Side: orderpb.OrderSide(c.Submission.Side),
				PriceTicks: c.PriceTicks, QuantityLots: c.QuantityLots,
			},
		},
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(command)
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("serialize frozen matching command: %w", err)
	}
	// Both the owner/generation and lease expiry are checked against the
	// PostgreSQL clock AFTER the FOR UPDATE lock is held. Expired/stale
	// claimants may never finalize or create publication obligations.
	var updated int64
	err = tx.QueryRow(ctx, `UPDATE order_service.orders
		SET admission_status='ACCEPTED', reservation_id=$4,
			reservation_confirmed_at=clock_timestamp(), accepted_at=clock_timestamp(),
			admission_lease_owner=NULL, admission_lease_until=NULL,
			updated_at=clock_timestamp()
		WHERE order_id=$1 AND admission_status='PENDING'
			AND admission_lease_owner=$2::uuid AND admission_lease_generation=$3
			AND admission_lease_until > clock_timestamp()
		RETURNING order_id`, orderID, lease.Owner, lease.Generation, evidence.ReservationID).Scan(&updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.AdmissionRecord{}, app.ErrAdmissionLeaseLost
	}
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("fenced acceptance: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO order_service.matching_outbox (
		order_id,command_id,command_type,market_numeric_id,kafka_topic,kafka_key,payload
	) VALUES ($1,$2,'PLACE_LIMIT',$3,$4,$5,$6)`, orderID, commandID,
		int64(c.Market.NumericMarketID), r.matchingTopic, strconv.FormatUint(uint64(c.Market.NumericMarketID), 10), payload)
	if err != nil {
		return app.AdmissionRecord{}, fmt.Errorf("insert matching outbox (TX B rolled back): %w", err)
	}
	// Deferred triggers validate both directions of the ACCEPTED/outbox link
	// at COMMIT, in addition to the single-transaction application invariant.
	if err := tx.Commit(ctx); err != nil {
		// A connection loss during COMMIT is ambiguous. The caller must read
		// the original idempotency identity before attempting further work.
		return app.AdmissionRecord{}, fmt.Errorf("%w: %w", app.ErrAdmissionCommitUnknown, err)
	}
	record.State = domain.AdmissionAccepted
	return record, nil
}
