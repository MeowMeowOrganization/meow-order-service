package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	query "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PrivateQueryRepository is read-only. Every SQL statement scopes ownership
// inside PostgreSQL; no intermediate cross-account order data is returned.
type PrivateQueryRepository struct{ db *pgxpool.Pool }

func NewPrivateQueryRepository(db *pgxpool.Pool) (*PrivateQueryRepository, error) {
	if db == nil {
		return nil, errors.New("private query repository requires PostgreSQL")
	}
	return &PrivateQueryRepository{db: db}, nil
}

const privateOrderColumns = `
    o.order_id, o.account_id, o.client_order_id, o.market_id,
    o.side, o.time_in_force, o.price_decimal, o.quantity_decimal,
    COALESCE(o.price_units::text, ''), COALESCE(o.quantity_base_atoms::text, ''),
    COALESCE((l.filled_lots::numeric * o.quantity_step_base_atoms)::text, ''),
    COALESCE((l.cancelled_lots::numeric * o.quantity_step_base_atoms)::text, ''),
    COALESCE((CASE WHEN l.status = 'REJECTED' THEN 0::numeric
                   ELSE (l.total_lots - l.filled_lots - l.cancelled_lots)::numeric * o.quantity_step_base_atoms
              END)::text, ''),
    COALESCE(l.total_lots, 0), COALESCE(l.filled_lots, 0), COALESCE(l.cancelled_lots, 0),
    CASE WHEN o.admission_status = 'PENDING' THEN 'ADMISSION_PENDING'
         WHEN o.admission_status = 'REJECTED' THEN 'ADMISSION_REJECTED'
         ELSE COALESCE(l.status, 'MISSING_LIFECYCLE') END,
    o.admission_status, COALESCE(o.rejection_code, ''),
    COALESCE(l.matcher_rejection_reason, ''), COALESCE(c.status, ''),
    COALESCE(c.matcher_rejection_reason, ''), COALESCE(l.version,0),
    COALESCE(l.last_market_sequence::text, ''),
    o.created_at,
    GREATEST(o.updated_at, COALESCE(l.updated_at, o.updated_at),
             COALESCE(c.completed_at, c.created_at, o.updated_at)),
    l.opened_at, l.terminal_at`

const privateOrderFrom = ` FROM order_service.orders o
    LEFT JOIN order_service.order_lifecycle l ON l.order_id = o.order_id
    LEFT JOIN order_service.cancel_requests c ON c.order_id = o.order_id`

func scanPrivateOrder(row pgx.Row) (query.Order, error) {
	var o query.Order
	err := row.Scan(&o.ID, &o.AccountID, &o.ClientOrderID, &o.MarketID,
		&o.Side, &o.TimeInForce, &o.PriceDecimal, &o.QuantityDecimal,
		&o.PriceUnits, &o.QuantityBaseAtoms, &o.FilledBaseAtoms,
		&o.CancelledBaseAtoms, &o.RemainingBaseAtoms,
		&o.TotalLots, &o.FilledLots, &o.CancelledLots,
		&o.Status, &o.AdmissionStatus, &o.AdmissionRejection, &o.MatcherRejection,
		&o.CancelStatus, &o.CancelRejection, &o.Version, &o.MarketSequence,
		&o.CreatedAt, &o.UpdatedAt, &o.OpenedAt, &o.TerminalAt)
	if err == nil && o.Status == "MISSING_LIFECYCLE" {
		return query.Order{}, errors.New("accepted order missing canonical lifecycle")
	}
	return o, err
}

func (r *PrivateQueryRepository) FindOrder(ctx context.Context, account string, id int64) (query.Order, error) {
	sql := `SELECT ` + privateOrderColumns + privateOrderFrom + ` WHERE o.account_id=$1 AND o.order_id=$2`
	item, err := scanPrivateOrder(r.db.QueryRow(ctx, sql, account, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return query.Order{}, query.ErrNotFound
	}
	if err != nil {
		return query.Order{}, fmt.Errorf("get private order: %w", err)
	}
	return item, nil
}

func (r *PrivateQueryRepository) ListOrders(ctx context.Context, f query.OrdersFilter) ([]query.Order, error) {
	sql := `SELECT ` + privateOrderColumns + privateOrderFrom + ` WHERE o.account_id=$1`
	args := []any{f.AccountID}
	if f.MarketID != "" {
		args = append(args, f.MarketID)
		sql += fmt.Sprintf(` AND o.market_id=$%d::text`, len(args))
	}
	if f.OpenOnly {
		sql += ` AND o.admission_status='ACCEPTED' AND l.status IN ('OPEN','PARTIALLY_FILLED')`
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		sql += fmt.Sprintf(` AND (o.created_at, o.order_id) < ($%d::timestamptz, $%d::bigint)`, len(args)-1, len(args))
	}
	args = append(args, f.Limit+1)
	sql += fmt.Sprintf(` ORDER BY o.created_at DESC, o.order_id DESC LIMIT $%d::integer`, len(args))
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list private orders: %w", err)
	}
	defer rows.Close()
	items := make([]query.Order, 0, f.Limit+1)
	for rows.Next() {
		o, err := scanPrivateOrder(rows)
		if err != nil {
			return nil, fmt.Errorf("scan private order: %w", err)
		}
		items = append(items, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read private orders: %w", err)
	}
	return items, nil
}

// UNION ALL is intentional: if one account owns both sides of an execution it
// receives distinct maker and taker private fills, never a counterparty ID.
const privateTradesSQL = `WITH legs AS (
    SELECT e.market_numeric_id, e.market_sequence, e.event_ordinal,
           e.maker_order_id AS owned_order_id, e.price_ticks, e.quantity_lots,
           e.applied_at, 'MAKER'::text AS role
    FROM order_service.orders owned
    JOIN order_service.private_executions e ON e.maker_order_id=owned.order_id
    WHERE owned.account_id=$1::text AND ($2::text='' OR owned.market_id=$2::text)
      AND ($3::bigint=0 OR owned.order_id=$3::bigint)
    UNION ALL
    SELECT e.market_numeric_id, e.market_sequence, e.event_ordinal,
           e.taker_order_id AS owned_order_id, e.price_ticks, e.quantity_lots,
           e.applied_at, 'TAKER'::text AS role
    FROM order_service.orders owned
    JOIN order_service.private_executions e ON e.taker_order_id=owned.order_id
    WHERE owned.account_id=$1::text AND ($2::text='' OR owned.market_id=$2::text)
      AND ($3::bigint=0 OR owned.order_id=$3::bigint)
)
SELECT e.owned_order_id, o.market_id, e.market_numeric_id,
       e.market_sequence::text, e.event_ordinal::text, e.role, o.side,
       e.price_ticks, e.quantity_lots,
       (e.price_ticks::numeric * o.price_tick_units)::text,
       (e.quantity_lots::numeric * o.quantity_step_base_atoms)::text,
       e.applied_at
FROM legs e JOIN order_service.orders o ON o.order_id=e.owned_order_id
WHERE true`

func (r *PrivateQueryRepository) ListTrades(ctx context.Context, f query.TradesFilter) ([]query.Trade, error) {
	sql := privateTradesSQL
	args := []any{f.AccountID, f.MarketID, f.OrderID}
	if f.After != nil {
		c := f.After
		args = append(args, c.AppliedAt, int64(c.MarketID), strconv.FormatUint(c.Sequence, 10), strconv.FormatUint(c.Ordinal, 10), c.Role)
		sql += fmt.Sprintf(` AND (e.applied_at, e.market_numeric_id, e.market_sequence, e.event_ordinal, e.role) <
            ($%d::timestamptz, $%d::bigint, $%d::numeric, $%d::numeric, $%d::text)`,
			len(args)-4, len(args)-3, len(args)-2, len(args)-1, len(args))
	}
	args = append(args, f.Limit+1)
	sql += fmt.Sprintf(` ORDER BY e.applied_at DESC, e.market_numeric_id DESC,
        e.market_sequence DESC, e.event_ordinal DESC, e.role DESC LIMIT $%d::integer`, len(args))
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list private trades: %w", err)
	}
	defer rows.Close()
	result := make([]query.Trade, 0, f.Limit+1)
	for rows.Next() {
		var tr query.Trade
		var marketID int64
		err := rows.Scan(&tr.OrderID, &tr.MarketID, &marketID,
			&tr.MarketSequence, &tr.EventOrdinal, &tr.Role, &tr.Side,
			&tr.PriceTicks, &tr.QuantityLots, &tr.PriceUnits, &tr.QuantityBaseAtoms,
			&tr.AppliedAt)
		if err != nil {
			return nil, fmt.Errorf("scan private trade: %w", err)
		}
		if marketID < 1 || marketID > 4294967295 {
			return nil, errors.New("invalid execution market")
		}
		tr.NumericMarketID = uint32(marketID)
		result = append(result, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read private trades: %w", err)
	}
	return result, nil
}
