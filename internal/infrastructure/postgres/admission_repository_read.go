package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/order"
	"github.com/jackc/pgx/v5"
)

// readAdmission intentionally reads the stored snapshot, never the current
// market catalog or a newly calculated fee quote, for a duplicate submission.
func readAdmission(ctx context.Context, tx pgx.Tx, accountID, clientOrderID string) (app.AdmissionRecord, bool, error) {
	const identitySQL = `SELECT order_id, request_fingerprint, market_id, side,
        time_in_force, price_decimal, quantity_decimal, admission_status,
        COALESCE(rejection_code, '')
        FROM order_service.orders WHERE account_id=$1 AND client_order_id=$2`
	rec := app.AdmissionRecord{AccountID: accountID}
	rec.Submission.ClientOrderID = clientOrderID
	var fp []byte
	var side, tif int16
	var storedState string
	err := tx.QueryRow(ctx, identitySQL, accountID, clientOrderID).Scan(
		&rec.OrderID, &fp, &rec.Submission.MarketID, &side, &tif,
		&rec.Submission.Price, &rec.Submission.Quantity, &storedState, &rec.RejectionCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.AdmissionRecord{}, false, nil
	}
	if err != nil {
		return app.AdmissionRecord{}, false, fmt.Errorf("read admission identity: %w", err)
	}
	if len(fp) != len(rec.Submission.Fingerprint) {
		return app.AdmissionRecord{}, false, errors.New("invalid persisted fingerprint length")
	}
	copy(rec.Submission.Fingerprint[:], fp)
	rec.State = domain.AdmissionState(storedState)
	rec.Submission.Side = domain.Side(side)
	rec.Submission.TimeInForce = domain.TimeInForce(tif)
	recheck, err := domain.NormalizeSubmission(domain.LimitSubmission{
		ClientOrderID: clientOrderID, MarketID: rec.Submission.MarketID,
		Side: rec.Submission.Side, Price: rec.Submission.Price, Quantity: rec.Submission.Quantity,
		TimeInForce: rec.Submission.TimeInForce,
	})
	if err != nil || recheck != rec.Submission {
		return app.AdmissionRecord{}, false, errors.New("stored admission fingerprint does not match canonical fields")
	}
	switch rec.State {
	case domain.AdmissionRejected:
		if rec.RejectionCode == "" {
			return app.AdmissionRecord{}, false, errors.New("missing persisted rejection code")
		}
		return rec, true, nil
	case domain.AdmissionPending, domain.AdmissionAccepted:
		if rec.RejectionCode != "" {
			return app.AdmissionRecord{}, false, errors.New("unexpected rejection code on non-rejected order")
		}
	default:
		return app.AdmissionRecord{}, false, errors.New("unknown persisted admission state")
	}
	c, p, err := readCanonicalSnapshot(ctx, tx, rec.OrderID, rec.Submission)
	if err != nil {
		return app.AdmissionRecord{}, false, err
	}
	rec.Canonical, rec.Reservation = &c, &p
	return rec, true, nil
}

func readCanonicalSnapshot(ctx context.Context, tx pgx.Tx, orderID int64, s domain.NormalizedSubmission) (domain.CanonicalLimit, app.ReservationPlan, error) {
	const snapshotSQL = `SELECT market_numeric_id, market_config_version, base_asset, quote_asset,
        price_scale, base_scale, quote_scale, price_tick_units::text,
        quantity_step_base_atoms::text, market_min_quantity_base_atoms::text,
        market_min_notional_quote_atoms::text, market_status,
        price_units::text, quantity_base_atoms::text, notional_quote_atoms::text,
        price_ticks, quantity_lots, reservation_asset, reservation_atoms::text,
        reservation_policy_id, reservation_policy_version
        FROM order_service.orders WHERE order_id=$1`
	var m domain.MarketRules
	var p app.ReservationPlan
	m.MarketID = s.MarketID
	var numeric int64
	var priceScale, baseScale, quoteScale int16
	var tick, step, minQty, minNotional, status, price, qty, notional, atoms string
	var priceTicks, quantityLots int64
	err := tx.QueryRow(ctx, snapshotSQL, orderID).Scan(
		&numeric, &m.ConfigVersion, &m.BaseAsset, &m.QuoteAsset,
		&priceScale, &baseScale, &quoteScale, &tick, &step, &minQty,
		&minNotional, &status, &price, &qty, &notional,
		&priceTicks, &quantityLots, &p.Asset, &atoms, &p.PolicyID, &p.PolicyVersion)
	if err != nil {
		return domain.CanonicalLimit{}, p, fmt.Errorf("read frozen admission snapshot: %w", err)
	}
	if numeric <= 0 || numeric > math.MaxUint32 || priceScale < 0 || baseScale < 0 || quoteScale < 0 {
		return domain.CanonicalLimit{}, p, errors.New("invalid persisted market snapshot")
	}
	m.NumericMarketID = uint32(numeric)
	m.PriceScale, m.BaseScale, m.QuoteScale = uint32(priceScale), uint32(baseScale), uint32(quoteScale)
	if status != "ACTIVE" {
		return domain.CanonicalLimit{}, p, errors.New("PENDING/ACCEPTED order has non-active frozen market")
	}
	m.Status = domain.MarketActive
	parse := func(v string) (uint64, error) { return strconv.ParseUint(v, 10, 64) }
	if m.PriceTickUnits, err = parse(tick); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if m.QuantityStepBaseAtoms, err = parse(step); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if m.MinQuantityBaseAtoms, err = parse(minQty); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if m.MinNotionalQuoteAtoms, err = parse(minNotional); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	var savedPrice, savedQty, savedNotional uint64
	if savedPrice, err = parse(price); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if savedQty, err = parse(qty); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if savedNotional, err = parse(notional); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	if p.Atoms, err = parse(atoms); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	c, err := domain.CanonicalizeLimit(s, m)
	if err != nil {
		return domain.CanonicalLimit{}, p, fmt.Errorf("invalid persisted canonical order: %w", err)
	}
	if c.PriceUnits != savedPrice || c.QuantityBaseAtoms != savedQty ||
		c.NotionalQuoteAtoms != savedNotional || c.PriceTicks != priceTicks || c.QuantityLots != quantityLots {
		return domain.CanonicalLimit{}, p, errors.New("persisted ticks/lots/atoms disagree with market snapshot")
	}
	if err := app.ValidateReservationPlan(c, p); err != nil {
		return domain.CanonicalLimit{}, p, err
	}
	return c, p, nil
}
