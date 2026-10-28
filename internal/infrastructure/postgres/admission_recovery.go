package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
	"github.com/jackc/pgx/v5"
)

// ReclaimExpired obtains a NEW generation using one atomic conditional UPDATE. The
// returned lease is the only authority to call Accounting again. It never
// recomputes the request, market snapshot, fee plan or operation_id.
// The caller is responsible for scheduling; no background worker is launched.
var _ app.RecoveryRepository = (*AdmissionRepository)(nil)

func (r *AdmissionRepository) ReclaimExpired(ctx context.Context, orderID int64) (app.ClaimResult, error) {
	if orderID <= 0 {
		return app.ClaimResult{}, app.ErrAdmissionNotFound
	}
	owner, err := newLeaseOwner()
	if err != nil {
		return app.ClaimResult{}, err
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return app.ClaimResult{}, fmt.Errorf("begin fenced recovery: %w", err)
	}
	defer tx.Rollback(context.Background())
	const acquireSQL = `UPDATE order_service.orders SET
        admission_lease_generation=admission_lease_generation+1,
        admission_lease_owner=$2::uuid,
        admission_lease_until=clock_timestamp()+($3::bigint * interval '1 millisecond'),
        updated_at=clock_timestamp()
        WHERE order_id=$1 AND admission_status='PENDING'
          AND admission_lease_until <= clock_timestamp()
          AND admission_lease_generation < 9223372036854775807
        RETURNING admission_lease_generation,admission_lease_until`
	var lease *app.Lease
	var generation int64
	var until time.Time
	err = tx.QueryRow(ctx, acquireSQL, orderID, owner, r.leaseTTL.Milliseconds()).Scan(&generation, &until)
	if err == nil {
		lease = &app.Lease{Owner: owner, Generation: generation, Until: until}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return app.ClaimResult{}, fmt.Errorf("reclaim PENDING lease: %w", err)
	}
	var accountID, clientOrderID string
	err = tx.QueryRow(ctx, `SELECT account_id,client_order_id FROM order_service.orders WHERE order_id=$1`, orderID).Scan(&accountID, &clientOrderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.ClaimResult{}, app.ErrAdmissionNotFound
	}
	if err != nil {
		return app.ClaimResult{}, fmt.Errorf("read recovery identity: %w", err)
	}
	record, found, err := readAdmission(ctx, tx, accountID, clientOrderID)
	if err != nil {
		return app.ClaimResult{}, err
	}
	if !found || record.OrderID != orderID {
		return app.ClaimResult{}, errors.New("recovery identity changed")
	}
	if err = tx.Commit(ctx); err != nil {
		// Outcome unknown: never issue Accounting RPC without a confirmed
		// lease. The next recovery call checks PostgreSQL again.
		return app.ClaimResult{}, fmt.Errorf("commit recovery lease: %w", err)
	}
	return app.ClaimResult{Record: record, Lease: lease}, nil
}

func (r *AdmissionRepository) LeaseValid(ctx context.Context, orderID int64, lease app.Lease) (bool, error) {
	if orderID <= 0 || lease.Owner == "" || lease.Generation <= 0 {
		return false, nil
	}
	var active bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM order_service.orders
        WHERE order_id=$1 AND admission_status='PENDING'
          AND admission_lease_owner=$2::uuid
          AND admission_lease_generation=$3
          AND admission_lease_until>clock_timestamp()
    )`, orderID, lease.Owner, lease.Generation).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("verify admission fencing token: %w", err)
	}
	return active, nil
}
