// Package accounting adapts the private Accounting Connect-RPC contract to
// the Order Service's consumer-owned FundsReserver application port.
package accounting

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"connectrpc.com/connect"
	accountingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/accounting/v1alpha1"
	commonpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/common/v1alpha1"
	privatepb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/accounting/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
)

const ReserveFundsProcedure = "/meow.exchange.private.accounting.v1alpha1.AccountingService/ReserveFunds"

// Client does not implement its own retries. Retrying automatically inside an
// HTTP transport could outlive the caller's admission lease. Recovery invokes
// ReserveFunds again, with the SAME caller-owned operation_id.
type Client struct {
	reserve *connect.Client[privatepb.ReserveFundsRequest, privatepb.ReserveFundsResponse]
}

var _ app.FundsReserver = (*Client)(nil)

func NewClient(httpClient *http.Client, baseURL string) (*Client, error) {
	if httpClient == nil {
		return nil, errors.New("accounting HTTP client is required")
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return nil, errors.New("accounting endpoint must be an http(s) origin with no credentials, path, query or fragment")
	}
	return &Client{reserve: connect.NewClient[privatepb.ReserveFundsRequest, privatepb.ReserveFundsResponse](
		httpClient, strings.TrimRight(baseURL, "/")+ReserveFundsProcedure,
	)}, nil
}

func (c *Client) Reserve(ctx context.Context, in app.ReservationRequest) (app.ReservationEvidence, error) {
	if c == nil || c.reserve == nil {
		return app.ReservationEvidence{}, errors.New("accounting client is not initialized")
	}
	if in.OperationID == "" || in.AccountID == "" || in.ReferenceID == "" || in.Asset == "" || in.Atoms == 0 {
		return app.ReservationEvidence{}, errors.New("invalid Accounting reservation request")
	}
	response, err := c.reserve.CallUnary(ctx, connect.NewRequest(&privatepb.ReserveFundsRequest{
		OperationId: in.OperationID,
		AccountId:   in.AccountID,
		ReferenceId: in.ReferenceID,
		Amount:      &commonpb.AssetAmount{Asset: in.Asset, Atoms: in.Atoms},
	}))
	if err != nil {
		switch connect.CodeOf(err) {
		case connect.CodeCanceled, connect.CodeDeadlineExceeded, connect.CodeUnavailable,
			connect.CodeUnknown, connect.CodeInternal:
			// A timeout or transport error cannot prove that Accounting
			// rolled back. Preserve PENDING and retry the same operation ID.
			return app.ReservationEvidence{}, fmt.Errorf("%w: %w", app.ErrReservationOutcomeUnknown, err)
		default:
			// E.g. a documented insufficient-funds FailedPrecondition;
			// CEX 18.3 never persists a terminal rejection yet.
			return app.ReservationEvidence{}, fmt.Errorf("accounting ReserveFunds: %w", err)
		}
	}
	if response == nil || response.Msg == nil || response.Msg.GetReservation() == nil {
		return app.ReservationEvidence{}, app.ErrReservationEvidenceInvalid
	}
	reservation := response.Msg.GetReservation()
	amount := reservation.GetOriginalAmount()
	if amount == nil {
		return app.ReservationEvidence{}, app.ErrReservationEvidenceInvalid
	}
	out := app.ReservationEvidence{
		ReservationID:  reservation.GetReservationId(),
		AccountID:      reservation.GetAccountId(),
		ReferenceID:    reservation.GetReferenceId(),
		Asset:          amount.GetAsset(),
		OriginalAtoms:  amount.GetAtoms(),
		RemainingAtoms: reservation.GetRemainingAtoms(),
		Active:         reservation.GetStatus() == accountingpb.ReservationStatus_RESERVATION_STATUS_ACTIVE,
	}
	// Validate inside the adapter too, even for consumers that don't use
	// the PlaceOrder coordinator. Nothing is ACCEPTED at this point.
	if out.ReservationID == "" || !out.Active || out.AccountID != in.AccountID ||
		out.ReferenceID != in.ReferenceID || out.Asset != in.Asset ||
		out.OriginalAtoms != in.Atoms || out.RemainingAtoms != in.Atoms {
		return app.ReservationEvidence{}, app.ErrReservationEvidenceInvalid
	}
	return out, nil
}
