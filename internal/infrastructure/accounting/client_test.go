package accounting

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	accountingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/accounting/v1alpha1"
	commonpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/common/v1alpha1"
	privatepb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/private/accounting/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/order"
)

func mockAccountingServer(t *testing.T, handle func(context.Context, *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(ReserveFundsProcedure, connect.NewUnaryHandler[privatepb.ReserveFundsRequest, privatepb.ReserveFundsResponse](
		ReserveFundsProcedure,
		func(ctx context.Context, in *connect.Request[privatepb.ReserveFundsRequest]) (*connect.Response[privatepb.ReserveFundsResponse], error) {
			out, err := handle(ctx, in.Msg)
			if err != nil {
				return nil, err
			}
			return connect.NewResponse(out), nil
		},
	))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func testRequest() app.ReservationRequest {
	return app.ReservationRequest{OperationID: "order-reserve:42", AccountID: "acct-1", ReferenceID: "42", Asset: "USDT", Atoms: 1234567}
}

func validResponse(req *privatepb.ReserveFundsRequest) *privatepb.ReserveFundsResponse {
	return &privatepb.ReserveFundsResponse{Reservation: &accountingpb.Reservation{
		ReservationId: "reservation-42", AccountId: req.GetAccountId(),
		ReferenceId: req.GetReferenceId(), OriginalAmount: req.GetAmount(),
		RemainingAtoms: req.GetAmount().GetAtoms(),
		Status:         accountingpb.ReservationStatus_RESERVATION_STATUS_ACTIVE,
	}}
}

func TestClientUsesExactAccountingContractAndValidatesResponse(t *testing.T) {
	var calls int
	srv := mockAccountingServer(t, func(_ context.Context, req *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error) {
		calls++
		if req.GetOperationId() != "order-reserve:42" || req.GetAccountId() != "acct-1" ||
			req.GetReferenceId() != "42" || req.GetAmount().GetAsset() != "USDT" || req.GetAmount().GetAtoms() != 1234567 {
			t.Errorf("wrong wire-level request %+v", req)
		}
		return validResponse(req), nil
	})
	client, err := NewClient(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Reserve(context.Background(), testRequest())
	if err != nil || calls != 1 || got.ReservationID != "reservation-42" || got.OriginalAtoms != 1234567 || !got.Active {
		t.Fatalf("client reservation %+v calls=%d err=%v", got, calls, err)
	}
}

func TestClientRejectsMalformedOrMismatchedEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*privatepb.ReserveFundsResponse)
	}{
		{"missing reservation", func(r *privatepb.ReserveFundsResponse) { r.Reservation = nil }},
		{"mismatched account", func(r *privatepb.ReserveFundsResponse) { r.Reservation.AccountId = "someone-else" }},
		{"mismatched asset", func(r *privatepb.ReserveFundsResponse) {
			r.Reservation.OriginalAmount = &commonpb.AssetAmount{Asset: "BTC", Atoms: 1234567}
		}},
		{"mismatched amount", func(r *privatepb.ReserveFundsResponse) { r.Reservation.OriginalAmount.Atoms-- }},
		{"partially consumed", func(r *privatepb.ReserveFundsResponse) { r.Reservation.RemainingAtoms-- }},
		{"not active", func(r *privatepb.ReserveFundsResponse) {
			r.Reservation.Status = accountingpb.ReservationStatus_RESERVATION_STATUS_RELEASED
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := mockAccountingServer(t, func(_ context.Context, req *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error) {
				resp := validResponse(req)
				tc.mutate(resp)
				return resp, nil
			})
			client, err := NewClient(srv.Client(), srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Reserve(context.Background(), testRequest())
			if !errors.Is(err, app.ErrReservationEvidenceInvalid) {
				t.Fatalf("unsafe reservation accepted: %v", err)
			}
		})
	}
}

func TestAccountingTimeoutIsAmbiguous(t *testing.T) {
	srv := mockAccountingServer(t, func(ctx context.Context, req *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	client, err := NewClient(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.Reserve(ctx, testRequest())
	if !errors.Is(err, app.ErrReservationOutcomeUnknown) {
		t.Fatalf("timeout must preserve PENDING: %v", err)
	}
}

func TestExplicitFailedPreconditionNotMistakenForTransportTimeout(t *testing.T) {
	srv := mockAccountingServer(t, func(context.Context, *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("insufficient funds"))
	})
	client, err := NewClient(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Reserve(context.Background(), testRequest())
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || errors.Is(err, app.ErrReservationOutcomeUnknown) {
		t.Fatalf("bad definitive-failure classification: %v", err)
	}
}

func TestClientRejectsUntrustedEndpointAndEmptyRequest(t *testing.T) {
	for _, u := range []string{"", "ftp://localhost:8080", "http://localhost:8080/path", "http://user:pass@localhost", "https://example.com?token=secret"} {
		if _, err := NewClient(http.DefaultClient, u); err == nil {
			t.Errorf("accepted invalid origin: %s", u)
		}
	}
	if _, err := NewClient(nil, "http://localhost:8080"); err == nil {
		t.Fatal("accepted nil http client")
	}
	srv := mockAccountingServer(t, func(context.Context, *privatepb.ReserveFundsRequest) (*privatepb.ReserveFundsResponse, error) {
		t.Fatal("invalid request reached Accounting")
		return nil, nil
	})
	client, err := NewClient(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Reserve(context.Background(), app.ReservationRequest{}); err == nil {
		t.Fatal("accepted empty request")
	}
}
