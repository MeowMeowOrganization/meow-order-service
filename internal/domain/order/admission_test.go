package order

import (
	"errors"
	"testing"
)

func TestAdmissionTransitions(t *testing.T) {
	t.Parallel()
	allowed := [][2]AdmissionState{
		{AdmissionPending, AdmissionPending},
		{AdmissionPending, AdmissionAccepted},
		{AdmissionPending, AdmissionRejected},
		{AdmissionAccepted, AdmissionAccepted},
		{AdmissionRejected, AdmissionRejected},
	}
	for _, pair := range allowed {
		if err := ValidateAdmissionTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("rejected legal transition %q -> %q: %v", pair[0], pair[1], err)
		}
	}
	invalid := [][2]AdmissionState{
		{AdmissionAccepted, AdmissionRejected}, {AdmissionAccepted, AdmissionPending},
		{AdmissionRejected, AdmissionAccepted}, {AdmissionRejected, AdmissionPending},
		{"", AdmissionPending}, {AdmissionPending, "INVALID"},
	}
	for _, pair := range invalid {
		if err := ValidateAdmissionTransition(pair[0], pair[1]); !errors.Is(err, ErrInvalidAdmissionTransition) {
			t.Fatalf("accepted illegal transition %q -> %q: %v", pair[0], pair[1], err)
		}
	}
}

func TestStableOperationAndCommandIDs(t *testing.T) {
	t.Parallel()
	for _, id := range []int64{-1, 0} {
		if _, err := ReservationOperationID(id); !errors.Is(err, ErrInvalidSubmission) {
			t.Fatal(err)
		}
		if _, err := PlaceCommandID(id); !errors.Is(err, ErrInvalidSubmission) {
			t.Fatal(err)
		}
	}
	reserve, err := ReservationOperationID(42)
	if err != nil || reserve != "order-reserve:42" {
		t.Fatalf("%q: %v", reserve, err)
	}
	command, err := PlaceCommandID(42)
	if err != nil || command != "place:42" {
		t.Fatalf("%q: %v", command, err)
	}
}
