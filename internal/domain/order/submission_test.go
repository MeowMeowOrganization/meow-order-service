package order

import (
	"encoding/hex"
	"errors"
	"testing"
)

func validSubmission() LimitSubmission {
	return LimitSubmission{
		ClientOrderID: "client:order-001", MarketID: "BTC-USDT",
		Side: SideBuy, Price: "060000.10000", Quantity: "00.0120000",
		TimeInForce: TimeInForceGTC,
	}
}

func TestCanonicalDecimal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ input, want string }{
		{"000", "0"}, {"0.000", "0"}, {"0.0001200", "0.00012"},
		{"0001.2300", "1.23"}, {"000001", "1"}, {"123.456", "123.456"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := CanonicalDecimal(tc.input)
			if err != nil || got != tc.want {
				t.Fatalf("CanonicalDecimal(%q)=(%q,%v), want %q", tc.input, got, err, tc.want)
			}
		})
	}
	for _, invalid := range []string{
		"", ".", ".5", "1.", "1.2.3", "+1", "-0", "1e3", " 1", "1 ",
		"1,000", "NaN", "1\n", "١", "１２", "1_000", "1/2",
	} {
		t.Run("reject_"+invalid, func(t *testing.T) {
			if _, err := CanonicalDecimal(invalid); !errors.Is(err, ErrInvalidDecimal) {
				t.Fatalf("%q: expected invalid decimal, got %v", invalid, err)
			}
		})
	}
}

func TestNormalizeSubmissionFingerprintStableAcrossTextualRepresentations(t *testing.T) {
	t.Parallel()
	first := validSubmission()
	a, err := NormalizeSubmission(first)
	if err != nil {
		t.Fatal(err)
	}
	if a.Price != "60000.1" || a.Quantity != "0.012" {
		t.Fatalf("unexpected normalization: %+v", a)
	}
	second := first
	second.Price = "60000.10"
	second.Quantity = "0.0120"
	second.ClientOrderID = "different-id" // Not part of the semantic fingerprint.
	b, err := NormalizeSubmission(second)
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("equivalent prices/quantities produced different fingerprints")
	}
	for _, mutate := range []func(*LimitSubmission){
		func(o *LimitSubmission) { o.Price = "60000.11" },
		func(o *LimitSubmission) { o.Quantity = "0.013" },
		func(o *LimitSubmission) { o.MarketID = "ETH-USDT" },
		func(o *LimitSubmission) { o.Side = SideSell },
	} {
		changed := first
		mutate(&changed)
		c, err := NormalizeSubmission(changed)
		if err != nil {
			t.Fatal(err)
		}
		if a.Fingerprint == c.Fingerprint {
			t.Fatal("different business requests produced equal fingerprint")
		}
	}
}

func TestNormalizeSubmissionRejectsMalformedInputs(t *testing.T) {
	t.Parallel()
	for _, mutate := range []func(*LimitSubmission){
		func(o *LimitSubmission) { o.ClientOrderID = "" },
		func(o *LimitSubmission) { o.ClientOrderID = "bad id" },
		func(o *LimitSubmission) { o.MarketID = "btc-usdt" },
		func(o *LimitSubmission) { o.Side = 0 },
		func(o *LimitSubmission) { o.TimeInForce = 0 },
	} {
		in := validSubmission()
		mutate(&in)
		if _, err := NormalizeSubmission(in); !errors.Is(err, ErrInvalidSubmission) {
			t.Fatalf("expected invalid submission for %+v, got %v", in, err)
		}
	}
	in := validSubmission()
	in.Price = "1e5"
	if _, err := NormalizeSubmission(in); !errors.Is(err, ErrInvalidDecimal) {
		t.Fatal(err)
	}
	if err := ValidateAccountID("trusted-account-123"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateAccountID("bad account"); !errors.Is(err, ErrInvalidSubmission) {
		t.Fatal(err)
	}
}

// Golden vector pins the exact fingerprint bytes across software releases.
// Changing this representation requires a new fingerprint_version + migration.
func TestSemanticFingerprintV1GoldenVector(t *testing.T) {
	t.Parallel()
	in, err := NormalizeSubmission(validSubmission())
	if err != nil {
		t.Fatal(err)
	}
	const want = "8c4703310a71e434889b646a6d5e5f905023fdc58517a409f33e5a8fce65c33d"
	if got := hex.EncodeToString(in.Fingerprint[:]); got != want {
		t.Fatalf("fingerprint changed: got %s, want %s", got, want)
	}
}
