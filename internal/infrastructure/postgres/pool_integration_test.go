//go:build integration

package postgres

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"
)

// Run against disposable Docker PostgreSQL, never a production database.
func TestPostgreSQLConnectivityAndAuthentication(t *testing.T) {
	dsn := os.Getenv("ORDER_SERVICE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("ORDER_SERVICE_TEST_DATABASE_URL is required for integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := Open(ctx, dsn, 3)
	if err != nil {
		t.Fatalf("open test PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 returned %d", one)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	username := parsed.User.Username()
	parsed.User = url.UserPassword(username, "intentionally-wrong-password")
	badCtx, badCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer badCancel()
	if badPool, err := Open(badCtx, parsed.String(), 1); err == nil {
		badPool.Close()
		t.Fatal("connection with bad password unexpectedly succeeded")
	}
}
