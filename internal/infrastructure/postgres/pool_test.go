package postgres

import (
	"context"
	"strings"
	"testing"
)

func TestOpenRejectsInvalidConfigurationWithoutEchoingSecret(t *testing.T) {
	_, err := Open(context.Background(), "postgres://me:secret@invalid:badport/db", 10)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("secret leaked in error: %v", err)
	}
}

func TestOpenRejectsInvalidPoolSize(t *testing.T) {
	if _, err := Open(context.Background(), "postgres://localhost/db", 0); err == nil {
		t.Fatal("expected invalid pool error")
	}
}
