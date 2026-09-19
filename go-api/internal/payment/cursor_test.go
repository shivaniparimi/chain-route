package payment

import (
	"encoding/base64"
	"errors"
	"testing"
)

func TestDecodeCursor_RoundTrip(t *testing.T) {
	encoded := EncodeCursor("2026-01-02T03:04:05Z", "pay-123")
	createdAt, id, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("unexpected error decoding a validly encoded cursor: %v", err)
	}
	if createdAt != "2026-01-02T03:04:05Z" || id != "pay-123" {
		t.Fatalf("expected round-tripped (createdAt, id), got (%q, %q)", createdAt, id)
	}
}

func TestDecodeCursor_InvalidBase64WrapsErrInvalidCursor(t *testing.T) {
	_, _, err := DecodeCursor("not-valid-base64!!!")
	if err == nil {
		t.Fatal("expected an error for non-base64 cursor input")
	}
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected errors.Is(err, ErrInvalidCursor) to be true, got err=%v", err)
	}
}

func TestDecodeCursor_MalformedFormatWrapsErrInvalidCursor(t *testing.T) {
	// Valid base64, but missing the "createdAt,id" comma-separated structure.
	malformed := base64.URLEncoding.EncodeToString([]byte("no-comma-here"))
	_, _, err := DecodeCursor(malformed)
	if err == nil {
		t.Fatal("expected an error for a cursor missing the createdAt,id structure")
	}
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected errors.Is(err, ErrInvalidCursor) to be true, got err=%v", err)
	}
}
