package payment

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// EncodeCursor and DecodeCursor implement opaque keyset-pagination cursors
// (base64 of "createdAtRFC3339Nano,id") for ListPayments. They live in this
// shared package -- rather than being duplicated in, or imported between,
// handler and postgres -- because both of those packages already import
// payment: handler needs to encode the outgoing next_cursor field, and
// postgres needs to decode an incoming cursor into SQL WHERE-clause
// arguments. postgres must not import handler (that would invert the
// dependency direction the rest of the codebase uses -- handler depends on
// postgres via the narrow *Store interfaces, never the reverse), so
// duplicating a decode function in postgres/store.go was the brief's
// fallback; payment is the natural single home since nothing needs to
// import a sibling package to reach it. The cursor's content is an
// implementation detail, never interpreted by the client.
func EncodeCursor(createdAt, id string) string {
	return base64.URLEncoding.EncodeToString([]byte(createdAt + "," + id))
}

func DecodeCursor(cursor string) (createdAt, id string, err error) {
	raw, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), ",", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid cursor format")
	}
	return parts[0], parts[1], nil
}
