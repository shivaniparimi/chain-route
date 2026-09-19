package handler

import (
	"net/http"
	"strings"
)

// CORS is a minimal, explicit middleware -- this API has no cookies/
// credentials anywhere, so the need is narrow: allow GET/POST from a
// configured origin list, never "*", never a blanket credentialed
// reflect-any-origin configuration.
func CORS(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[strings.TrimSpace(o)] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		// Vary: Origin must be set unconditionally, not only on the
		// allowed-and-matched path: the response genuinely differs based on
		// the Origin header's value on every code path (whether
		// Access-Control-* headers are added or not), so any shared/
		// intermediate cache sitting in front of this API needs that signal
		// on every response to avoid serving one origin's cached response to
		// another and silently defeating the CORS allowlist for cached
		// responses.
		w.Header().Set("Vary", "Origin")
		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
