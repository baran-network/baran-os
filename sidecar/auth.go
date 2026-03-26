package sidecar

import (
	"net/http"
	"strings"
)

// authMiddleware validates PSK authentication per request.
// Accepts: Authorization: Bearer <psk> header, or ?token=<psk> query parameter.
// If PSK is empty, all requests are allowed (development mode).
func authMiddleware(psk string, next http.Handler) http.Handler {
	if psk == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if extractToken(r) != psk {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid or missing authentication token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractToken retrieves the bearer token from the request.
// Priority: Authorization header > token query parameter.
func extractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return r.URL.Query().Get("token")
}
