package redisx

import (
	"crypto/subtle"
	"net/http"
)

// RequireToken guards an admin API with a bearer token.
func RequireToken(token string, next http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, `{"error":"admin token required"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
