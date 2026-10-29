package api

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// defaultCORSMaxAge is the Access-Control-Max-Age (seconds) applied when CORS
// is enabled and the configured MaxAge is zero.
const defaultCORSMaxAge = 600

// corsMiddleware wraps next with CORS handling per the configured CORSConfig.
// A disabled config (no origins) passes through untouched. With "*" it allows
// any origin; with a specific list it requires the request Origin to match and
// rejects everything else with 403 before the request is handled.
func corsMiddleware(cors CORSConfig, next http.Handler) http.Handler {
	if len(cors.Origins) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, ok := cors.allowOrigin(r.Header.Get("Origin"))
		if !ok {
			writeError(w, http.StatusForbidden, "origin not allowed")
			return
		}

		h := w.Header()
		h.Set("Access-Control-Allow-Origin", allowed)
		if allowed != "*" {
			h.Add("Vary", "Origin")
		}
		if cors.AllowCredentials {
			h.Set("Access-Control-Allow-Credentials", "true")
		}

		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", allowedHeaders(r))
			maxAge := cors.MaxAge
			if maxAge == 0 {
				maxAge = defaultCORSMaxAge
			}
			h.Set("Access-Control-Max-Age", strconv.Itoa(maxAge))
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// allowOrigin reports the Access-Control-Allow-Origin value for the request's
// Origin. "*" is always allowed; otherwise the Origin must appear in the list.
func (c CORSConfig) allowOrigin(origin string) (string, bool) {
	for _, o := range c.Origins {
		if o == "*" {
			return "*", true
		}
		if o == origin {
			return o, true
		}
	}
	return "", false
}

// allowedHeaders returns the value for Access-Control-Allow-Headers: the fixed
// set the API uses, plus whatever the preflight requested.
func allowedHeaders(r *http.Request) string {
	set := map[string]bool{
		"content-type":  true,
		"authorization": true,
		"accept":        true,
	}
	for _, h := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			set[strings.ToLower(h)] = true
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
