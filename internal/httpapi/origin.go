package httpapi

import (
	"net/http"
	"strings"
)

func sameOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" && origin != "http://"+r.Host && origin != "https://"+r.Host {
		writeError(w, 403, "Cross-origin changes are not allowed.")
		return false
	}
	if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
		writeError(w, 403, "Cross-site changes are not allowed.")
		return false
	}
	return true
}
