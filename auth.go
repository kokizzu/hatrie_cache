package hatriecache

import (
	"crypto/subtle"
	"strings"
)

const authBearerPrefix = "Bearer "

func normalizeAuthToken(token string) string {
	return strings.TrimSpace(token)
}

func authTokenMatches(candidate string, token string) bool {
	token = normalizeAuthToken(token)
	if token == "" {
		return true
	}
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1
}

func authBearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < len(authBearerPrefix) || !strings.EqualFold(value[:len(authBearerPrefix)], authBearerPrefix) {
		return ""
	}
	return strings.TrimSpace(value[len(authBearerPrefix):])
}
