package hatCommand

import (
	"errors"
	"net/http"
	"testing"
)

func TestProtocolNegotiateHighestCommonVersion(t *testing.T) {
	got, err := NegotiateProtocolVersion(ProtocolVersionRange{Min: 1, Max: 3}, ProtocolVersionRange{Min: 2, Max: 4})
	if err != nil {
		t.Fatalf("NegotiateProtocolVersion() error = %v", err)
	}
	if got != 3 {
		t.Fatalf("NegotiateProtocolVersion() = %d, want 3", got)
	}
}

func TestProtocolRejectsIncompatibleVersionRange(t *testing.T) {
	_, err := NegotiateProtocolVersion(ProtocolVersionRange{Min: 1, Max: 1}, ProtocolVersionRange{Min: 2, Max: 3})
	if !errors.Is(err, ErrIncompatibleProtocolVersion) {
		t.Fatalf("NegotiateProtocolVersion() error = %v, want ErrIncompatibleProtocolVersion", err)
	}
}

func TestProtocolHTTPHeaderNegotiation(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "http://example.test/api/command", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(HeaderProtocolVersion, "1-2")
	response := http.Header{}
	version, err := NegotiateHTTPProtocol(request, response, ProtocolVersionRange{Min: 1, Max: 1})
	if err != nil {
		t.Fatalf("NegotiateHTTPProtocol() error = %v", err)
	}
	if version != 1 {
		t.Fatalf("NegotiateHTTPProtocol() = %d, want 1", version)
	}
	if got := response.Get(HeaderProtocolVersion); got != "1" {
		t.Fatalf("response %s = %q, want 1", HeaderProtocolVersion, got)
	}
	if got := response.Get(HeaderProtocolSupportedVersions); got != "1" {
		t.Fatalf("response %s = %q, want 1", HeaderProtocolSupportedVersions, got)
	}
}

func TestProtocolHTTPHeaderDefaultsToCurrentVersion(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "http://example.test/api/command", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := http.Header{}
	version, err := NegotiateHTTPProtocol(request, response, SupportedProtocolVersions)
	if err != nil {
		t.Fatalf("NegotiateHTTPProtocol() error = %v", err)
	}
	if version != CurrentProtocolVersion {
		t.Fatalf("NegotiateHTTPProtocol() = %d, want current %d", version, CurrentProtocolVersion)
	}
}
