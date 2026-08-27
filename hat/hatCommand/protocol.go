package hatCommand

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	// HeaderProtocolVersion carries an exact selected version on responses and
	// an accepted version or inclusive version range on requests.
	HeaderProtocolVersion = "X-Hatrie-Protocol-Version"
	// HeaderProtocolSupportedVersions advertises the server's inclusive range.
	HeaderProtocolSupportedVersions = "X-Hatrie-Protocol-Supported"
)

// ProtocolVersion identifies a stable command-wire contract revision.
type ProtocolVersion uint16

const (
	// ProtocolVersion1 is the first explicitly versioned command wire contract.
	ProtocolVersion1 ProtocolVersion = 1
	// CurrentProtocolVersion is used by clients that omit the version header.
	CurrentProtocolVersion = ProtocolVersion1
)

// ProtocolVersionRange is an inclusive range of compatible protocol versions.
type ProtocolVersionRange struct {
	Min ProtocolVersion
	Max ProtocolVersion
}

// SupportedProtocolVersions is the range this build can safely serve.
var SupportedProtocolVersions = ProtocolVersionRange{Min: ProtocolVersion1, Max: CurrentProtocolVersion}

var (
	// ErrInvalidProtocolVersion indicates malformed, zero, or inverted ranges.
	ErrInvalidProtocolVersion = errors.New("hatriecache: invalid protocol version")
	// ErrIncompatibleProtocolVersion indicates client and server ranges do not overlap.
	ErrIncompatibleProtocolVersion = errors.New("hatriecache: incompatible protocol version")
)

// Valid reports whether the range is non-zero and inclusive.
func (versions ProtocolVersionRange) Valid() bool {
	return versions.Min > 0 && versions.Max >= versions.Min
}

// String returns the compact wire representation of the inclusive range.
func (versions ProtocolVersionRange) String() string {
	if versions.Min == versions.Max {
		return strconv.FormatUint(uint64(versions.Min), 10)
	}
	return strconv.FormatUint(uint64(versions.Min), 10) + "-" + strconv.FormatUint(uint64(versions.Max), 10)
}

// ParseProtocolVersionRange parses an exact version ("1") or inclusive range
// ("1-3") used by the HTTP command protocol header.
func ParseProtocolVersionRange(value string) (ProtocolVersionRange, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return ProtocolVersionRange{}, ErrInvalidProtocolVersion
	}
	minText, maxText, ranged := strings.Cut(value, "-")
	if ranged && strings.Contains(maxText, "-") {
		return ProtocolVersionRange{}, fmt.Errorf("%w: %q", ErrInvalidProtocolVersion, value)
	}
	min, err := parseProtocolVersion(minText)
	if err != nil {
		return ProtocolVersionRange{}, err
	}
	if !ranged {
		return ProtocolVersionRange{Min: min, Max: min}, nil
	}
	max, err := parseProtocolVersion(maxText)
	if err != nil {
		return ProtocolVersionRange{}, err
	}
	versions := ProtocolVersionRange{Min: min, Max: max}
	if !versions.Valid() {
		return ProtocolVersionRange{}, fmt.Errorf("%w: %q", ErrInvalidProtocolVersion, value)
	}
	return versions, nil
}

func parseProtocolVersion(value string) (ProtocolVersion, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 16)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidProtocolVersion, value)
	}
	return ProtocolVersion(parsed), nil
}

// NegotiateProtocolVersion returns the highest version supported by both sides.
func NegotiateProtocolVersion(client ProtocolVersionRange, server ProtocolVersionRange) (ProtocolVersion, error) {
	if !client.Valid() || !server.Valid() {
		return 0, ErrInvalidProtocolVersion
	}
	min := client.Min
	if server.Min > min {
		min = server.Min
	}
	max := client.Max
	if server.Max < max {
		max = server.Max
	}
	if max < min {
		return 0, fmt.Errorf("%w: client %s, server %s", ErrIncompatibleProtocolVersion, client, server)
	}
	return max, nil
}

// RequestProtocolVersionRange reads the optional command protocol request
// header. Missing headers retain backward compatibility with the current
// version; malformed values are rejected.
func RequestProtocolVersionRange(request *http.Request) (ProtocolVersionRange, error) {
	if request == nil || strings.TrimSpace(request.Header.Get(HeaderProtocolVersion)) == "" {
		return ProtocolVersionRange{Min: CurrentProtocolVersion, Max: CurrentProtocolVersion}, nil
	}
	return ParseProtocolVersionRange(request.Header.Get(HeaderProtocolVersion))
}

// NegotiateHTTPProtocol negotiates an HTTP request and writes the selected and
// supported versions to the response headers before a response body is sent.
func NegotiateHTTPProtocol(request *http.Request, response http.Header, server ProtocolVersionRange) (ProtocolVersion, error) {
	client, err := RequestProtocolVersionRange(request)
	if err != nil {
		return 0, err
	}
	version, err := NegotiateProtocolVersion(client, server)
	if err != nil {
		return 0, err
	}
	if response != nil {
		response.Set(HeaderProtocolVersion, strconv.FormatUint(uint64(version), 10))
		response.Set(HeaderProtocolSupportedVersions, server.String())
		addProtocolVaryHeader(response)
	}
	return version, nil
}

func addProtocolVaryHeader(headers http.Header) {
	for _, value := range headers.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), HeaderProtocolVersion) {
				return
			}
		}
	}
	headers.Add("Vary", HeaderProtocolVersion)
}
