package hatSql

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"strconv"
)

// SQLIPv4 is a compact, comparable IPv4 value stored in network byte order.
// It occupies four bytes in a struct and sorts by numeric address order.
type SQLIPv4 uint32

// SQLIPv6 is a compact, comparable IPv6 value stored in network byte order.
// Its byte order also matches lexicographic address order.
type SQLIPv6 [16]byte

// ParseSQLIPv4 parses a strict IPv4 address without a prefix length.
func ParseSQLIPv4(value string) (SQLIPv4, error) {
	address, err := netip.ParseAddr(value)
	if err != nil || !address.Is4() {
		if err == nil {
			err = fmt.Errorf("address is not IPv4")
		}
		return 0, fmt.Errorf("parse IPv4 %q: %w", value, err)
	}
	octets := address.As4()
	return SQLIPv4(binary.BigEndian.Uint32(octets[:])), nil
}

// ParseSQLIPv6 parses a strict IPv6 address without a prefix length.
func ParseSQLIPv6(value string) (SQLIPv6, error) {
	address, err := netip.ParseAddr(value)
	if err != nil || !address.Is6() || address.Is4In6() {
		if err == nil {
			err = fmt.Errorf("address is not IPv6")
		}
		return SQLIPv6{}, fmt.Errorf("parse IPv6 %q: %w", value, err)
	}
	return SQLIPv6(address.As16()), nil
}

// String returns the canonical dotted-decimal representation.
func (ip SQLIPv4) String() string {
	var octets [4]byte
	binary.BigEndian.PutUint32(octets[:], uint32(ip))
	return netip.AddrFrom4(octets).String()
}

// String returns the canonical compressed representation.
func (ip SQLIPv6) String() string {
	return netip.AddrFrom16([16]byte(ip)).String()
}

// MarshalJSON keeps the HTTP representation human-readable while the
// in-memory and RowBinary representations stay fixed-width.
func (ip SQLIPv4) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(ip.String())), nil
}

// MarshalJSON keeps the HTTP representation human-readable while the
// in-memory and RowBinary representations stay fixed-width.
func (ip SQLIPv6) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(ip.String())), nil
}

type sqlIPv4 = SQLIPv4
type sqlIPv6 = SQLIPv6
