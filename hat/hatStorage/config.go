package hatStorage

import (
	"fmt"
	"strings"
)

// Backend selects a local persistent key/value engine.
type Backend string

const (
	BackendAuto    Backend = "auto"
	BackendPebble  Backend = "pebble"
	BackendLevelDB Backend = "leveldb"
)

// DefaultBackend is selected for an empty new data path.
const DefaultBackend = BackendPebble

// Format selects the on-disk record encoding used by a persistent store.
type Format string

const (
	FormatJSON   Format = "json"
	FormatBinary Format = "binary"
)

// DefaultFormat is the compact binary record encoding.
const DefaultFormat = FormatBinary

// ParseBackend validates and canonicalizes a persistent-store backend name.
func ParseBackend(value string) (Backend, error) {
	switch Backend(strings.ToLower(strings.TrimSpace(value))) {
	case "", BackendAuto:
		return BackendAuto, nil
	case BackendPebble:
		return BackendPebble, nil
	case BackendLevelDB:
		return BackendLevelDB, nil
	default:
		return "", fmt.Errorf("hatriecache: storage backend must be auto, pebble, or leveldb")
	}
}

// ParseFormat validates and canonicalizes a persistent-store record format.
func ParseFormat(value string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(FormatBinary), "bin":
		return FormatBinary, nil
	case string(FormatJSON):
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported storage format %q", value)
	}
}
