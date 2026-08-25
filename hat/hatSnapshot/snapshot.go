// Package hatSnapshot defines portable snapshot format and metadata contracts.
package hatSnapshot

import (
	"fmt"
	"strings"
)

type Format string

const (
	FormatBinary         Format = "binary"
	FormatGzipBinary     Format = "gzip-binary"
	FormatGzipBestBinary Format = "gzip-best-binary"
	FormatJSON           Format = "json"
	FormatGzipJSON       Format = "gzip-json"
	FormatGzipBestJSON   Format = "gzip-best-json"
)

// DefaultFormat favors compact snapshots for durable storage and transfer.
const DefaultFormat = FormatGzipBestBinary

// Metadata is the portable information verified before applying a snapshot.
type Metadata struct {
	JournalSequence uint64
}

// ParseFormat canonicalizes supported snapshot format names and aliases.
func ParseFormat(value string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return DefaultFormat, nil
	case string(FormatGzipBestBinary), "best-gzip-binary", "gzip-small-binary", "small-gzip-binary":
		return FormatGzipBestBinary, nil
	case string(FormatGzipBinary), "gzip-bin", "binary.gz", "gzbin":
		return FormatGzipBinary, nil
	case string(FormatBinary), "bin":
		return FormatBinary, nil
	case string(FormatGzipBestJSON), "gzip-best", "best-gzip-json", "gzip-small-json", "small-gzip-json":
		return FormatGzipBestJSON, nil
	case string(FormatGzipJSON), "gzip", "json.gz", "gzjson":
		return FormatGzipJSON, nil
	case string(FormatJSON):
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported snapshot format %q", value)
	}
}
