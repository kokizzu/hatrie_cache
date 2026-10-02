package hatStorage

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
)

// VerifyRemotePartChecksum verifies the checksum format emitted by the
// built-in remote multipart uploader. Other caller-defined checksum schemes
// remain valid when RemotePartCacheOptions.VerifyChecksums is disabled.
func VerifyRemotePartChecksum(data []byte, expected string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(expected, prefix) || len(expected)-len(prefix) != sha256.Size*2 {
		return ErrRemotePartCacheChecksumUnsupported
	}
	encoded := expected[len(prefix):]
	var wanted [sha256.Size]byte
	if !decodeRemotePartChecksumHex(wanted[:], encoded) {
		return ErrRemotePartCacheChecksumUnsupported
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare(digest[:], wanted[:]) != 1 {
		return ErrRemotePartCacheChecksumMismatch
	}
	return nil
}

func decodeRemotePartChecksumHex(dst []byte, encoded string) bool {
	if len(dst) != sha256.Size || len(encoded) != sha256.Size*2 {
		return false
	}
	for i := range dst {
		high, ok := remotePartChecksumHexValue(encoded[i*2])
		if !ok {
			return false
		}
		low, ok := remotePartChecksumHexValue(encoded[i*2+1])
		if !ok {
			return false
		}
		dst[i] = high<<4 | low
	}
	return true
}

func remotePartChecksumHexValue(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}
