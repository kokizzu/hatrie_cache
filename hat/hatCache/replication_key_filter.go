package hatCache

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
)

const (
	replicationKeyPrefixesMetadata     = "replication_key_prefixes"
	replicationKeyPrefixExcludedReason = "key excluded by replication prefix policy"
	maxReplicationKeyPrefixes          = 64
	maxReplicationKeyPrefixBytes       = 4096
	replicationKeyPrefixesWireVersion  = 1
)

func normalizeReplicationKeyPrefixes(prefixes []string) []string {
	if len(prefixes) == 0 {
		return nil
	}
	capacity := len(prefixes)
	if capacity > maxReplicationKeyPrefixes {
		capacity = maxReplicationKeyPrefixes
	}
	result := make([]string, 0, capacity)
	seen := make(map[string]struct{}, len(prefixes))
	totalBytes := 0
	for _, prefix := range prefixes {
		if prefix == "" {
			continue
		}
		if _, exists := seen[prefix]; exists {
			continue
		}
		if len(result) >= maxReplicationKeyPrefixes || totalBytes+len(prefix) > maxReplicationKeyPrefixBytes {
			break
		}
		seen[prefix] = struct{}{}
		result = append(result, prefix)
		totalBytes += len(prefix)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func replicationKeyAllowedForPrefixes(prefixes []string, key string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (replicator *HTTPReplicator) replicationKeyAllowed(key string) bool {
	if replicator == nil {
		return false
	}
	return replicationKeyAllowedForPrefixes(replicator.keyPrefixes, key)
}

func encodeReplicationKeyPrefixes(prefixes []string) string {
	if len(prefixes) == 0 {
		return ""
	}
	data := make([]byte, 0, 1+binary.MaxVarintLen64*(len(prefixes)+1)+maxReplicationKeyPrefixBytes)
	data = append(data, replicationKeyPrefixesWireVersion)
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], uint64(len(prefixes)))
	data = append(data, encoded[:n]...)
	for _, prefix := range prefixes {
		n = binary.PutUvarint(encoded[:], uint64(len(prefix)))
		data = append(data, encoded[:n]...)
		data = append(data, prefix...)
	}
	return base64.RawStdEncoding.EncodeToString(data)
}

func decodeReplicationKeyPrefixes(encoded string) ([]string, error) {
	if encoded == "" {
		return nil, nil
	}
	data, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil || len(data) < 1 || data[0] != replicationKeyPrefixesWireVersion {
		return nil, errors.New("hatriecache: invalid replication key prefixes")
	}
	data = data[1:]
	count, size := binary.Uvarint(data)
	if size <= 0 || count > maxReplicationKeyPrefixes {
		return nil, errors.New("hatriecache: invalid replication key prefix count")
	}
	data = data[size:]
	prefixes := make([]string, 0, int(count))
	totalBytes := 0
	for index := uint64(0); index < count; index++ {
		length, size := binary.Uvarint(data)
		if size <= 0 || length == 0 || length > maxReplicationKeyPrefixBytes || length > uint64(len(data)-size) {
			return nil, errors.New("hatriecache: invalid replication key prefix")
		}
		data = data[size:]
		if totalBytes+int(length) > maxReplicationKeyPrefixBytes {
			return nil, errors.New("hatriecache: replication key prefixes are too large")
		}
		prefixes = append(prefixes, string(data[:length]))
		totalBytes += int(length)
		data = data[length:]
	}
	if len(data) != 0 {
		return nil, errors.New("hatriecache: trailing replication key prefixes")
	}
	return normalizeReplicationKeyPrefixes(prefixes), nil
}
