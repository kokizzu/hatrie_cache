// Package hatHash provides deterministic non-cryptographic hashes used by
// compact probabilistic data structures.
package hatHash

const (
	FNVOffset64 uint64 = 14695981039346656037
	FNVPrime64  uint64 = 1099511628211
)

// FNV1a64 calculates the 64-bit FNV-1a hash of value.
func FNV1a64(value []byte) uint64 {
	hash := FNVOffset64
	for _, value := range value {
		hash ^= uint64(value)
		hash *= FNVPrime64
	}
	return hash
}

// FNV1_64 calculates the 64-bit FNV-1 hash of value.
func FNV1_64(value []byte) uint64 {
	hash := FNVOffset64
	for _, value := range value {
		hash *= FNVPrime64
		hash ^= uint64(value)
	}
	return hash
}

// FNV1a64JSONString hashes a JSON string payload without allocating it.
func FNV1a64JSONString(value string) uint64 {
	hash := FNVOffset64
	hash ^= uint64('"')
	hash *= FNVPrime64
	for idx := 0; idx < len(value); idx++ {
		hash ^= uint64(value[idx])
		hash *= FNVPrime64
	}
	hash ^= uint64('"')
	hash *= FNVPrime64
	return hash
}

// FNV1_64JSONString hashes a JSON string payload without allocating it.
func FNV1_64JSONString(value string) uint64 {
	hash := FNVOffset64
	hash *= FNVPrime64
	hash ^= uint64('"')
	for idx := 0; idx < len(value); idx++ {
		hash *= FNVPrime64
		hash ^= uint64(value[idx])
	}
	hash *= FNVPrime64
	hash ^= uint64('"')
	return hash
}
