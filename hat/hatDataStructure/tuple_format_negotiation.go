package hatDataStructure

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	// MaxTupleFormatCapabilities bounds one capability advertisement.
	MaxTupleFormatCapabilities = 1 << 10
	// MaxTupleFormatCapabilityNameBytes bounds one logical format name.
	MaxTupleFormatCapabilityNameBytes = 256
	// MaxTupleFormatCapabilityWireBytes bounds an encoded capability advertisement.
	MaxTupleFormatCapabilityWireBytes = 1 << 20

	tupleFormatCapabilityWireVersion byte = 1
)

var (
	// ErrTupleFormatCapabilityInvalid indicates a malformed capability record.
	ErrTupleFormatCapabilityInvalid = errors.New("hatDataStructure: tuple format capability is invalid")
	// ErrTupleFormatCapabilityLimit indicates that a capability advertisement is
	// larger than the configured bound.
	ErrTupleFormatCapabilityLimit = errors.New("hatDataStructure: tuple format capability limit exceeded")
	// ErrTupleFormatCapabilityWire indicates malformed or unsupported wire data.
	ErrTupleFormatCapabilityWire = errors.New("hatDataStructure: tuple format capability wire is invalid")
	// ErrTupleFormatNoCompatibleVersion indicates that the peers have no exact
	// common version and schema shape.
	ErrTupleFormatNoCompatibleVersion = errors.New("hatDataStructure: no compatible tuple format version")
)

var tupleFormatCapabilityMagic = [4]byte{'H', 'T', 'F', '1'}

// TupleFormatFingerprint identifies the ordered physical field shape of a
// tuple format. The schema version is intentionally excluded so that an
// unchanged shape can be advertised at more than one version.
type TupleFormatFingerprint [16]byte

// String returns the lowercase hexadecimal fingerprint, useful in logs and
// diagnostics.
func (fingerprint TupleFormatFingerprint) String() string {
	return hex.EncodeToString(fingerprint[:])
}

// TupleFormatCapability advertises one version of a named tuple format.
// Version and Fingerprint must both match on the two peers before a format is
// selected.
type TupleFormatCapability struct {
	Name        string                 `json:"name"`
	Version     uint64                 `json:"version"`
	Fingerprint TupleFormatFingerprint `json:"fingerprint"`
}

// Fingerprint returns the stable physical field-shape fingerprint for format.
// Defaults and generated-field callbacks are excluded because they are local
// value-resolution behavior rather than tuple wire layout.
func (format TupleFormat) Fingerprint() (TupleFormatFingerprint, error) {
	if err := format.validateDefinition(); err != nil {
		return TupleFormatFingerprint{}, err
	}

	hasher := sha256.New()
	_, _ = hasher.Write([]byte("hatrie_tuple_format_shape_v1"))
	writeUint := func(value uint64) {
		var encoded [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(encoded[:], value)
		_, _ = hasher.Write(encoded[:n])
	}
	writeUint(uint64(len(format.fields)))
	for _, field := range format.fields {
		writeUint(uint64(len(field.Name)))
		_, _ = hasher.Write([]byte(field.Name))
		_, _ = hasher.Write([]byte{byte(field.Type)})
		if field.Nullable {
			_, _ = hasher.Write([]byte{1})
		} else {
			_, _ = hasher.Write([]byte{0})
		}
	}

	digest := hasher.Sum(nil)
	var fingerprint TupleFormatFingerprint
	copy(fingerprint[:], digest[:len(fingerprint)])
	return fingerprint, nil
}

// Capability creates a validated advertisement for one format version.
func (format TupleFormat) Capability(name string) (TupleFormatCapability, error) {
	if err := format.validateDefinition(); err != nil {
		return TupleFormatCapability{}, err
	}
	name = strings.TrimSpace(name)
	if err := validateTupleFormatCapabilityName(name); err != nil {
		return TupleFormatCapability{}, err
	}
	fingerprint, err := format.Fingerprint()
	if err != nil {
		return TupleFormatCapability{}, err
	}
	return TupleFormatCapability{Name: name, Version: format.version, Fingerprint: fingerprint}, nil
}

// NegotiateTupleFormat chooses the highest exact common version and shape from
// two bounded capability advertisements. Selection is deterministic even when
// multiple named formats are shared: version is preferred first, then name,
// then fingerprint bytes.
func NegotiateTupleFormat(local, remote []TupleFormatCapability) (TupleFormatCapability, error) {
	if err := validateTupleFormatCapabilities(local); err != nil {
		return TupleFormatCapability{}, err
	}
	if err := validateTupleFormatCapabilities(remote); err != nil {
		return TupleFormatCapability{}, err
	}

	remoteSet := make(map[tupleFormatCapabilityKey]struct{}, len(remote))
	for _, capability := range remote {
		remoteSet[tupleFormatCapabilityKeyOf(capability)] = struct{}{}
	}

	var chosen TupleFormatCapability
	found := false
	for _, capability := range local {
		if _, ok := remoteSet[tupleFormatCapabilityKeyOf(capability)]; !ok {
			continue
		}
		if !found || tupleFormatCapabilityPreferred(capability, chosen) {
			chosen = capability
			found = true
		}
	}
	if !found {
		return TupleFormatCapability{}, ErrTupleFormatNoCompatibleVersion
	}
	return chosen, nil
}

// MarshalTupleFormatCapabilities encodes capabilities in deterministic compact
// HTF1 form for transfer over a peer protocol. The input slice is not mutated.
func MarshalTupleFormatCapabilities(capabilities []TupleFormatCapability) ([]byte, error) {
	if err := validateTupleFormatCapabilities(capabilities); err != nil {
		return nil, err
	}
	ordered := capabilities
	if !tupleFormatCapabilitiesSorted(capabilities) {
		ordered = append([]TupleFormatCapability(nil), capabilities...)
		sort.Slice(ordered, func(left, right int) bool {
			return tupleFormatCapabilityLess(ordered[left], ordered[right])
		})
	}

	encoded := make([]byte, 0, tupleFormatCapabilityWireCapacity(ordered))
	encoded = append(encoded, tupleFormatCapabilityMagic[:]...)
	encoded = append(encoded, tupleFormatCapabilityWireVersion)
	encoded = appendTupleFormatUvarint(encoded, uint64(len(ordered)))
	for _, capability := range ordered {
		encoded = appendTupleFormatUvarint(encoded, uint64(len(capability.Name)))
		encoded = append(encoded, capability.Name...)
		encoded = appendTupleFormatUvarint(encoded, capability.Version)
		encoded = append(encoded, capability.Fingerprint[:]...)
		if len(encoded) > MaxTupleFormatCapabilityWireBytes {
			return nil, ErrTupleFormatCapabilityLimit
		}
	}
	return encoded, nil
}

// UnmarshalTupleFormatCapabilities decodes and strictly validates HTF1
// capability data. It rejects trailing bytes, duplicate versions, oversized
// names, unknown wire versions, and truncated varints before returning data.
func UnmarshalTupleFormatCapabilities(data []byte) ([]TupleFormatCapability, error) {
	if len(data) > MaxTupleFormatCapabilityWireBytes || len(data) < len(tupleFormatCapabilityMagic)+1 {
		return nil, tupleFormatWireError("payload length %d is outside bounds", len(data))
	}
	if !bytes.Equal(data[:len(tupleFormatCapabilityMagic)], tupleFormatCapabilityMagic[:]) {
		return nil, tupleFormatWireError("magic is not HTF1")
	}
	if data[len(tupleFormatCapabilityMagic)] != tupleFormatCapabilityWireVersion {
		return nil, tupleFormatWireError("wire version %d is unsupported", data[len(tupleFormatCapabilityMagic)])
	}

	offset := len(tupleFormatCapabilityMagic) + 1
	count, next, err := readTupleFormatUvarint(data, offset)
	if err != nil {
		return nil, err
	}
	if count > MaxTupleFormatCapabilities {
		return nil, tupleFormatWireError("capability count %d exceeds %d", count, MaxTupleFormatCapabilities)
	}
	offset = next
	capabilities := make([]TupleFormatCapability, 0, int(count))
	seen := make(map[tupleFormatCapabilityVersionKey]struct{}, int(count))
	for index := uint64(0); index < count; index++ {
		nameLength, next, err := readTupleFormatUvarint(data, offset)
		if err != nil {
			return nil, err
		}
		if nameLength == 0 || nameLength > MaxTupleFormatCapabilityNameBytes {
			return nil, tupleFormatWireError("capability %d name length %d is invalid", index, nameLength)
		}
		offset = next
		if nameLength > uint64(len(data)-offset) {
			return nil, tupleFormatWireError("capability %d name is truncated", index)
		}
		name := string(data[offset : offset+int(nameLength)])
		offset += int(nameLength)

		version, next, err := readTupleFormatUvarint(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if len(data)-offset < len(TupleFormatFingerprint{}) {
			return nil, tupleFormatWireError("capability %d fingerprint is truncated", index)
		}
		var fingerprint TupleFormatFingerprint
		copy(fingerprint[:], data[offset:offset+len(fingerprint)])
		offset += len(fingerprint)

		capability := TupleFormatCapability{Name: name, Version: version, Fingerprint: fingerprint}
		if err := validateTupleFormatCapability(capability); err != nil {
			return nil, tupleFormatWireError("capability %d: %v", index, err)
		}
		key := tupleFormatCapabilityVersionKey{Name: capability.Name, Version: capability.Version}
		if _, exists := seen[key]; exists {
			return nil, tupleFormatWireError("capability %d duplicates %q version %d", index, capability.Name, capability.Version)
		}
		seen[key] = struct{}{}
		capabilities = append(capabilities, capability)
	}
	if offset != len(data) {
		return nil, tupleFormatWireError("trailing bytes: got %d extra bytes", len(data)-offset)
	}
	return capabilities, nil
}

type tupleFormatCapabilityVersionKey struct {
	Name    string
	Version uint64
}

type tupleFormatCapabilityKey struct {
	Name        string
	Version     uint64
	Fingerprint TupleFormatFingerprint
}

func tupleFormatCapabilityKeyOf(capability TupleFormatCapability) tupleFormatCapabilityKey {
	return tupleFormatCapabilityKey{
		Name:        capability.Name,
		Version:     capability.Version,
		Fingerprint: capability.Fingerprint,
	}
}

func validateTupleFormatCapabilities(capabilities []TupleFormatCapability) error {
	if len(capabilities) > MaxTupleFormatCapabilities {
		return fmt.Errorf("%w: got %d, maximum %d", ErrTupleFormatCapabilityLimit, len(capabilities), MaxTupleFormatCapabilities)
	}
	seen := make(map[tupleFormatCapabilityVersionKey]struct{}, len(capabilities))
	for index, capability := range capabilities {
		if err := validateTupleFormatCapability(capability); err != nil {
			return fmt.Errorf("%w at index %d: %v", ErrTupleFormatCapabilityInvalid, index, err)
		}
		key := tupleFormatCapabilityVersionKey{Name: capability.Name, Version: capability.Version}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w at index %d: duplicate %q version %d", ErrTupleFormatCapabilityInvalid, index, capability.Name, capability.Version)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTupleFormatCapability(capability TupleFormatCapability) error {
	if err := validateTupleFormatCapabilityName(capability.Name); err != nil {
		return err
	}
	if capability.Version == 0 {
		return errors.New("version must be positive")
	}
	if capability.Fingerprint == (TupleFormatFingerprint{}) {
		return errors.New("fingerprint must not be zero")
	}
	return nil
}

func validateTupleFormatCapabilityName(name string) error {
	if name == "" {
		return errors.New("name is required")
	}
	if len(name) > MaxTupleFormatCapabilityNameBytes {
		return fmt.Errorf("name length %d exceeds %d", len(name), MaxTupleFormatCapabilityNameBytes)
	}
	if strings.TrimSpace(name) != name {
		return errors.New("name must be trimmed")
	}
	if !utf8.ValidString(name) {
		return errors.New("name must be valid UTF-8")
	}
	return nil
}

func tupleFormatCapabilityPreferred(candidate, current TupleFormatCapability) bool {
	if candidate.Version != current.Version {
		return candidate.Version > current.Version
	}
	if candidate.Name != current.Name {
		return candidate.Name < current.Name
	}
	return bytes.Compare(candidate.Fingerprint[:], current.Fingerprint[:]) < 0
}

func tupleFormatCapabilityLess(left, right TupleFormatCapability) bool {
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	if left.Version != right.Version {
		return left.Version < right.Version
	}
	return bytes.Compare(left.Fingerprint[:], right.Fingerprint[:]) < 0
}

func tupleFormatCapabilitiesSorted(capabilities []TupleFormatCapability) bool {
	for index := 1; index < len(capabilities); index++ {
		if tupleFormatCapabilityLess(capabilities[index], capabilities[index-1]) {
			return false
		}
	}
	return true
}

func tupleFormatCapabilityWireCapacity(capabilities []TupleFormatCapability) int {
	capacity := len(tupleFormatCapabilityMagic) + 1 + binary.MaxVarintLen64
	for _, capability := range capabilities {
		capacity += binary.MaxVarintLen64 + len(capability.Name) + binary.MaxVarintLen64 + len(capability.Fingerprint)
		if capacity >= MaxTupleFormatCapabilityWireBytes {
			return MaxTupleFormatCapabilityWireBytes
		}
	}
	return capacity
}

func appendTupleFormatUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func readTupleFormatUvarint(data []byte, offset int) (value uint64, next int, err error) {
	if offset < 0 || offset >= len(data) {
		return 0, 0, tupleFormatWireError("missing varint at offset %d", offset)
	}
	value, size := binary.Uvarint(data[offset:])
	if size <= 0 {
		return 0, 0, tupleFormatWireError("invalid varint at offset %d", offset)
	}
	return value, offset + size, nil
}

func tupleFormatWireError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrTupleFormatCapabilityWire, fmt.Sprintf(format, args...))
}
