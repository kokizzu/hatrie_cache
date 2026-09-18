package hatSchema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	// SchemaDiscoveryWireVersion identifies the binary discovery frame layout.
	SchemaDiscoveryWireVersion uint8 = 1
	// DefaultSchemaDiscoveryProtocolVersion is the initial application protocol
	// version advertised by callers that do not need a custom version.
	DefaultSchemaDiscoveryProtocolVersion uint64 = 1
	// SchemaDiscoveryMaxWireBytes bounds one untrusted discovery frame.
	SchemaDiscoveryMaxWireBytes = 64 << 10
	// SchemaDiscoveryMaxDDLCapabilities bounds the number of advertised DDL
	// operations in one discovery frame.
	SchemaDiscoveryMaxDDLCapabilities = 128
	// SchemaDiscoveryMaxStringBytes bounds one fingerprint or DDL capability.
	SchemaDiscoveryMaxStringBytes = 256
)

const schemaDiscoveryMagic = "HSD1"

// SchemaDiscovery is the compact control-plane description exchanged before
// a client or replica sends schema-dependent data. DDL capabilities are
// caller-defined stable tokens such as "add_nullable_column".
type SchemaDiscovery struct {
	ProtocolVersion   uint64   `json:"protocol_version"`
	SchemaVersion     uint64   `json:"schema_version"`
	SchemaFingerprint string   `json:"schema_fingerprint"`
	DDLCapabilities   []string `json:"ddl_capabilities,omitempty"`
}

// SchemaDiscoveryReport describes whether two endpoints can use the same
// schema-dependent protocol. NeedsSchemaTransfer distinguishes a schema drift
// from a protocol or capability incompatibility.
type SchemaDiscoveryReport struct {
	Compatible             bool     `json:"compatible"`
	ProtocolMatch          bool     `json:"protocol_match"`
	SchemaMatch            bool     `json:"schema_match"`
	NeedsSchemaTransfer    bool     `json:"needs_schema_transfer"`
	MissingDDLCapabilities []string `json:"missing_ddl_capabilities,omitempty"`
}

// NewSchemaDiscovery validates a schema and creates a canonical discovery
// record. Capabilities are copied, duplicate-checked, and sorted so the
// binary representation is deterministic.
func NewSchemaDiscovery(schema Schema, protocolVersion uint64, ddlCapabilities []string) (SchemaDiscovery, error) {
	if protocolVersion == 0 {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: schema discovery protocol version must be positive")
	}
	if err := schema.Validate(); err != nil {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: schema discovery schema: %w", err)
	}
	normalized, err := normalizeSchemaDiscoveryCapabilities(ddlCapabilities)
	if err != nil {
		return SchemaDiscovery{}, err
	}
	discovery := SchemaDiscovery{
		ProtocolVersion:   protocolVersion,
		SchemaVersion:     schema.Version,
		SchemaFingerprint: schema.Fingerprint(),
		DDLCapabilities:   normalized,
	}
	if err := discovery.Validate(); err != nil {
		return SchemaDiscovery{}, err
	}
	return discovery, nil
}

// Validate checks bounds and canonical ordering before a discovery record is
// used on a wire or in a comparison.
func (discovery SchemaDiscovery) Validate() error {
	if discovery.ProtocolVersion == 0 {
		return fmt.Errorf("hatSchema: schema discovery protocol version must be positive")
	}
	if discovery.SchemaFingerprint == "" {
		return fmt.Errorf("hatSchema: schema discovery fingerprint is required")
	}
	if err := validateSchemaDiscoveryString("schema discovery fingerprint", discovery.SchemaFingerprint); err != nil {
		return err
	}
	if len(discovery.DDLCapabilities) > SchemaDiscoveryMaxDDLCapabilities {
		return fmt.Errorf("hatSchema: schema discovery has %d DDL capabilities, maximum is %d", len(discovery.DDLCapabilities), SchemaDiscoveryMaxDDLCapabilities)
	}
	previous := ""
	for index, capability := range discovery.DDLCapabilities {
		if err := validateSchemaDiscoveryString("schema discovery DDL capability", capability); err != nil {
			return fmt.Errorf("hatSchema: capability %d: %w", index, err)
		}
		if index > 0 && capability <= previous {
			return fmt.Errorf("hatSchema: schema discovery DDL capabilities must be strictly sorted and unique")
		}
		previous = capability
	}
	return nil
}

// SupportsDDLCapability reports whether the discovery record advertises one
// caller-defined DDL operation.
func (discovery SchemaDiscovery) SupportsDDLCapability(capability string) bool {
	if err := discovery.Validate(); err != nil {
		return false
	}
	capability = strings.TrimSpace(capability)
	if capability == "" || !utf8.ValidString(capability) {
		return false
	}
	index := sort.SearchStrings(discovery.DDLCapabilities, capability)
	return index < len(discovery.DDLCapabilities) && discovery.DDLCapabilities[index] == capability
}

// UnmarshalBinary decodes one complete HSD1 frame into discovery.
func (discovery *SchemaDiscovery) UnmarshalBinary(frame []byte) error {
	if discovery == nil {
		return fmt.Errorf("hatSchema: schema discovery receiver is nil")
	}
	decoded, err := DecodeSchemaDiscovery(frame)
	if err != nil {
		return err
	}
	*discovery = decoded
	return nil
}

// MarshalBinary encodes a bounded HSD1 discovery frame. The frame contains a
// wire version, protocol/schema versions, one fingerprint, and sorted DDL
// capability strings using unsigned varints for numeric fields.
func (discovery SchemaDiscovery) MarshalBinary() ([]byte, error) {
	if err := discovery.Validate(); err != nil {
		return nil, err
	}
	capacity := len(schemaDiscoveryMagic) + 1 + 10 + 10 + 10 + len(discovery.SchemaFingerprint) + 10
	for _, capability := range discovery.DDLCapabilities {
		capacity += 10 + len(capability)
	}
	frame := make([]byte, 0, capacity)
	frame = append(frame, schemaDiscoveryMagic...)
	frame = append(frame, SchemaDiscoveryWireVersion)
	frame = appendSchemaDiscoveryUvarint(frame, discovery.ProtocolVersion)
	frame = appendSchemaDiscoveryUvarint(frame, discovery.SchemaVersion)
	frame = appendSchemaDiscoveryString(frame, discovery.SchemaFingerprint)
	frame = appendSchemaDiscoveryUvarint(frame, uint64(len(discovery.DDLCapabilities)))
	for _, capability := range discovery.DDLCapabilities {
		frame = appendSchemaDiscoveryString(frame, capability)
	}
	if len(frame) > SchemaDiscoveryMaxWireBytes {
		return nil, fmt.Errorf("hatSchema: schema discovery frame is %d bytes, maximum is %d", len(frame), SchemaDiscoveryMaxWireBytes)
	}
	return frame, nil
}

// DecodeSchemaDiscovery decodes and validates one complete HSD1 frame. It
// rejects truncation, trailing bytes, duplicate capabilities, and oversized
// lengths before allocating based on untrusted input.
func DecodeSchemaDiscovery(frame []byte) (SchemaDiscovery, error) {
	if len(frame) > SchemaDiscoveryMaxWireBytes {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: schema discovery frame is %d bytes, maximum is %d", len(frame), SchemaDiscoveryMaxWireBytes)
	}
	if len(frame) < len(schemaDiscoveryMagic)+1 || !bytes.Equal(frame[:len(schemaDiscoveryMagic)], []byte(schemaDiscoveryMagic)) {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: invalid schema discovery frame magic")
	}
	offset := len(schemaDiscoveryMagic)
	if frame[offset] != SchemaDiscoveryWireVersion {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: unsupported schema discovery wire version %d", frame[offset])
	}
	offset++
	protocolVersion, err := readSchemaDiscoveryUvarint(frame, &offset)
	if err != nil {
		return SchemaDiscovery{}, err
	}
	schemaVersion, err := readSchemaDiscoveryUvarint(frame, &offset)
	if err != nil {
		return SchemaDiscovery{}, err
	}
	fingerprint, err := readSchemaDiscoveryString(frame, &offset, "fingerprint")
	if err != nil {
		return SchemaDiscovery{}, err
	}
	capabilityCount, err := readSchemaDiscoveryUvarint(frame, &offset)
	if err != nil {
		return SchemaDiscovery{}, err
	}
	if capabilityCount > SchemaDiscoveryMaxDDLCapabilities {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: schema discovery has %d DDL capabilities, maximum is %d", capabilityCount, SchemaDiscoveryMaxDDLCapabilities)
	}
	capabilities := make([]string, 0, int(capabilityCount))
	for index := uint64(0); index < capabilityCount; index++ {
		capability, err := readSchemaDiscoveryString(frame, &offset, "DDL capability")
		if err != nil {
			return SchemaDiscovery{}, err
		}
		capabilities = append(capabilities, capability)
	}
	if offset != len(frame) {
		return SchemaDiscovery{}, fmt.Errorf("hatSchema: schema discovery frame has %d trailing bytes", len(frame)-offset)
	}
	discovery := SchemaDiscovery{
		ProtocolVersion:   protocolVersion,
		SchemaVersion:     schemaVersion,
		SchemaFingerprint: fingerprint,
		DDLCapabilities:   capabilities,
	}
	if err := discovery.Validate(); err != nil {
		return SchemaDiscovery{}, err
	}
	return discovery, nil
}

// CompareSchemaDiscovery compares local and peer metadata. requiredDDL lists
// operations the peer must advertise; it is normalized and never retained by
// the returned report.
func CompareSchemaDiscovery(local, peer SchemaDiscovery, requiredDDL []string) (SchemaDiscoveryReport, error) {
	if err := local.Validate(); err != nil {
		return SchemaDiscoveryReport{}, fmt.Errorf("hatSchema: local discovery: %w", err)
	}
	if err := peer.Validate(); err != nil {
		return SchemaDiscoveryReport{}, fmt.Errorf("hatSchema: peer discovery: %w", err)
	}
	required, err := normalizeSchemaDiscoveryCapabilities(requiredDDL)
	if err != nil {
		return SchemaDiscoveryReport{}, fmt.Errorf("hatSchema: required DDL capabilities: %w", err)
	}
	missing := make([]string, 0, len(required))
	for _, capability := range required {
		if !peer.SupportsDDLCapability(capability) {
			missing = append(missing, capability)
		}
	}
	report := SchemaDiscoveryReport{
		ProtocolMatch:          local.ProtocolVersion == peer.ProtocolVersion,
		SchemaMatch:            local.SchemaVersion == peer.SchemaVersion && local.SchemaFingerprint == peer.SchemaFingerprint,
		MissingDDLCapabilities: missing,
	}
	report.NeedsSchemaTransfer = report.ProtocolMatch && !report.SchemaMatch
	report.Compatible = report.ProtocolMatch && report.SchemaMatch && len(report.MissingDDLCapabilities) == 0
	return report, nil
}

func normalizeSchemaDiscoveryCapabilities(capabilities []string) ([]string, error) {
	if len(capabilities) > SchemaDiscoveryMaxDDLCapabilities {
		return nil, fmt.Errorf("hatSchema: schema discovery has %d DDL capabilities, maximum is %d", len(capabilities), SchemaDiscoveryMaxDDLCapabilities)
	}
	normalized := make([]string, len(capabilities))
	for index, capability := range capabilities {
		capability = strings.TrimSpace(capability)
		if err := validateSchemaDiscoveryString("schema discovery DDL capability", capability); err != nil {
			return nil, fmt.Errorf("hatSchema: capability %d: %w", index, err)
		}
		normalized[index] = capability
	}
	sort.Strings(normalized)
	for index := 1; index < len(normalized); index++ {
		if normalized[index] == normalized[index-1] {
			return nil, fmt.Errorf("hatSchema: duplicate schema discovery DDL capability %q", normalized[index])
		}
	}
	return normalized, nil
}

func validateSchemaDiscoveryString(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if len(value) > SchemaDiscoveryMaxStringBytes {
		return fmt.Errorf("%s is %d bytes, maximum is %d", label, len(value), SchemaDiscoveryMaxStringBytes)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have leading or trailing whitespace", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", label)
	}
	return nil
}

func appendSchemaDiscoveryUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendSchemaDiscoveryString(dst []byte, value string) []byte {
	dst = appendSchemaDiscoveryUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readSchemaDiscoveryUvarint(frame []byte, offset *int) (uint64, error) {
	if *offset >= len(frame) {
		return 0, fmt.Errorf("hatSchema: truncated schema discovery varint")
	}
	value, count := binary.Uvarint(frame[*offset:])
	if count == 0 {
		return 0, fmt.Errorf("hatSchema: truncated schema discovery varint")
	}
	if count < 0 {
		return 0, fmt.Errorf("hatSchema: schema discovery varint overflows uint64")
	}
	*offset += count
	return value, nil
}

func readSchemaDiscoveryString(frame []byte, offset *int, label string) (string, error) {
	length, err := readSchemaDiscoveryUvarint(frame, offset)
	if err != nil {
		return "", err
	}
	if length > SchemaDiscoveryMaxStringBytes {
		return "", fmt.Errorf("hatSchema: schema discovery %s is %d bytes, maximum is %d", label, length, SchemaDiscoveryMaxStringBytes)
	}
	if length > uint64(len(frame)-*offset) {
		return "", fmt.Errorf("hatSchema: truncated schema discovery %s", label)
	}
	end := *offset + int(length)
	value := string(frame[*offset:end])
	*offset = end
	if err := validateSchemaDiscoveryString("schema discovery "+label, value); err != nil {
		return "", err
	}
	return value, nil
}
