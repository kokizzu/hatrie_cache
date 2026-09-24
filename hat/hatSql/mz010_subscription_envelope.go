package hatSql

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	// SQLSubscriptionWireEnvelopeVersion identifies the current binary format.
	SQLSubscriptionWireEnvelopeVersion byte = 1

	// SQLSubscriptionWireSignatureBytes is the size of the HMAC-SHA256 suffix.
	SQLSubscriptionWireSignatureBytes = sha256.Size

	// MaxSQLSubscriptionWireSubscriptionIDBytes bounds untrusted subscription
	// labels before they are copied into a decoded envelope.
	MaxSQLSubscriptionWireSubscriptionIDBytes = 256
	// MaxSQLSubscriptionWirePayloadBytes bounds one signed result payload.
	MaxSQLSubscriptionWirePayloadBytes = 16 << 20
	// MaxSQLSubscriptionWireKeyBytes bounds a configured signing key supplied
	// by callers so accidental oversized keys cannot amplify signing work.
	MaxSQLSubscriptionWireKeyBytes = 4096

	sqlSubscriptionWireHeaderBytes = 4 + 1 + 1 + 2 + 8 + 8 + 4
	sqlSubscriptionWireMagic       = "HSE1"

	// MaxSQLSubscriptionWireEnvelopeBytes includes the fixed header, maximum
	// identifier, maximum payload, and authentication suffix.
	MaxSQLSubscriptionWireEnvelopeBytes = sqlSubscriptionWireHeaderBytes + MaxSQLSubscriptionWireSubscriptionIDBytes + MaxSQLSubscriptionWirePayloadBytes + SQLSubscriptionWireSignatureBytes
)

var (
	// ErrSQLSubscriptionWireEnvelopeInvalid reports malformed or unsupported
	// envelope input before or after authentication.
	ErrSQLSubscriptionWireEnvelopeInvalid = errors.New("hatSql: invalid SQL subscription wire envelope")
	// ErrSQLSubscriptionWireEnvelopeAuthentication reports a failed HMAC check.
	ErrSQLSubscriptionWireEnvelopeAuthentication = errors.New("hatSql: SQL subscription wire envelope authentication failed")
)

// SQLSubscriptionWireEnvelope is the authenticated payload exchanged by an
// opt-in SQL subscription transport. Payload is application-defined bytes,
// commonly a JSON or native row batch; this package does not reinterpret it.
type SQLSubscriptionWireEnvelope struct {
	Mode           SQLSubscriptionMode
	SubscriptionID string
	Sequence       uint64
	Diff           int64
	Payload        []byte
}

// SealSQLSubscriptionWireEnvelope encodes and authenticates one envelope with
// HMAC-SHA256. The returned bytes are deterministic for identical input and
// can be sent over an untrusted transport. Existing subscription APIs do not
// call this function implicitly.
func SealSQLSubscriptionWireEnvelope(key []byte, envelope SQLSubscriptionWireEnvelope) ([]byte, error) {
	mode, ok := sqlSubscriptionWireMode(envelope.Mode)
	if !ok || !validSQLSubscriptionWireKey(key) || !validSQLSubscriptionWireEnvelope(envelope) {
		return nil, ErrSQLSubscriptionWireEnvelopeInvalid
	}

	identifier := []byte(envelope.SubscriptionID)
	payloadLength := len(envelope.Payload)
	bodyLength := sqlSubscriptionWireHeaderBytes + len(identifier) + payloadLength
	wire := make([]byte, bodyLength+SQLSubscriptionWireSignatureBytes)
	copy(wire[:4], sqlSubscriptionWireMagic)
	wire[4] = SQLSubscriptionWireEnvelopeVersion
	wire[5] = mode
	binary.BigEndian.PutUint16(wire[6:8], uint16(len(identifier)))
	binary.BigEndian.PutUint64(wire[8:16], envelope.Sequence)
	binary.BigEndian.PutUint64(wire[16:24], uint64(envelope.Diff))
	binary.BigEndian.PutUint32(wire[24:28], uint32(payloadLength))
	position := sqlSubscriptionWireHeaderBytes
	position += copy(wire[position:], identifier)
	copy(wire[position:], envelope.Payload)

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(wire[:bodyLength])
	copy(wire[bodyLength:], mac.Sum(nil))
	return wire, nil
}

// OpenSQLSubscriptionWireEnvelope authenticates and decodes one envelope.
// Length checks happen before allocation, and authentication uses a
// constant-time comparison. The returned identifier and payload do not alias
// the input wire buffer.
func OpenSQLSubscriptionWireEnvelope(key, wire []byte) (SQLSubscriptionWireEnvelope, error) {
	if !validSQLSubscriptionWireKey(key) || len(wire) < sqlSubscriptionWireHeaderBytes+SQLSubscriptionWireSignatureBytes || len(wire) > MaxSQLSubscriptionWireEnvelopeBytes {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeInvalid
	}
	if string(wire[:4]) != sqlSubscriptionWireMagic || wire[4] != SQLSubscriptionWireEnvelopeVersion {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeInvalid
	}

	identifierLength := int(binary.BigEndian.Uint16(wire[6:8]))
	payloadLength := binary.BigEndian.Uint32(wire[24:28])
	if identifierLength > MaxSQLSubscriptionWireSubscriptionIDBytes || payloadLength > MaxSQLSubscriptionWirePayloadBytes {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeInvalid
	}
	bodyLength := sqlSubscriptionWireHeaderBytes + identifierLength + int(payloadLength)
	if bodyLength+SQLSubscriptionWireSignatureBytes != len(wire) {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeInvalid
	}

	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(wire[:bodyLength])
	if !hmac.Equal(mac.Sum(nil), wire[bodyLength:]) {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeAuthentication
	}

	envelope := SQLSubscriptionWireEnvelope{
		Mode:           sqlSubscriptionWireModeValue(wire[5]),
		SubscriptionID: string(wire[sqlSubscriptionWireHeaderBytes : sqlSubscriptionWireHeaderBytes+identifierLength]),
		Sequence:       binary.BigEndian.Uint64(wire[8:16]),
		Diff:           int64(binary.BigEndian.Uint64(wire[16:24])),
		Payload:        append([]byte(nil), wire[sqlSubscriptionWireHeaderBytes+identifierLength:bodyLength]...),
	}
	if !validSQLSubscriptionWireEnvelope(envelope) {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireEnvelopeInvalid
	}
	return envelope, nil
}

func validSQLSubscriptionWireKey(key []byte) bool {
	return len(key) > 0 && len(key) <= MaxSQLSubscriptionWireKeyBytes
}

func validSQLSubscriptionWireEnvelope(envelope SQLSubscriptionWireEnvelope) bool {
	if _, ok := sqlSubscriptionWireMode(envelope.Mode); !ok || len(envelope.SubscriptionID) == 0 || len(envelope.SubscriptionID) > MaxSQLSubscriptionWireSubscriptionIDBytes || len(envelope.Payload) > MaxSQLSubscriptionWirePayloadBytes {
		return false
	}
	for index := 0; index < len(envelope.SubscriptionID); index++ {
		if envelope.SubscriptionID[index] == 0 {
			return false
		}
	}
	return envelope.Mode != SQLSubscriptionModeSnapshot || envelope.Diff == 0
}

func sqlSubscriptionWireMode(mode SQLSubscriptionMode) (byte, bool) {
	switch mode {
	case SQLSubscriptionModeSnapshot:
		return 1, true
	case SQLSubscriptionModeDifferential:
		return 2, true
	default:
		return 0, false
	}
}

func sqlSubscriptionWireModeValue(mode byte) SQLSubscriptionMode {
	switch mode {
	case 1:
		return SQLSubscriptionModeSnapshot
	case 2:
		return SQLSubscriptionModeDifferential
	default:
		return SQLSubscriptionMode(mode)
	}
}
