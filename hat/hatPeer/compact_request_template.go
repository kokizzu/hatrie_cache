package hatPeer

import (
	"encoding/binary"
	"errors"
)

var ErrCompactRequestTemplateInvalid = errors.New("hatPeer: compact request template is invalid")

// CompactRequestTemplate stores immutable command metadata for repeated
// request encoding. The command bytes are copied at construction time.
type CompactRequestTemplate struct {
	command []byte
}

// NewCompactRequestTemplate creates a reusable compact request template.
func NewCompactRequestTemplate(command []byte) (CompactRequestTemplate, error) {
	if len(command) == 0 || len(command) > maxCompactProtocolCommandBytes {
		return CompactRequestTemplate{}, ErrCompactRequestTemplateInvalid
	}
	return CompactRequestTemplate{command: append([]byte(nil), command...)}, nil
}

// Marshal encodes one request using a newly allocated output buffer.
func (template CompactRequestTemplate) Marshal(protocol CompactProtocol, requestID uint64, payload []byte) ([]byte, error) {
	return template.MarshalInto(protocol, requestID, payload, nil)
}

// MarshalInto appends one request frame to dst. Reusing dst[:0] avoids the
// allocation performed by CompactProtocol.Marshal for each frame.
func (template CompactRequestTemplate) MarshalInto(protocol CompactProtocol, requestID uint64, payload, dst []byte) ([]byte, error) {
	frame := CompactFrame{
		Kind:      CompactRequest,
		RequestID: requestID,
		Command:   template.command,
		Payload:   payload,
	}
	if err := protocol.validateFrame(frame); err != nil {
		return dst, err
	}
	bodyBytes := compactProtocolHeader + compactUvarintSize(frame.RequestID) + compactUvarintSize(uint64(len(frame.Command))) + len(frame.Command) + compactUvarintSize(uint64(len(frame.Payload))) + len(frame.Payload)
	if bodyBytes > protocol.maxFrameBytes {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	prefixBytes := compactUvarintSize(uint64(bodyBytes))
	totalBytes := prefixBytes + bodyBytes
	start := len(dst)
	if cap(dst)-start < totalBytes {
		grown := make([]byte, start+totalBytes)
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:start+totalBytes]
	}
	binaryPutCompactRequest(dst[start:], frame)
	return dst, nil
}

func binaryPutCompactRequest(encoded []byte, frame CompactFrame) {
	bodyBytes := compactProtocolHeader + compactUvarintSize(frame.RequestID) + compactUvarintSize(uint64(len(frame.Command))) + len(frame.Command) + compactUvarintSize(uint64(len(frame.Payload))) + len(frame.Payload)
	// The caller has already sized encoded for the prefix and complete body.
	prefixBytes := compactUvarintSize(uint64(bodyBytes))
	binary.PutUvarint(encoded, uint64(bodyBytes))
	offset := prefixBytes
	encoded[offset] = compactProtocolMagic0
	offset++
	encoded[offset] = compactProtocolMagic1
	offset++
	encoded[offset] = compactProtocolVersion
	offset++
	encoded[offset] = byte(frame.Kind)
	offset++
	encoded[offset] = frame.Flags
	offset++
	offset += binary.PutUvarint(encoded[offset:], frame.RequestID)
	offset += binary.PutUvarint(encoded[offset:], uint64(len(frame.Command)))
	offset += copy(encoded[offset:], frame.Command)
	offset += binary.PutUvarint(encoded[offset:], uint64(len(frame.Payload)))
	copy(encoded[offset:], frame.Payload)
}

// RequestTemplate registers a request using a prepared command template.
func (multiplexer *CompactMultiplexer) RequestTemplate(template CompactRequestTemplate, payload []byte) (CompactFrame, *CompactPendingResponse, error) {
	if len(template.command) == 0 || len(template.command) > maxCompactProtocolCommandBytes {
		return CompactFrame{}, nil, ErrCompactRequestTemplateInvalid
	}
	return multiplexer.Request(template.command, payload)
}
