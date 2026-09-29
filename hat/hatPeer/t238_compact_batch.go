package hatPeer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// CompactBatchCommand identifies the compact binary batch envelope.
	CompactBatchCommand = "BATCH"
	// DefaultCompactBatchMaxItems bounds requests and responses in one batch.
	DefaultCompactBatchMaxItems      = 256
	compactBatchWireVersion     byte = 1
)

var (
	ErrCompactBatchInvalid         = errors.New("hatPeer: compact batch is invalid")
	ErrCompactBatchTooManyItems    = errors.New("hatPeer: compact batch has too many items")
	ErrCompactBatchCommandTooLarge = errors.New("hatPeer: compact batch command is too large")
	ErrCompactBatchPayloadTooLarge = errors.New("hatPeer: compact batch payload is too large")
	ErrCompactBatchHandlerRequired = errors.New("hatPeer: compact batch handler is required")
	ErrCompactBatchResponseInvalid = errors.New("hatPeer: compact batch response is invalid")
)

// CompactBatchRequest is one command in a compact binary batch. Items are
// executed in slice order by NewCompactBatchHandler.
type CompactBatchRequest struct {
	Command []byte
	Payload []byte
}

// CompactBatchResponse is one ordered result from a compact binary batch.
// Item errors use Kind CompactError and retain the other items' results.
type CompactBatchResponse struct {
	Kind             CompactFrameKind
	ResponseSchemaID uint64
	Command          []byte
	Payload          []byte
}

// MarshalBatch encodes bounded ordered requests into one frame payload.
func (protocol CompactProtocol) MarshalBatch(requests []CompactBatchRequest) ([]byte, error) {
	size, err := protocol.compactBatchRequestSize(requests)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, size)
	writeCompactBatchRequests(encoded, 0, requests)
	return encoded, nil
}

// marshalCompactBatchFrameInto appends one uncompressed BATCH request frame
// without first allocating a separate batch payload. Session writes use this
// to reuse their normal bounded frame buffer.
func (protocol CompactProtocol) marshalCompactBatchFrameInto(requestID uint64, requests []CompactBatchRequest, dst []byte) ([]byte, error) {
	if requestID == 0 {
		return dst, ErrCompactProtocolRequestIDInvalid
	}
	if len(CompactBatchCommand) > protocol.compactBatchCommandLimit() {
		return dst, ErrCompactProtocolCommandTooLarge
	}
	payloadBytes, err := protocol.compactBatchRequestSize(requests)
	if err != nil {
		return dst, err
	}
	bodyBytes := compactProtocolHeader
	var ok bool
	bodyBytes, ok = addCompactBatchSize(bodyBytes, compactUvarintSize(requestID))
	if !ok {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	bodyBytes, ok = addCompactBatchSize(bodyBytes, compactUvarintSize(uint64(len(CompactBatchCommand))))
	if !ok {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	bodyBytes, ok = addCompactBatchSize(bodyBytes, len(CompactBatchCommand))
	if !ok {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	bodyBytes, ok = addCompactBatchSize(bodyBytes, compactUvarintSize(uint64(payloadBytes)))
	if !ok {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	bodyBytes, ok = addCompactBatchSize(bodyBytes, payloadBytes)
	if !ok || bodyBytes > protocol.compactBatchFrameLimit() {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	prefixBytes := compactUvarintSize(uint64(bodyBytes))
	totalBytes, ok := addCompactBatchSize(prefixBytes, bodyBytes)
	if !ok {
		return dst, ErrCompactProtocolFrameTooLarge
	}
	start := len(dst)
	if cap(dst)-start < totalBytes {
		grown := make([]byte, start+totalBytes)
		copy(grown, dst)
		dst = grown
	} else {
		dst = dst[:start+totalBytes]
	}
	encoded := dst[start:]
	binary.PutUvarint(encoded, uint64(bodyBytes))
	offset := prefixBytes
	encoded[offset] = compactProtocolMagic0
	offset++
	encoded[offset] = compactProtocolMagic1
	offset++
	encoded[offset] = compactProtocolVersion
	offset++
	encoded[offset] = byte(CompactRequest)
	offset++
	encoded[offset] = 0
	offset++
	offset += binary.PutUvarint(encoded[offset:], requestID)
	offset += binary.PutUvarint(encoded[offset:], uint64(len(CompactBatchCommand)))
	offset += copy(encoded[offset:], CompactBatchCommand)
	offset += binary.PutUvarint(encoded[offset:], uint64(payloadBytes))
	writeCompactBatchRequests(encoded, offset, requests)
	return dst, nil
}

// UnmarshalBatch decodes and owns one compact binary request batch payload.
func (protocol CompactProtocol) UnmarshalBatch(payload []byte) ([]CompactBatchRequest, error) {
	if len(payload) == 0 || payload[0] != compactBatchWireVersion {
		return nil, ErrCompactBatchInvalid
	}
	if len(payload) > protocol.compactBatchPayloadLimit() {
		return nil, ErrCompactBatchPayloadTooLarge
	}
	offset := 1
	count, ok := readCompactBatchUvarint(payload, &offset)
	if !ok || count == 0 {
		return nil, ErrCompactBatchInvalid
	}
	if count > DefaultCompactBatchMaxItems {
		return nil, ErrCompactBatchTooManyItems
	}
	requests := make([]CompactBatchRequest, 0, int(count))
	for index := uint64(0); index < count; index++ {
		command, ok := readCompactBatchBytes(payload, &offset)
		if !ok {
			return nil, ErrCompactBatchInvalid
		}
		requestPayload, ok := readCompactBatchBytes(payload, &offset)
		if !ok {
			return nil, ErrCompactBatchInvalid
		}
		request := CompactBatchRequest{Command: command, Payload: requestPayload}
		if err := protocol.validateCompactBatchRequest(request); err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	if offset != len(payload) {
		return nil, ErrCompactBatchInvalid
	}
	return requests, nil
}

// MarshalBatchResponses encodes ordered per-item responses into one payload.
func (protocol CompactProtocol) MarshalBatchResponses(responses []CompactBatchResponse) ([]byte, error) {
	if len(responses) == 0 {
		return nil, ErrCompactBatchResponseInvalid
	}
	if len(responses) > DefaultCompactBatchMaxItems {
		return nil, ErrCompactBatchTooManyItems
	}
	encoded := make([]byte, 0, len(responses)*20)
	encoded = append(encoded, compactBatchWireVersion)
	encoded = appendCompactBatchUvarint(encoded, uint64(len(responses)))
	for _, response := range responses {
		if response.Kind != CompactResponse && response.Kind != CompactError {
			return nil, ErrCompactBatchResponseInvalid
		}
		if len(response.Command) > protocol.compactBatchCommandLimit() {
			return nil, ErrCompactBatchCommandTooLarge
		}
		if len(response.Payload) > protocol.compactBatchPayloadLimit() {
			return nil, ErrCompactBatchPayloadTooLarge
		}
		encoded = append(encoded, byte(response.Kind))
		encoded = appendCompactBatchUvarint(encoded, response.ResponseSchemaID)
		encoded = appendCompactBatchBytes(encoded, response.Command)
		encoded = appendCompactBatchBytes(encoded, response.Payload)
		if len(encoded) > protocol.compactBatchPayloadLimit() {
			return nil, ErrCompactBatchPayloadTooLarge
		}
	}
	return encoded, nil
}

// UnmarshalBatchResponses decodes and owns ordered compact batch responses.
func (protocol CompactProtocol) UnmarshalBatchResponses(payload []byte) ([]CompactBatchResponse, error) {
	if len(payload) == 0 || payload[0] != compactBatchWireVersion {
		return nil, ErrCompactBatchResponseInvalid
	}
	if len(payload) > protocol.compactBatchPayloadLimit() {
		return nil, ErrCompactBatchPayloadTooLarge
	}
	offset := 1
	count, ok := readCompactBatchUvarint(payload, &offset)
	if !ok || count == 0 {
		return nil, ErrCompactBatchResponseInvalid
	}
	if count > DefaultCompactBatchMaxItems {
		return nil, ErrCompactBatchTooManyItems
	}
	responses := make([]CompactBatchResponse, 0, int(count))
	for index := uint64(0); index < count; index++ {
		if offset >= len(payload) || (CompactFrameKind(payload[offset]) != CompactResponse && CompactFrameKind(payload[offset]) != CompactError) {
			return nil, ErrCompactBatchResponseInvalid
		}
		kind := CompactFrameKind(payload[offset])
		offset++
		schemaID, ok := readCompactBatchUvarint(payload, &offset)
		if !ok {
			return nil, ErrCompactBatchResponseInvalid
		}
		command, ok := readCompactBatchBytes(payload, &offset)
		if !ok || len(command) > protocol.compactBatchCommandLimit() {
			return nil, ErrCompactBatchResponseInvalid
		}
		responsePayload, ok := readCompactBatchBytes(payload, &offset)
		if !ok || len(responsePayload) > protocol.compactBatchPayloadLimit() {
			return nil, ErrCompactBatchResponseInvalid
		}
		responses = append(responses, CompactBatchResponse{
			Kind:             kind,
			ResponseSchemaID: schemaID,
			Command:          command,
			Payload:          responsePayload,
		})
	}
	if offset != len(payload) {
		return nil, ErrCompactBatchResponseInvalid
	}
	return responses, nil
}

// NewCompactBatchHandler adapts a normal compact handler to execute BATCH
// items sequentially and return one ordered response envelope. An item error
// is encoded as CompactError so later items still execute.
func NewCompactBatchHandler(handler CompactPeerHandler) (CompactPeerHandler, error) {
	if handler == nil {
		return nil, ErrCompactBatchHandlerRequired
	}
	protocol, err := NewCompactProtocol(CompactProtocolOptions{
		MaxFrameBytes:   DefaultCompactProtocolMaxFrameBytes,
		MaxCommandBytes: DefaultCompactProtocolMaxCommandBytes,
		MaxPayloadBytes: DefaultCompactProtocolMaxPayloadBytes,
	})
	if err != nil {
		return nil, err
	}
	return NewCompactBatchHandlerWithProtocol(handler, protocol)
}

// NewCompactBatchHandlerWithProtocol adapts a compact handler using the same
// command and payload limits as the session that receives the outer frame.
func NewCompactBatchHandlerWithProtocol(handler CompactPeerHandler, protocol CompactProtocol) (CompactPeerHandler, error) {
	if handler == nil {
		return nil, ErrCompactBatchHandlerRequired
	}
	return func(ctx context.Context, frame CompactFrame) (CompactFrame, error) {
		if !bytes.Equal(frame.Command, []byte(CompactBatchCommand)) {
			return handler(ctx, frame)
		}
		requests, err := protocol.UnmarshalBatch(frame.Payload)
		if err != nil {
			return CompactFrame{}, err
		}
		responses := make([]CompactBatchResponse, 0, len(requests))
		for index, request := range requests {
			inner, handlerErr := handler(ctx, CompactFrame{
				Kind:      CompactRequest,
				RequestID: uint64(index + 1),
				Command:   request.Command,
				Payload:   request.Payload,
			})
			if handlerErr != nil {
				responses = append(responses, CompactBatchResponse{
					Kind:    CompactError,
					Command: append([]byte(nil), request.Command...),
					Payload: []byte(handlerErr.Error()),
				})
				continue
			}
			if inner.Kind != CompactResponse && inner.Kind != CompactError {
				responses = append(responses, CompactBatchResponse{
					Kind:    CompactError,
					Command: append([]byte(nil), request.Command...),
					Payload: []byte(ErrCompactBatchResponseInvalid.Error()),
				})
				continue
			}
			if len(inner.Command) == 0 {
				inner.Command = request.Command
			}
			responses = append(responses, CompactBatchResponse{
				Kind:             inner.Kind,
				ResponseSchemaID: inner.ResponseSchemaID,
				Command:          append([]byte(nil), inner.Command...),
				Payload:          append([]byte(nil), inner.Payload...),
			})
		}
		encoded, err := protocol.MarshalBatchResponses(responses)
		if err != nil {
			return CompactFrame{}, err
		}
		return CompactFrame{
			Kind:      CompactResponse,
			RequestID: frame.RequestID,
			Command:   append([]byte(nil), frame.Command...),
			Payload:   encoded,
		}, nil
	}, nil
}

// CallBatch sends one bounded BATCH frame and returns responses in request
// order. Each item is handled independently; item errors are returned as
// CompactError responses rather than failing the whole transport call.
func (session *CompactPeerSession) CallBatch(ctx context.Context, requests []CompactBatchRequest) ([]CompactBatchResponse, error) {
	if session == nil {
		return nil, ErrCompactPeerClosed
	}
	if session.protocol.compressPayloadsAbove > 0 {
		payload, err := session.protocol.MarshalBatch(requests)
		if err != nil {
			return nil, err
		}
		response, err := session.Call(ctx, []byte(CompactBatchCommand), payload)
		if err != nil {
			return nil, err
		}
		return session.protocol.UnmarshalBatchResponses(response.Payload)
	}
	if _, err := session.protocol.compactBatchRequestSize(requests); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := session.Err(); err != nil {
		return nil, err
	}
	request, pending, err := session.multiplex.Request([]byte(CompactBatchCommand), nil)
	if err != nil {
		return nil, err
	}
	if err := session.writeCompactBatch(request.RequestID, requests); err != nil {
		session.multiplex.Cancel(request.RequestID)
		session.fail(err)
		return nil, err
	}
	response, err := pending.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil {
			session.multiplex.Cancel(request.RequestID)
			session.sendRequestCancellation(request.RequestID)
			return nil, ctx.Err()
		}
		return nil, err
	}
	if response.Kind == CompactError {
		return nil, fmt.Errorf("%w: %s", ErrCompactPeerRemote, response.Payload)
	}
	return session.protocol.UnmarshalBatchResponses(response.Payload)
}

func (session *CompactPeerSession) writeCompactBatch(requestID uint64, requests []CompactBatchRequest) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	if err := session.Err(); err != nil {
		return err
	}
	encoded, err := session.protocol.marshalCompactBatchFrameInto(requestID, requests, session.writeBuffer[:0])
	if err != nil {
		return err
	}
	if err := writeCompactPeerBytes(session.conn, encoded); err != nil {
		return err
	}
	session.writeBuffer = retainCompactPeerWriteBuffer(encoded)
	return nil
}

func (protocol CompactProtocol) compactBatchRequestSize(requests []CompactBatchRequest) (int, error) {
	if len(requests) == 0 {
		return 0, ErrCompactBatchInvalid
	}
	if len(requests) > DefaultCompactBatchMaxItems {
		return 0, ErrCompactBatchTooManyItems
	}
	size := 1 + compactUvarintSize(uint64(len(requests)))
	for _, request := range requests {
		if err := protocol.validateCompactBatchRequest(request); err != nil {
			return 0, err
		}
		var ok bool
		size, ok = addCompactBatchSize(size, compactUvarintSize(uint64(len(request.Command))))
		if !ok {
			return 0, ErrCompactBatchPayloadTooLarge
		}
		size, ok = addCompactBatchSize(size, len(request.Command))
		if !ok {
			return 0, ErrCompactBatchPayloadTooLarge
		}
		size, ok = addCompactBatchSize(size, compactUvarintSize(uint64(len(request.Payload))))
		if !ok {
			return 0, ErrCompactBatchPayloadTooLarge
		}
		size, ok = addCompactBatchSize(size, len(request.Payload))
		if !ok || size > protocol.compactBatchPayloadLimit() {
			return 0, ErrCompactBatchPayloadTooLarge
		}
	}
	return size, nil
}

func writeCompactBatchRequests(dst []byte, offset int, requests []CompactBatchRequest) int {
	dst[offset] = compactBatchWireVersion
	offset++
	offset += binary.PutUvarint(dst[offset:], uint64(len(requests)))
	for _, request := range requests {
		offset += binary.PutUvarint(dst[offset:], uint64(len(request.Command)))
		offset += copy(dst[offset:], request.Command)
		offset += binary.PutUvarint(dst[offset:], uint64(len(request.Payload)))
		offset += copy(dst[offset:], request.Payload)
	}
	return offset
}

func addCompactBatchSize(total, extra int) (int, bool) {
	if total < 0 || extra < 0 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	if total > maxInt-extra {
		return 0, false
	}
	return total + extra, true
}

func (protocol CompactProtocol) compactBatchFrameLimit() int {
	if protocol.maxFrameBytes > 0 {
		return protocol.maxFrameBytes
	}
	return DefaultCompactProtocolMaxFrameBytes
}

func (protocol CompactProtocol) validateCompactBatchRequest(request CompactBatchRequest) error {
	if len(request.Command) == 0 {
		return ErrCompactBatchInvalid
	}
	if len(request.Command) > protocol.compactBatchCommandLimit() {
		return ErrCompactBatchCommandTooLarge
	}
	if len(request.Payload) > protocol.compactBatchPayloadLimit() {
		return ErrCompactBatchPayloadTooLarge
	}
	return nil
}

func (protocol CompactProtocol) compactBatchCommandLimit() int {
	if protocol.maxCommandBytes > 0 {
		return protocol.maxCommandBytes
	}
	return DefaultCompactProtocolMaxCommandBytes
}

func (protocol CompactProtocol) compactBatchPayloadLimit() int {
	if protocol.maxPayloadBytes > 0 {
		return protocol.maxPayloadBytes
	}
	return DefaultCompactProtocolMaxPayloadBytes
}

func appendCompactBatchUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(dst, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendCompactBatchBytes(dst, value []byte) []byte {
	dst = appendCompactBatchUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readCompactBatchUvarint(payload []byte, offset *int) (uint64, bool) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}

func readCompactBatchBytes(payload []byte, offset *int) ([]byte, bool) {
	length, ok := readCompactBatchUvarint(payload, offset)
	if !ok || length > uint64(len(payload)-*offset) {
		return nil, false
	}
	end := *offset + int(length)
	if end < *offset || end > len(payload) {
		return nil, false
	}
	value := append([]byte(nil), payload[*offset:end]...)
	*offset = end
	return value, true
}
