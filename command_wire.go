package hatriecache

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"hatrie_cache/internal/gen/hatriecache/v1"
	"hatrie_cache/internal/jsonwire"

	json "github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"
)

const (
	commandWireContentTypeJSON     = "application/json"
	commandWireContentTypeProtobuf = "application/x-protobuf"
	maxCommandWireReadLimit        = int64(1<<63 - 2)
)

type CommandWireFormat string

const (
	CommandWireFormatJSON     CommandWireFormat = "json"
	CommandWireFormatProtobuf CommandWireFormat = "protobuf"
)

const DefaultCommandWireFormat = CommandWireFormatProtobuf

// ErrUnsupportedCommandResponseContentType is returned when an HTTP command
// response advertises a content type that the command wire decoder cannot read.
var ErrUnsupportedCommandResponseContentType = errors.New("hatriecache: unsupported command response content type")

// ErrUnsupportedCommandWireProtobufValue is returned when a command request
// contains structured Values or Pairs that cannot fit the scalar protobuf API.
var ErrUnsupportedCommandWireProtobufValue = errors.New("hatriecache: command request cannot be encoded as protobuf")

var errCommandWireInvalidLimit = errors.New("hatriecache: command wire read limit is invalid")

var commandRequestProtoPool = sync.Pool{
	New: func() interface{} {
		return new(hatriecachev1.CommandRequest)
	},
}

var commandResponseProtoPool = sync.Pool{
	New: func() interface{} {
		return new(hatriecachev1.CommandResponse)
	},
}

var commandWireBufferPool = sync.Pool{
	New: func() interface{} {
		data := make([]byte, 0, 4096)
		return data
	},
}

const maxPooledCommandWireBufferCapacity = 1 << 20

func ParseCommandWireFormat(value string) (CommandWireFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(CommandWireFormatProtobuf), "proto", "pb":
		return CommandWireFormatProtobuf, nil
	case string(CommandWireFormatJSON):
		return CommandWireFormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported command wire format %q", value)
	}
}

func commandWireFormatFromContentType(value string) (CommandWireFormat, bool) {
	mediaType, _, _ := strings.Cut(value, ";")
	mediaType = strings.TrimSpace(mediaType)
	if mediaType == "" {
		return CommandWireFormatJSON, true
	}
	switch {
	case strings.EqualFold(mediaType, commandWireContentTypeJSON), strings.EqualFold(mediaType, "text/json"):
		return CommandWireFormatJSON, true
	case strings.EqualFold(mediaType, commandWireContentTypeProtobuf),
		strings.EqualFold(mediaType, "application/protobuf"),
		strings.EqualFold(mediaType, "application/octet-stream"):
		return CommandWireFormatProtobuf, true
	default:
		return "", false
	}
}

func commandWireFormatFromAccept(value string, fallback CommandWireFormat) (CommandWireFormat, bool) {
	if strings.TrimSpace(value) == "" {
		return fallback, true
	}
	jsonQuality := -1.0
	protobufQuality := -1.0
	wildcardQuality := -1.0
	for {
		part, rest, ok := strings.Cut(value, ",")
		mediaType, quality := parseCommandWireAccept(part)
		switch {
		case commandWireAcceptMatchesProtobuf(mediaType):
			if quality > protobufQuality {
				protobufQuality = quality
			}
		case commandWireAcceptMatchesJSON(mediaType):
			if quality > jsonQuality {
				jsonQuality = quality
			}
		case commandWireAcceptMatchesWildcard(mediaType):
			if quality > wildcardQuality {
				wildcardQuality = quality
			}
		}
		if !ok {
			break
		}
		value = rest
	}
	bestFormat := fallback
	bestQuality := -1.0
	for _, format := range commandWireAcceptPreference(fallback) {
		quality := commandWireAcceptQuality(format, jsonQuality, protobufQuality, wildcardQuality)
		if quality < 0 {
			continue
		}
		if quality > bestQuality && quality > 0 {
			bestFormat = format
			bestQuality = quality
		}
	}
	if bestQuality <= 0 {
		return "", false
	}
	return bestFormat, true
}

func commandWireAcceptMatchesJSON(mediaType string) bool {
	return strings.EqualFold(mediaType, commandWireContentTypeJSON) ||
		strings.EqualFold(mediaType, "text/json")
}

func commandWireAcceptMatchesProtobuf(mediaType string) bool {
	return strings.EqualFold(mediaType, commandWireContentTypeProtobuf) ||
		strings.EqualFold(mediaType, "application/protobuf") ||
		strings.EqualFold(mediaType, "application/octet-stream")
}

func commandWireAcceptMatchesWildcard(mediaType string) bool {
	return mediaType == "*/*" || strings.EqualFold(mediaType, "application/*")
}

func commandWireAcceptQuality(format CommandWireFormat, jsonQuality float64, protobufQuality float64, wildcardQuality float64) float64 {
	switch format {
	case CommandWireFormatJSON:
		if jsonQuality >= 0 {
			return jsonQuality
		}
	case CommandWireFormatProtobuf:
		if protobufQuality >= 0 {
			return protobufQuality
		}
	}
	return wildcardQuality
}

func parseCommandWireAccept(value string) (string, float64) {
	mediaType, params, _ := strings.Cut(value, ";")
	mediaType = strings.TrimSpace(mediaType)
	if mediaType == "" {
		return "", 0
	}
	quality := 1.0
	for params != "" {
		var part string
		part, params, _ = strings.Cut(params, ";")
		key, raw, hasValue := strings.Cut(strings.TrimSpace(part), "=")
		if !hasValue || !strings.EqualFold(strings.TrimSpace(key), "q") {
			continue
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return mediaType, 0
		}
		if parsed < 0 {
			parsed = 0
		}
		if parsed > 1 {
			parsed = 1
		}
		quality = parsed
	}
	return mediaType, quality
}

func commandWireAcceptPreference(fallback CommandWireFormat) []CommandWireFormat {
	if fallback == CommandWireFormatJSON {
		return []CommandWireFormat{CommandWireFormatJSON, CommandWireFormatProtobuf}
	}
	return []CommandWireFormat{CommandWireFormatProtobuf, CommandWireFormatJSON}
}

func commandRequestBody(request CacheCommandRequest, format CommandWireFormat, estimatedJSONSize int, compressionThreshold int) (io.Reader, string, string, error) {
	switch format {
	case CommandWireFormatProtobuf:
		message, err := cacheCommandRequestToPooledProto(request)
		if err != nil {
			return nil, "", "", err
		}
		data := acquireCommandWireBuffer(proto.Size(message))
		data, err = proto.MarshalOptions{}.MarshalAppend(data, message)
		releaseCommandRequestProto(message)
		if err != nil {
			releaseCommandWireBuffer(data)
			return nil, "", "", err
		}
		body, contentEncoding, err := jsonwire.EncodedRequestBodyWithRelease(data, compressionThreshold, releaseCommandWireBuffer)
		if err != nil {
			releaseCommandWireBuffer(data)
			return nil, "", "", err
		}
		return body, commandWireContentTypeProtobuf, contentEncoding, nil
	case CommandWireFormatJSON:
		body, contentEncoding, err := jsonwire.RequestBody(request, estimatedJSONSize, compressionThreshold)
		if err != nil {
			return nil, "", "", err
		}
		return body, commandWireContentTypeJSON, contentEncoding, nil
	default:
		return nil, "", "", fmt.Errorf("hatriecache: unsupported command wire format %q", format)
	}
}

// CommandRequestBody serializes a cache command request for the HTTP command API.
func CommandRequestBody(request CacheCommandRequest, format CommandWireFormat, estimatedJSONSize int, compressionThreshold int) (io.Reader, string, string, error) {
	return commandRequestBody(request, format, estimatedJSONSize, compressionThreshold)
}

func decodeCommandResponseWire(reader io.Reader, contentType string, limit int64) (CacheCommandResponse, error) {
	format, ok := commandWireFormatFromContentType(contentType)
	if !ok {
		return CacheCommandResponse{}, fmt.Errorf("%w: %q", ErrUnsupportedCommandResponseContentType, contentType)
	}
	if format == CommandWireFormatProtobuf {
		data, err := readLimitedCommandWire(reader, limit)
		if err != nil {
			return CacheCommandResponse{}, err
		}
		var response hatriecachev1.CommandResponse
		if err := proto.Unmarshal(data, &response); err != nil {
			return CacheCommandResponse{}, err
		}
		return cacheCommandResponseFromProto(&response), nil
	}
	return decodeCommandResponseJSON(reader, limit)
}

func cacheCommandResponseFromProto(response *hatriecachev1.CommandResponse) CacheCommandResponse {
	if response == nil {
		return CacheCommandResponse{}
	}
	out := CacheCommandResponse{
		OK:      response.GetOk(),
		Message: response.GetMessage(),
		Value:   response.GetValue(),
	}
	if len(response.GetResponses()) > 0 {
		out.Responses = make([]CacheCommandResponse, len(response.GetResponses()))
		for i, value := range response.GetResponses() {
			out.Responses[i] = cacheCommandResponseFromProto(value)
		}
	}
	return out
}

// DecodeCommandResponseWire decodes an HTTP command API response body.
func DecodeCommandResponseWire(reader io.Reader, contentType string, limit int64) (CacheCommandResponse, error) {
	return decodeCommandResponseWire(reader, contentType, limit)
}

func decodeCommandResponseJSON(reader io.Reader, limit int64) (CacheCommandResponse, error) {
	limited, err := newCommandWireLimitedReader(reader, limit)
	if err != nil {
		return CacheCommandResponse{}, err
	}
	decoder := jsonwire.NewDecoder(limited)
	var response CacheCommandResponse
	if err := decoder.Decode(&response); err != nil {
		if limitedReaderExceeded(limited) {
			return CacheCommandResponse{}, errReplicationResponseTooLarge
		}
		return CacheCommandResponse{}, err
	}
	if limited.N <= 0 {
		return CacheCommandResponse{}, errReplicationResponseTooLarge
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if limitedReaderExceeded(limited) {
			return CacheCommandResponse{}, errReplicationResponseTooLarge
		}
		if err == nil {
			return CacheCommandResponse{}, errors.New("hatriecache: invalid command response JSON")
		}
		return CacheCommandResponse{}, err
	}
	if limitedReaderExceeded(limited) {
		return CacheCommandResponse{}, errReplicationResponseTooLarge
	}
	return response, nil
}

func newCommandWireLimitedReader(reader io.Reader, limit int64) (*io.LimitedReader, error) {
	if limit < 0 || limit > maxCommandWireReadLimit {
		return nil, errCommandWireInvalidLimit
	}
	return &io.LimitedReader{R: reader, N: limit + 1}, nil
}

func readLimitedCommandWire(reader io.Reader, limit int64) ([]byte, error) {
	limited, err := newCommandWireLimitedReader(reader, limit)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if limitedReaderExceeded(limited) {
		return nil, errReplicationResponseTooLarge
	}
	return data, nil
}

func decodeCommandRequestProto(reader io.Reader, limit int64) (CacheCommandRequest, error) {
	data, err := readLimitedCommandWire(reader, limit)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	var request hatriecachev1.CommandRequest
	if err := proto.Unmarshal(data, &request); err != nil {
		return CacheCommandRequest{}, err
	}
	return cacheCommandRequestFromProto(&request), nil
}

func writeCommandResponseWire(w http.ResponseWriter, r *http.Request, status int, response CacheCommandResponse, fallback CommandWireFormat) {
	addVaryHeader(w.Header(), "Accept")
	format, ok := commandWireFormatFromAccept(r.Header.Get("Accept"), fallback)
	if !ok {
		http.Error(w, "no acceptable command response content type", http.StatusNotAcceptable)
		return
	}
	if format != CommandWireFormatProtobuf {
		writeJSONStatus(w, status, response)
		return
	}
	message := cacheCommandResponseToPooledProto(response)
	data := acquireCommandWireBuffer(proto.Size(message))
	data, err := proto.MarshalOptions{}.MarshalAppend(data, message)
	releaseCommandResponseProto(message)
	if err != nil {
		releaseCommandWireBuffer(data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", commandWireContentTypeProtobuf)
	w.WriteHeader(status)
	_, _ = w.Write(data)
	releaseCommandWireBuffer(data)
}

func cacheCommandRequestToProto(request CacheCommandRequest) (*hatriecachev1.CommandRequest, error) {
	batch, err := cacheCommandBatchToProto(request.Batch)
	if err != nil {
		return nil, err
	}
	out := &hatriecachev1.CommandRequest{
		Command:     request.Command,
		Key:         request.Key,
		Value:       request.Value,
		Subkey:      request.Subkey,
		TtlSeconds:  request.TTLSeconds,
		UnixSeconds: request.UnixSeconds,
		Priority:    request.Priority,
		Batch:       batch,
	}
	if len(request.Values) > 0 {
		out.Values = make([]string, len(request.Values))
		for idx, value := range request.Values {
			text, ok := commandWireScalar(value)
			if !ok {
				return nil, fmt.Errorf("%w: command value %d", ErrUnsupportedCommandWireProtobufValue, idx)
			}
			out.Values[idx] = text
		}
	}
	if len(request.Pairs) > 0 {
		out.Pairs = make(map[string]string, len(request.Pairs))
		for key, value := range request.Pairs {
			text, ok := commandWireScalar(value)
			if !ok {
				return nil, fmt.Errorf("%w: command pair %q", ErrUnsupportedCommandWireProtobufValue, key)
			}
			out.Pairs[key] = text
		}
	}
	return out, nil
}

func cacheCommandBatchToProto(batch []CacheCommandRequest) ([]*hatriecachev1.CommandRequest, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	out := make([]*hatriecachev1.CommandRequest, len(batch))
	for idx, request := range batch {
		message, err := cacheCommandRequestToProto(request)
		if err != nil {
			return nil, fmt.Errorf("batch command %d: %w", idx, err)
		}
		out[idx] = message
	}
	return out, nil
}

func cacheCommandRequestToPooledProto(request CacheCommandRequest) (*hatriecachev1.CommandRequest, error) {
	message := acquireCommandRequestProto()
	if err := fillCacheCommandRequestProto(message, request); err != nil {
		releaseCommandRequestProto(message)
		return nil, err
	}
	return message, nil
}

func acquireCommandRequestProto() *hatriecachev1.CommandRequest {
	return commandRequestProtoPool.Get().(*hatriecachev1.CommandRequest)
}

func releaseCommandRequestProto(message *hatriecachev1.CommandRequest) {
	if message == nil {
		return
	}
	children := message.Batch
	message.Reset()
	for _, child := range children {
		releaseCommandRequestProto(child)
	}
	commandRequestProtoPool.Put(message)
}

func fillCacheCommandRequestProto(out *hatriecachev1.CommandRequest, request CacheCommandRequest) error {
	batch, err := cacheCommandBatchToPooledProto(request.Batch)
	if err != nil {
		return err
	}
	out.Command = request.Command
	out.Key = request.Key
	out.Value = request.Value
	out.Subkey = request.Subkey
	out.TtlSeconds = request.TTLSeconds
	out.UnixSeconds = request.UnixSeconds
	out.Priority = request.Priority
	out.Batch = batch
	if len(request.Values) > 0 {
		out.Values = make([]string, len(request.Values))
		for idx, value := range request.Values {
			text, ok := commandWireScalar(value)
			if !ok {
				return fmt.Errorf("%w: command value %d", ErrUnsupportedCommandWireProtobufValue, idx)
			}
			out.Values[idx] = text
		}
	}
	if len(request.Pairs) > 0 {
		out.Pairs = make(map[string]string, len(request.Pairs))
		for key, value := range request.Pairs {
			text, ok := commandWireScalar(value)
			if !ok {
				return fmt.Errorf("%w: command pair %q", ErrUnsupportedCommandWireProtobufValue, key)
			}
			out.Pairs[key] = text
		}
	}
	return nil
}

func cacheCommandBatchToPooledProto(batch []CacheCommandRequest) ([]*hatriecachev1.CommandRequest, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	out := make([]*hatriecachev1.CommandRequest, len(batch))
	for idx, request := range batch {
		message := acquireCommandRequestProto()
		if err := fillCacheCommandRequestProto(message, request); err != nil {
			releaseCommandRequestProto(message)
			for _, previous := range out[:idx] {
				releaseCommandRequestProto(previous)
			}
			return nil, fmt.Errorf("batch command %d: %w", idx, err)
		}
		out[idx] = message
	}
	return out, nil
}

func acquireCommandWireBuffer(size int) []byte {
	if size < 0 {
		size = 0
	}
	data := commandWireBufferPool.Get().([]byte)
	if cap(data) < size {
		return make([]byte, 0, size)
	}
	return data[:0]
}

func releaseCommandWireBuffer(data []byte) {
	if data == nil || cap(data) > maxPooledCommandWireBufferCapacity {
		return
	}
	commandWireBufferPool.Put(data[:0])
}

func cacheCommandResponseToPooledProto(response CacheCommandResponse) *hatriecachev1.CommandResponse {
	message := acquireCommandResponseProto()
	fillCacheCommandResponseProto(message, response)
	return message
}

func acquireCommandResponseProto() *hatriecachev1.CommandResponse {
	return commandResponseProtoPool.Get().(*hatriecachev1.CommandResponse)
}

func releaseCommandResponseProto(message *hatriecachev1.CommandResponse) {
	if message == nil {
		return
	}
	children := message.Responses
	for _, child := range children {
		releaseCommandResponseProto(child)
	}
	for idx := range children {
		children[idx] = nil
	}
	message.Reset()
	if cap(children) > 0 {
		message.Responses = children[:0]
	}
	commandResponseProtoPool.Put(message)
}

func fillCacheCommandResponseProto(out *hatriecachev1.CommandResponse, response CacheCommandResponse) {
	out.Ok = response.OK
	out.Message = response.Message
	out.Value = response.Value
	if len(response.Responses) == 0 {
		return
	}
	if cap(out.Responses) < len(response.Responses) {
		out.Responses = make([]*hatriecachev1.CommandResponse, len(response.Responses))
	} else {
		out.Responses = out.Responses[:len(response.Responses)]
	}
	for idx := range response.Responses {
		child := acquireCommandResponseProto()
		fillCacheCommandResponseProto(child, response.Responses[idx])
		out.Responses[idx] = child
	}
}

func commandWireScalar(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case int:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	case float32:
		return commandWireFloat(float64(v), 32)
	case float64:
		return commandWireFloat(v, 64)
	default:
		return "", false
	}
}

func commandWireFloat(value float64, bitSize int) (string, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", false
	}
	return strconv.FormatFloat(value, 'g', -1, bitSize), true
}
