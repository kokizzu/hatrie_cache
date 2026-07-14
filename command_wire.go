package hatriecache

import (
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"hatrie_cache/internal/gen/hatriecache/v1"
	"hatrie_cache/internal/jsonwire"

	json "github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"
)

const (
	commandWireContentTypeJSON     = "application/json"
	commandWireContentTypeProtobuf = "application/x-protobuf"
)

type CommandWireFormat string

const (
	CommandWireFormatJSON     CommandWireFormat = "json"
	CommandWireFormatProtobuf CommandWireFormat = "protobuf"
)

const DefaultCommandWireFormat = CommandWireFormatProtobuf

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
	mediaType := strings.TrimSpace(value)
	if mediaType == "" {
		return CommandWireFormatJSON, true
	}
	if parsed, _, err := mime.ParseMediaType(mediaType); err == nil {
		mediaType = parsed
	}
	switch strings.ToLower(mediaType) {
	case commandWireContentTypeJSON, "text/json":
		return CommandWireFormatJSON, true
	case commandWireContentTypeProtobuf, "application/protobuf", "application/octet-stream":
		return CommandWireFormatProtobuf, true
	default:
		return "", false
	}
}

func commandWireFormatFromAccept(value string, fallback CommandWireFormat) CommandWireFormat {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	specific := map[CommandWireFormat]float64{}
	wildcardQuality := -1.0
	for _, part := range strings.Split(value, ",") {
		mediaType, quality := parseCommandWireAccept(part)
		switch strings.ToLower(mediaType) {
		case commandWireContentTypeProtobuf, "application/protobuf", "application/octet-stream":
			if current, ok := specific[CommandWireFormatProtobuf]; !ok || quality > current {
				specific[CommandWireFormatProtobuf] = quality
			}
		case commandWireContentTypeJSON, "text/json":
			if current, ok := specific[CommandWireFormatJSON]; !ok || quality > current {
				specific[CommandWireFormatJSON] = quality
			}
		case "*/*", "application/*":
			if quality > wildcardQuality {
				wildcardQuality = quality
			}
		}
	}
	bestFormat := fallback
	bestQuality := -1.0
	for _, format := range commandWireAcceptPreference(fallback) {
		quality, ok := specific[format]
		if !ok {
			quality = wildcardQuality
		}
		if quality > bestQuality && quality > 0 {
			bestFormat = format
			bestQuality = quality
		}
	}
	return bestFormat
}

func parseCommandWireAccept(value string) (string, float64) {
	parts := strings.Split(value, ";")
	mediaType := strings.TrimSpace(parts[0])
	if mediaType == "" {
		return "", 0
	}
	quality := 1.0
	for _, part := range parts[1:] {
		key, raw, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
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
	if format == CommandWireFormatProtobuf {
		message, err := cacheCommandRequestToProto(request)
		if err != nil {
			return nil, "", "", err
		}
		data, err := proto.Marshal(message)
		if err != nil {
			return nil, "", "", err
		}
		body, contentEncoding, err := jsonwire.EncodedRequestBody(data, compressionThreshold)
		if err != nil {
			return nil, "", "", err
		}
		return body, commandWireContentTypeProtobuf, contentEncoding, nil
	}

	body, contentEncoding, err := jsonwire.RequestBody(request, estimatedJSONSize, compressionThreshold)
	if err != nil {
		return nil, "", "", err
	}
	return body, commandWireContentTypeJSON, contentEncoding, nil
}

func decodeCommandResponseWire(reader io.Reader, contentType string, limit int64) (CacheCommandResponse, error) {
	format, ok := commandWireFormatFromContentType(contentType)
	if !ok {
		return CacheCommandResponse{}, errors.New("hatriecache: unsupported command response content type")
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
		return CacheCommandResponse{
			OK:      response.GetOk(),
			Message: response.GetMessage(),
			Value:   response.GetValue(),
		}, nil
	}
	return decodeCommandResponseJSON(reader, limit)
}

func decodeCommandResponseJSON(reader io.Reader, limit int64) (CacheCommandResponse, error) {
	limited := &io.LimitedReader{R: reader, N: limit + 1}
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

func readLimitedCommandWire(reader io.Reader, limit int64) ([]byte, error) {
	limited := &io.LimitedReader{R: reader, N: limit + 1}
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
	format := commandWireFormatFromAccept(r.Header.Get("Accept"), fallback)
	if format != CommandWireFormatProtobuf {
		writeJSONStatus(w, status, response)
		return
	}
	data, err := proto.Marshal(grpcCommandResponse(response))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", commandWireContentTypeProtobuf)
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func cacheCommandRequestToProto(request CacheCommandRequest) (*hatriecachev1.CommandRequest, error) {
	out := &hatriecachev1.CommandRequest{
		Command:     request.Command,
		Key:         request.Key,
		Value:       request.Value,
		Subkey:      request.Subkey,
		TtlSeconds:  request.TTLSeconds,
		UnixSeconds: request.UnixSeconds,
		Priority:    request.Priority,
	}
	if len(request.Values) > 0 {
		out.Values = make([]string, len(request.Values))
		for idx, value := range request.Values {
			text, ok := commandWireScalar(value)
			if !ok {
				return nil, fmt.Errorf("hatriecache: command value %d cannot be encoded as protobuf", idx)
			}
			out.Values[idx] = text
		}
	}
	if len(request.Pairs) > 0 {
		out.Pairs = make(map[string]string, len(request.Pairs))
		for key, value := range request.Pairs {
			text, ok := commandWireScalar(value)
			if !ok {
				return nil, fmt.Errorf("hatriecache: command pair %q cannot be encoded as protobuf", key)
			}
			out.Pairs[key] = text
		}
	}
	return out, nil
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
