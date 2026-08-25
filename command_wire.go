package hatriecache

import (
	"io"
	"net/http"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/internal/gen/hatriecache/v1"
)

// CommandWireFormat is retained as a root-package alias for compatibility.
type CommandWireFormat = hatCommand.CommandWireFormat

const (
	CommandWireFormatJSON     = hatCommand.CommandWireFormatJSON
	CommandWireFormatProtobuf = hatCommand.CommandWireFormatProtobuf
	DefaultCommandWireFormat  = hatCommand.DefaultCommandWireFormat

	commandWireContentTypeJSON     = hatCommand.ContentTypeJSON
	commandWireContentTypeProtobuf = hatCommand.ContentTypeProtobuf
	maxCommandWireReadLimit        = int64(1<<63 - 2)
)

var (
	ErrUnsupportedCommandResponseContentType = hatCommand.ErrUnsupportedCommandResponseContentType
	ErrUnsupportedCommandWireProtobufValue   = hatCommand.ErrUnsupportedCommandWireProtobufValue
	errCommandWireInvalidLimit               = hatCommand.ErrInvalidWireReadLimit
)

func ParseCommandWireFormat(value string) (CommandWireFormat, error) {
	return hatCommand.ParseCommandWireFormat(value)
}

func commandWireFormatFromContentType(value string) (CommandWireFormat, bool) {
	return hatCommand.FormatFromContentType(value)
}

func commandWireFormatFromAccept(value string, fallback CommandWireFormat) (CommandWireFormat, bool) {
	return hatCommand.FormatFromAccept(value, fallback)
}

func commandRequestBody(request CacheCommandRequest, format CommandWireFormat, estimatedJSONSize int, compressionThreshold int) (io.Reader, string, string, error) {
	return hatCommand.CommandRequestBody(request, format, estimatedJSONSize, compressionThreshold)
}

func CommandRequestBody(request CacheCommandRequest, format CommandWireFormat, estimatedJSONSize int, compressionThreshold int) (io.Reader, string, string, error) {
	return hatCommand.CommandRequestBody(request, format, estimatedJSONSize, compressionThreshold)
}

func decodeCommandResponseWire(reader io.Reader, contentType string, limit int64) (CacheCommandResponse, error) {
	return hatCommand.DecodeCommandResponseWire(reader, contentType, limit)
}

func DecodeCommandResponseWire(reader io.Reader, contentType string, limit int64) (CacheCommandResponse, error) {
	return hatCommand.DecodeCommandResponseWire(reader, contentType, limit)
}

func decodeCommandRequestProto(reader io.Reader, limit int64) (CacheCommandRequest, error) {
	return hatCommand.DecodeRequestProtobuf(reader, limit)
}

func readLimitedCommandWire(reader io.Reader, limit int64) ([]byte, error) {
	return hatCommand.ReadLimitedWire(reader, limit)
}

func writeCommandResponseWire(w http.ResponseWriter, r *http.Request, status int, response CacheCommandResponse, fallback CommandWireFormat) {
	hatCommand.WriteResponseWire(w, r, status, response, fallback)
}

func cacheCommandRequestToProto(request CacheCommandRequest) (*hatriecachev1.CommandRequest, error) {
	return hatCommand.RequestToProto(request)
}

func cacheCommandResponseFromProto(response *hatriecachev1.CommandResponse) CacheCommandResponse {
	return hatCommand.ResponseFromProto(response)
}

func acquireCommandWireBuffer(size int) []byte {
	return hatCommand.AcquireWireBuffer(size)
}

func releaseCommandWireBuffer(data []byte) {
	hatCommand.ReleaseWireBuffer(data)
}

func releaseCommandRequestProto(message *hatriecachev1.CommandRequest) {
	hatCommand.ReleaseRequestProto(message)
}

func releaseCommandResponseProto(message *hatriecachev1.CommandResponse) {
	hatCommand.ReleaseResponseProto(message)
}
