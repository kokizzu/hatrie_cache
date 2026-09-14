package hatPeer

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
)

var (
	// ErrCompactProtocolCompressedPayloadInvalid indicates a malformed gzip
	// payload or unexpected trailing bytes in a compressed frame.
	ErrCompactProtocolCompressedPayloadInvalid = errors.New("hatPeer: compact protocol compressed payload is invalid")
	// ErrCompactProtocolDecompressedPayloadTooLarge indicates that a valid
	// compressed payload exceeded its configured inflation bound.
	ErrCompactProtocolDecompressedPayloadTooLarge = errors.New("hatPeer: compact protocol decompressed payload is too large")
)

const maxCachedCompactGzipWriters = 4

var compactGzipWriterCache = make(chan *gzip.Writer, maxCachedCompactGzipWriters)

func (protocol CompactProtocol) preparePayload(payload []byte, flags byte) ([]byte, byte, error) {
	requested := flags&CompactFrameFlagPayloadCompressed != 0
	if !requested && (protocol.compressPayloadsAbove <= 0 || len(payload) < protocol.compressPayloadsAbove) {
		return payload, flags, nil
	}
	if len(payload) > protocol.maxDecompressedPayloadBytes {
		return nil, flags, ErrCompactProtocolDecompressedPayloadTooLarge
	}
	compressed, err := gzipCompactPayload(payload)
	if err != nil {
		return nil, flags, err
	}
	if !requested && len(compressed) >= len(payload) {
		return payload, flags, nil
	}
	return compressed, flags | CompactFrameFlagPayloadCompressed, nil
}

func (protocol CompactProtocol) decodePayload(flags byte, payload []byte) ([]byte, error) {
	if flags&CompactFrameFlagPayloadCompressed == 0 {
		return payload, nil
	}
	source := bytes.NewReader(payload)
	reader, err := gzip.NewReader(source)
	if err != nil {
		return nil, errors.Join(ErrCompactProtocolCompressedPayloadInvalid, err)
	}
	reader.Multistream(false)
	decoded, readErr := io.ReadAll(io.LimitReader(reader, int64(protocol.maxDecompressedPayloadBytes)+1))
	closeErr := reader.Close()
	if len(decoded) > protocol.maxDecompressedPayloadBytes {
		return nil, ErrCompactProtocolDecompressedPayloadTooLarge
	}
	if readErr != nil {
		return nil, errors.Join(ErrCompactProtocolCompressedPayloadInvalid, readErr)
	}
	if closeErr != nil || source.Len() != 0 {
		return nil, ErrCompactProtocolCompressedPayloadInvalid
	}
	return decoded, nil
}

func gzipCompactPayload(payload []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := acquireCompactGzipWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		releaseCompactGzipWriter(writer)
		return nil, err
	}
	if err := writer.Close(); err != nil {
		releaseCompactGzipWriter(writer)
		return nil, err
	}
	releaseCompactGzipWriter(writer)
	return buffer.Bytes(), nil
}

func acquireCompactGzipWriter(writer io.Writer) *gzip.Writer {
	var gzipWriter *gzip.Writer
	select {
	case gzipWriter = <-compactGzipWriterCache:
	default:
		var err error
		gzipWriter, err = gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		if err != nil {
			panic(err)
		}
	}
	gzipWriter.Reset(writer)
	return gzipWriter
}

func releaseCompactGzipWriter(writer *gzip.Writer) {
	if writer == nil {
		return
	}
	writer.Reset(io.Discard)
	select {
	case compactGzipWriterCache <- writer:
	default:
	}
}
