package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	// ErrTupleCompressionInvalid reports invalid codec options or a nil codec.
	ErrTupleCompressionInvalid = errors.New("hatDataStructure: invalid tuple compression")
	// ErrTupleCompressionTooLarge reports a tuple that exceeds the configured bound.
	ErrTupleCompressionTooLarge = errors.New("hatDataStructure: tuple is too large")
	// ErrTupleCompressionCorrupt reports a malformed, truncated, or checksum-invalid frame.
	ErrTupleCompressionCorrupt = errors.New("hatDataStructure: corrupt tuple compression frame")
	// ErrTupleCompressionAlgorithm reports an unsupported frame or option algorithm.
	ErrTupleCompressionAlgorithm = errors.New("hatDataStructure: unsupported tuple compression algorithm")
	// ErrTupleCompressionClosed reports use after TupleCompressor.Close.
	ErrTupleCompressionClosed = errors.New("hatDataStructure: tuple compressor is closed")
)

const (
	// DefaultTupleCompressionMinSize avoids spending compression CPU on tiny tuples.
	DefaultTupleCompressionMinSize = 256
	// DefaultTupleCompressionMinSavingsBytes accounts for the compression decision
	// and keeps marginally smaller frames on the raw path.
	DefaultTupleCompressionMinSavingsBytes = 16
	// DefaultTupleCompressionMaxSize bounds decompression allocations and frame sizes.
	DefaultTupleCompressionMaxSize = 64 << 20

	tupleCompressionHeaderSize = 20
	tupleCompressionVersion    = 1
)

var tupleCompressionMagic = [4]byte{'H', 'T', 'C', '1'}

// TupleCompressionAlgorithm identifies the payload encoding in a frame.
type TupleCompressionAlgorithm uint8

const (
	// TupleCompressionAuto selects the default adaptive Zstandard policy.
	TupleCompressionAuto TupleCompressionAlgorithm = iota
	// TupleCompressionNone stores the tuple without compression inside a frame.
	TupleCompressionNone
	// TupleCompressionZSTD stores tuples compressed with Zstandard when worthwhile.
	TupleCompressionZSTD
)

// String returns a stable algorithm name.
func (algorithm TupleCompressionAlgorithm) String() string {
	switch algorithm {
	case TupleCompressionAuto:
		return "auto"
	case TupleCompressionNone:
		return "none"
	case TupleCompressionZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

// TupleCompressionOptions configures adaptive per-tuple compression.
type TupleCompressionOptions struct {
	Algorithm       TupleCompressionAlgorithm
	MinSize         int
	MinSavingsBytes int
	MaxTupleSize    int
}

// DefaultTupleCompressionOptions returns an adaptive, bounded Zstandard policy.
func DefaultTupleCompressionOptions() TupleCompressionOptions {
	return TupleCompressionOptions{
		Algorithm:       TupleCompressionAuto,
		MinSize:         DefaultTupleCompressionMinSize,
		MinSavingsBytes: DefaultTupleCompressionMinSavingsBytes,
		MaxTupleSize:    DefaultTupleCompressionMaxSize,
	}
}

func normalizeTupleCompressionOptions(options TupleCompressionOptions) (TupleCompressionOptions, error) {
	defaults := DefaultTupleCompressionOptions()
	if options.Algorithm == TupleCompressionAuto {
		options.Algorithm = TupleCompressionZSTD
	}
	if options.MinSize == 0 {
		options.MinSize = defaults.MinSize
	}
	if options.MinSavingsBytes == 0 {
		options.MinSavingsBytes = defaults.MinSavingsBytes
	}
	if options.MaxTupleSize == 0 {
		options.MaxTupleSize = defaults.MaxTupleSize
	}
	if options.Algorithm != TupleCompressionNone && options.Algorithm != TupleCompressionZSTD {
		return TupleCompressionOptions{}, ErrTupleCompressionAlgorithm
	}
	if options.MinSize < 0 || options.MinSavingsBytes < 0 || options.MaxTupleSize <= 0 {
		return TupleCompressionOptions{}, ErrTupleCompressionInvalid
	}
	if uint64(options.MaxTupleSize) > uint64(^uint32(0)) {
		return TupleCompressionOptions{}, ErrTupleCompressionTooLarge
	}
	return options, nil
}

// TupleCompressionFrameInfo describes a validated frame without decoding it.
type TupleCompressionFrameInfo struct {
	Algorithm    TupleCompressionAlgorithm
	OriginalSize int
	PayloadSize  int
	FrameSize    int
}

// TupleCompressor adaptively compresses bounded individual tuple payloads.
// Compress and Decompress are safe for concurrent callers. The zero options
// value uses DefaultTupleCompressionOptions.
type TupleCompressor struct {
	mu      sync.Mutex
	options TupleCompressionOptions
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	scratch []byte
	closed  bool
}

// NewTupleCompressor creates a reusable tuple compressor.
func NewTupleCompressor(options TupleCompressionOptions) (*TupleCompressor, error) {
	options, err := normalizeTupleCompressionOptions(options)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTupleCompressionInvalid, err)
	}
	compressor := &TupleCompressor{options: options}
	if options.Algorithm == TupleCompressionZSTD {
		compressor.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("%w: create encoder: %v", ErrTupleCompressionInvalid, err)
		}
		compressor.decoder, err = zstd.NewReader(nil)
		if err != nil {
			_ = compressor.encoder.Close()
			return nil, fmt.Errorf("%w: create decoder: %v", ErrTupleCompressionInvalid, err)
		}
	}
	return compressor, nil
}

// Options returns the normalized codec options.
func (compressor *TupleCompressor) Options() TupleCompressionOptions {
	if compressor == nil {
		return TupleCompressionOptions{}
	}
	compressor.mu.Lock()
	defer compressor.mu.Unlock()
	return compressor.options
}

// Close releases reusable compression resources. It is safe to call more than once.
func (compressor *TupleCompressor) Close() error {
	if compressor == nil {
		return nil
	}
	compressor.mu.Lock()
	defer compressor.mu.Unlock()
	if compressor.closed {
		return nil
	}
	compressor.closed = true
	var closeErr error
	if compressor.encoder != nil {
		closeErr = errors.Join(closeErr, compressor.encoder.Close())
		compressor.encoder = nil
	}
	if compressor.decoder != nil {
		compressor.decoder.Close()
		compressor.decoder = nil
	}
	compressor.scratch = nil
	return closeErr
}

// Compress returns a self-describing HTC1 frame. In adaptive mode, the raw
// payload is selected when Zstandard does not save at least MinSavingsBytes.
func (compressor *TupleCompressor) Compress(tuple []byte) ([]byte, error) {
	if compressor == nil {
		return nil, ErrTupleCompressionInvalid
	}
	compressor.mu.Lock()
	defer compressor.mu.Unlock()
	if compressor.closed {
		return nil, ErrTupleCompressionClosed
	}
	if len(tuple) > compressor.options.MaxTupleSize {
		return nil, ErrTupleCompressionTooLarge
	}

	algorithm := TupleCompressionNone
	payload := tuple
	if compressor.options.Algorithm == TupleCompressionZSTD && len(tuple) >= compressor.options.MinSize {
		if tupleCompressionLikelyCompressible(tuple) {
			compressed := compressor.encoder.EncodeAll(tuple, compressor.scratch[:0])
			compressor.scratch = compressed[:0]
			if len(compressed)+compressor.options.MinSavingsBytes <= len(tuple) {
				algorithm = TupleCompressionZSTD
				payload = compressed
			}
		}
	}
	return marshalTupleCompressionFrame(algorithm, tuple, payload), nil
}

const (
	tupleCompressionEntropySampleSize = 512
	tupleCompressionMaxDistinctBytes  = 96
)

// tupleCompressionLikelyCompressible cheaply rejects high-entropy tuples before
// Zstandard allocates a temporary output buffer.
func tupleCompressionLikelyCompressible(tuple []byte) bool {
	sampleSize := len(tuple)
	if sampleSize > tupleCompressionEntropySampleSize {
		sampleSize = tupleCompressionEntropySampleSize
	}
	var seen [4]uint64
	distinct := 0
	for index := 0; index < sampleSize; index++ {
		position := index
		if len(tuple) > sampleSize {
			position = index * (len(tuple) - 1) / (sampleSize - 1)
		}
		value := tuple[position]
		word := value >> 6
		bit := uint64(1) << uint(value&63)
		if seen[word]&bit == 0 {
			seen[word] |= bit
			distinct++
			if distinct > tupleCompressionMaxDistinctBytes {
				return false
			}
		}
	}
	return true
}

// Decompress validates and decodes an HTC1 frame, returning a new byte slice.
func (compressor *TupleCompressor) Decompress(frame []byte) ([]byte, error) {
	if compressor == nil {
		return nil, ErrTupleCompressionInvalid
	}
	compressor.mu.Lock()
	defer compressor.mu.Unlock()
	if compressor.closed {
		return nil, ErrTupleCompressionClosed
	}
	info, err := inspectTupleCompressionFrame(frame)
	if err != nil {
		return nil, err
	}
	if info.OriginalSize > compressor.options.MaxTupleSize {
		return nil, ErrTupleCompressionTooLarge
	}
	if info.PayloadSize > compressor.options.MaxTupleSize {
		return nil, ErrTupleCompressionTooLarge
	}
	payload := frame[tupleCompressionHeaderSize:]
	var decoded []byte
	switch info.Algorithm {
	case TupleCompressionNone:
		if info.PayloadSize != info.OriginalSize {
			return nil, ErrTupleCompressionCorrupt
		}
		decoded = append([]byte(nil), payload...)
	case TupleCompressionZSTD:
		if compressor.decoder == nil {
			return nil, ErrTupleCompressionAlgorithm
		}
		decoded, err = compressor.decoder.DecodeAll(payload, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: zstd decode: %v", ErrTupleCompressionCorrupt, err)
		}
	default:
		return nil, ErrTupleCompressionAlgorithm
	}
	if len(decoded) != info.OriginalSize || crc32.ChecksumIEEE(decoded) != binary.LittleEndian.Uint32(frame[16:20]) {
		return nil, ErrTupleCompressionCorrupt
	}
	return decoded, nil
}

// InspectTupleCompressionFrame validates an HTC1 frame and returns its sizes.
func InspectTupleCompressionFrame(frame []byte) (TupleCompressionFrameInfo, error) {
	return inspectTupleCompressionFrame(frame)
}

func inspectTupleCompressionFrame(frame []byte) (TupleCompressionFrameInfo, error) {
	if len(frame) < tupleCompressionHeaderSize || !equalTupleCompressionMagic(frame[:4]) {
		return TupleCompressionFrameInfo{}, ErrTupleCompressionCorrupt
	}
	if frame[5] != tupleCompressionVersion || binary.LittleEndian.Uint16(frame[6:8]) != tupleCompressionHeaderSize {
		return TupleCompressionFrameInfo{}, ErrTupleCompressionCorrupt
	}
	algorithm := TupleCompressionAlgorithm(frame[4])
	if algorithm != TupleCompressionNone && algorithm != TupleCompressionZSTD {
		return TupleCompressionFrameInfo{}, ErrTupleCompressionAlgorithm
	}
	originalSize := uint64(binary.LittleEndian.Uint32(frame[8:12]))
	payloadSize := uint64(binary.LittleEndian.Uint32(frame[12:16]))
	if payloadSize != uint64(len(frame)-tupleCompressionHeaderSize) || originalSize > uint64(^uint(0)>>1) {
		return TupleCompressionFrameInfo{}, ErrTupleCompressionCorrupt
	}
	return TupleCompressionFrameInfo{
		Algorithm:    algorithm,
		OriginalSize: int(originalSize),
		PayloadSize:  int(payloadSize),
		FrameSize:    len(frame),
	}, nil
}

func equalTupleCompressionMagic(magic []byte) bool {
	return len(magic) == len(tupleCompressionMagic) && magic[0] == tupleCompressionMagic[0] && magic[1] == tupleCompressionMagic[1] && magic[2] == tupleCompressionMagic[2] && magic[3] == tupleCompressionMagic[3]
}

func marshalTupleCompressionFrame(algorithm TupleCompressionAlgorithm, tuple, payload []byte) []byte {
	frame := make([]byte, tupleCompressionHeaderSize+len(payload))
	copy(frame[:4], tupleCompressionMagic[:])
	frame[4] = byte(algorithm)
	frame[5] = tupleCompressionVersion
	binary.LittleEndian.PutUint16(frame[6:8], tupleCompressionHeaderSize)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(tuple)))
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[16:20], crc32.ChecksumIEEE(tuple))
	copy(frame[tupleCompressionHeaderSize:], payload)
	return frame
}
