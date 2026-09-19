package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrAggregateStateCodecInvalid indicates an incomplete or invalid codec
	// definition.
	ErrAggregateStateCodecInvalid = errors.New("hatriecache: aggregate state codec is invalid")
	// ErrAggregateStateCodecExists indicates a duplicate kind/version pair.
	ErrAggregateStateCodecExists = errors.New("hatriecache: aggregate state codec already exists")
	// ErrAggregateStateCodecMissing indicates that no codec is registered for a
	// kind.
	ErrAggregateStateCodecMissing = errors.New("hatriecache: aggregate state codec is not registered")
	// ErrAggregateStateCodecVersionMismatch indicates a registered kind with no
	// decoder for the envelope version.
	ErrAggregateStateCodecVersionMismatch = errors.New("hatriecache: aggregate state codec version mismatch")
	// ErrAggregateStateCodecEncode indicates that a registered encoder rejected
	// a state value.
	ErrAggregateStateCodecEncode = errors.New("hatriecache: aggregate state codec encode failed")
	// ErrAggregateStateCodecDecode indicates that a registered decoder rejected
	// an envelope payload.
	ErrAggregateStateCodecDecode = errors.New("hatriecache: aggregate state codec decode failed")
)

// AggregateStateEncodeFunc converts one typed partial state into its payload.
// The registry supplies the HAG1 envelope and checksum around the returned
// bytes.
type AggregateStateEncodeFunc func(state interface{}) ([]byte, error)

// AggregateStateDecodeFunc reconstructs one typed partial state from an
// already validated and detached HAG1 payload.
type AggregateStateDecodeFunc func(payload []byte) (interface{}, error)

// AggregateStateCodec binds one aggregate kind and schema version to typed
// encode/decode callbacks.
type AggregateStateCodec struct {
	Kind    string
	Version uint64
	Encode  AggregateStateEncodeFunc
	Decode  AggregateStateDecodeFunc
}

// NewAggregateStateCodec validates a kind/version pair and its callbacks.
func NewAggregateStateCodec(kind string, version uint64, encode AggregateStateEncodeFunc, decode AggregateStateDecodeFunc) (AggregateStateCodec, error) {
	if err := validateAggregateStateMetadata(kind, version); err != nil {
		return AggregateStateCodec{}, fmt.Errorf("%w: %v", ErrAggregateStateCodecInvalid, err)
	}
	if encode == nil || decode == nil {
		return AggregateStateCodec{}, fmt.Errorf("%w: encode and decode callbacks are required", ErrAggregateStateCodecInvalid)
	}
	return AggregateStateCodec{Kind: kind, Version: version, Encode: encode, Decode: decode}, nil
}

type aggregateStateCodecKey struct {
	kind    string
	version uint64
}

// AggregateStateRegistry dispatches HAG1 envelopes to typed aggregate
// codecs. Registration and decode are safe for concurrent use; a registry is
// opt-in and does not add work to direct envelope callers.
type AggregateStateRegistry struct {
	mu     sync.RWMutex
	codecs map[aggregateStateCodecKey]AggregateStateCodec
}

// NewAggregateStateRegistry returns an empty registry.
func NewAggregateStateRegistry() *AggregateStateRegistry {
	return &AggregateStateRegistry{codecs: make(map[aggregateStateCodecKey]AggregateStateCodec)}
}

// Register adds a kind/version codec and rejects duplicate definitions.
func (registry *AggregateStateRegistry) Register(codec AggregateStateCodec) error {
	if registry == nil {
		return ErrAggregateStateCodecInvalid
	}
	validated, err := NewAggregateStateCodec(codec.Kind, codec.Version, codec.Encode, codec.Decode)
	if err != nil {
		return err
	}
	key := aggregateStateCodecKey{kind: validated.Kind, version: validated.Version}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.codecs == nil {
		registry.codecs = make(map[aggregateStateCodecKey]AggregateStateCodec)
	}
	if _, exists := registry.codecs[key]; exists {
		return fmt.Errorf("%w: %s v%d", ErrAggregateStateCodecExists, key.kind, key.version)
	}
	registry.codecs[key] = validated
	return nil
}

// Encode applies the exact registered codec and wraps its payload in the
// existing bounded, checksummed HAG1 envelope.
func (registry *AggregateStateRegistry) Encode(kind string, version uint64, state interface{}) ([]byte, error) {
	codec, err := registry.lookup(kind, version)
	if err != nil {
		return nil, err
	}
	payload, err := codec.Encode(state)
	if err != nil {
		return nil, fmt.Errorf("%w: %s v%d: %v", ErrAggregateStateCodecEncode, codec.Kind, codec.Version, err)
	}
	return MarshalAggregateStateEnvelope(codec.Kind, codec.Version, payload)
}

// Decode validates one HAG1 envelope, selects its exact kind/version codec,
// and passes a detached bounded payload to the registered decoder.
func (registry *AggregateStateRegistry) Decode(data []byte) (AggregateStateEnvelope, interface{}, error) {
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return AggregateStateEnvelope{}, nil, err
	}
	codec, err := registry.lookup(envelope.Kind, envelope.Version)
	if err != nil {
		return envelope, nil, err
	}
	value, err := codec.Decode(envelope.Payload)
	if err != nil {
		return envelope, nil, fmt.Errorf("%w: %s v%d: %v", ErrAggregateStateCodecDecode, codec.Kind, codec.Version, err)
	}
	return envelope, value, nil
}

func (registry *AggregateStateRegistry) lookup(kind string, version uint64) (AggregateStateCodec, error) {
	if registry == nil {
		return AggregateStateCodec{}, ErrAggregateStateCodecMissing
	}
	if err := validateAggregateStateMetadata(kind, version); err != nil {
		return AggregateStateCodec{}, fmt.Errorf("%w: %v", ErrAggregateStateCodecInvalid, err)
	}
	key := aggregateStateCodecKey{kind: kind, version: version}
	registry.mu.RLock()
	codec, exists := registry.codecs[key]
	if !exists {
		for registeredKey := range registry.codecs {
			if registeredKey.kind == kind {
				registry.mu.RUnlock()
				return AggregateStateCodec{}, fmt.Errorf("%w: %s encoded=%d registered=%d", ErrAggregateStateCodecVersionMismatch, kind, version, registeredKey.version)
			}
		}
	}
	registry.mu.RUnlock()
	if !exists {
		return AggregateStateCodec{}, fmt.Errorf("%w: %s", ErrAggregateStateCodecMissing, kind)
	}
	return codec, nil
}
