package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// MaxAggregateStateRegistryCodecs bounds caller-registered aggregate kinds.
	MaxAggregateStateRegistryCodecs = 128
)

var (
	ErrAggregateStateRegistryNil           = errors.New("hatriecache: aggregate state registry is nil")
	ErrAggregateStateRegistryCodecInvalid  = errors.New("hatriecache: aggregate state codec is invalid")
	ErrAggregateStateRegistryCodecExists   = errors.New("hatriecache: aggregate state codec already exists")
	ErrAggregateStateRegistryCodecMissing  = errors.New("hatriecache: aggregate state codec is not registered")
	ErrAggregateStateRegistryCodecMismatch = errors.New("hatriecache: aggregate state codec does not match wire metadata")
	ErrAggregateStateRegistryValueMismatch = errors.New("hatriecache: aggregate state value does not match codec")
	ErrAggregateStateRegistryLimit         = errors.New("hatriecache: aggregate state registry limit exceeded")
)

type aggregateStateRegistryKey struct {
	kind    string
	version uint64
}

// AggregateStateCodec connects one aggregate kind/version to typed functions.
// Encode must return a complete HAG1 AggregateStateEnvelope. Decode receives
// the same complete wire representation, so codecs can enforce their own
// payload validation before returning a typed value.
type AggregateStateCodec struct {
	Kind    string
	Version uint64
	Encode  func(value any) ([]byte, error)
	Decode  func(data []byte) (any, error)
}

// AggregateStateRegistry maps bounded versioned wire metadata to typed codecs.
// It has no package-global state and is safe to share after construction.
type AggregateStateRegistry struct {
	mu     sync.RWMutex
	codecs map[aggregateStateRegistryKey]AggregateStateCodec
}

// NewAggregateStateRegistry creates an empty registry. Existing typed state
// APIs remain available without constructing a registry.
func NewAggregateStateRegistry() *AggregateStateRegistry {
	return &AggregateStateRegistry{codecs: make(map[aggregateStateRegistryKey]AggregateStateCodec)}
}

// NewDefaultAggregateStateRegistry registers the built-in HLL and t-digest
// codecs. Callers may add application-defined codecs after construction.
func NewDefaultAggregateStateRegistry() (*AggregateStateRegistry, error) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(AggregateStateCodec{
		Kind:    AggregateStateKindHyperLogLog,
		Version: hyperLogLogAggregateStateVersion,
		Encode: func(value any) ([]byte, error) {
			hll, ok := value.(HyperLogLog)
			if !ok {
				return nil, ErrAggregateStateRegistryValueMismatch
			}
			return hll.MarshalAggregateState()
		},
		Decode: func(data []byte) (any, error) {
			return NewHyperLogLogFromAggregateState(data)
		},
	}); err != nil {
		return nil, err
	}
	if err := registry.Register(AggregateStateCodec{
		Kind:    AggregateStateKindTDigest,
		Version: tdigestAggregateStateVersion,
		Encode: func(value any) ([]byte, error) {
			digest, ok := value.(TDigest)
			if !ok {
				return nil, ErrAggregateStateRegistryValueMismatch
			}
			return digest.MarshalAggregateState()
		},
		Decode: func(data []byte) (any, error) {
			return NewTDigestFromAggregateState(data)
		},
	}); err != nil {
		return nil, err
	}
	return registry, nil
}

// Register adds one exact kind/version codec. Duplicate keys are rejected so
// a late registration cannot silently change decoding semantics.
func (registry *AggregateStateRegistry) Register(codec AggregateStateCodec) error {
	if registry == nil {
		return ErrAggregateStateRegistryNil
	}
	if err := validateAggregateStateMetadata(codec.Kind, codec.Version); err != nil {
		return fmt.Errorf("%w: %v", ErrAggregateStateRegistryCodecInvalid, err)
	}
	if codec.Encode == nil || codec.Decode == nil {
		return fmt.Errorf("%w: encode and decode functions are required", ErrAggregateStateRegistryCodecInvalid)
	}
	key := aggregateStateRegistryKey{kind: codec.Kind, version: codec.Version}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, ok := registry.codecs[key]; ok {
		return ErrAggregateStateRegistryCodecExists
	}
	if registry.codecs == nil {
		registry.codecs = make(map[aggregateStateRegistryKey]AggregateStateCodec)
	}
	if len(registry.codecs) >= MaxAggregateStateRegistryCodecs {
		return ErrAggregateStateRegistryLimit
	}
	registry.codecs[key] = codec
	return nil
}

// CodecCount returns the number of registered exact kind/version codecs.
func (registry *AggregateStateRegistry) CodecCount() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	count := len(registry.codecs)
	registry.mu.RUnlock()
	return count
}

// Marshal encodes one typed value through the exact registered codec and
// verifies that the returned HAG1 metadata matches the requested key.
func (registry *AggregateStateRegistry) Marshal(kind string, version uint64, value any) ([]byte, error) {
	codec, err := registry.codec(kind, version)
	if err != nil {
		return nil, err
	}
	wire, err := codec.Encode(value)
	if err != nil {
		return nil, err
	}
	envelope, err := UnmarshalAggregateStateEnvelope(wire)
	if err != nil {
		return nil, err
	}
	if envelope.Kind != kind || envelope.Version != version {
		return nil, ErrAggregateStateRegistryCodecMismatch
	}
	return wire, nil
}

// Unmarshal validates a complete HAG1 envelope, selects its exact registered
// codec, and returns both metadata and the typed value.
func (registry *AggregateStateRegistry) Unmarshal(data []byte) (AggregateStateEnvelope, any, error) {
	if registry == nil {
		return AggregateStateEnvelope{}, nil, ErrAggregateStateRegistryNil
	}
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return AggregateStateEnvelope{}, nil, err
	}
	codec, err := registry.codec(envelope.Kind, envelope.Version)
	if err != nil {
		return AggregateStateEnvelope{}, nil, err
	}
	value, err := codec.Decode(data)
	if err != nil {
		return AggregateStateEnvelope{}, nil, err
	}
	return envelope, value, nil
}

func (registry *AggregateStateRegistry) codec(kind string, version uint64) (AggregateStateCodec, error) {
	if registry == nil {
		return AggregateStateCodec{}, ErrAggregateStateRegistryNil
	}
	key := aggregateStateRegistryKey{kind: kind, version: version}
	registry.mu.RLock()
	codec, ok := registry.codecs[key]
	registry.mu.RUnlock()
	if !ok {
		return AggregateStateCodec{}, fmt.Errorf("%w: %s v%d", ErrAggregateStateRegistryCodecMissing, kind, version)
	}
	return codec, nil
}
