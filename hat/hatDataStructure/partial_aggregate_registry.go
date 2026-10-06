package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// DefaultMaxAggregateStateRegistryCodecs bounds the zero-configuration
	// registry while leaving room for the common built-in and application kinds.
	DefaultMaxAggregateStateRegistryCodecs = 64
	// MaxAggregateStateRegistryCodecs prevents an accidental unbounded registry.
	MaxAggregateStateRegistryCodecs = 1024
)

var (
	// ErrAggregateStateRegistryInvalid indicates an invalid registry or codec.
	ErrAggregateStateRegistryInvalid = errors.New("hatriecache: aggregate state registry is invalid")
	// ErrAggregateStateRegistryDuplicate indicates a duplicate kind/version.
	ErrAggregateStateRegistryDuplicate = errors.New("hatriecache: aggregate state codec is already registered")
	// ErrAggregateStateRegistryFull indicates that the registry reached its cap.
	ErrAggregateStateRegistryFull = errors.New("hatriecache: aggregate state registry is full")
	// ErrAggregateStateRegistryUnknown indicates an unregistered kind/version.
	ErrAggregateStateRegistryUnknown = errors.New("hatriecache: aggregate state codec is unknown")
	// ErrAggregateStateRegistryCodec indicates a codec callback failure.
	ErrAggregateStateRegistryCodec = errors.New("hatriecache: aggregate state codec failed")
	// ErrAggregateStateRegistryMergeUnsupported indicates that a codec cannot merge.
	ErrAggregateStateRegistryMergeUnsupported = errors.New("hatriecache: aggregate state merge is unsupported")
)

// AggregateStateCodec describes one versioned, mergeable partial aggregate.
// Callbacks run without the registry lock and must not retain mutable payload
// buffers supplied to Decode.
type AggregateStateCodec struct {
	Kind    string
	Version uint64
	Encode  func(value any) ([]byte, error)
	Decode  func(payload []byte) (any, error)
	Merge   func(left, right any) (any, error)
}

// AggregateStateValue is a decoded registered partial aggregate.
type AggregateStateValue struct {
	Kind    string
	Version uint64
	Value   any
}

type aggregateStateCodecKey struct {
	kind    string
	version uint64
}

// AggregateStateRegistry maps bounded HAG1 kind/version envelopes to typed
// application callbacks. Registration is intended for startup/configuration;
// Marshal, Unmarshal, and Merge are safe for concurrent use afterward.
type AggregateStateRegistry struct {
	mu        sync.RWMutex
	codecs    map[aggregateStateCodecKey]AggregateStateCodec
	maxCodecs int
}

// NewAggregateStateRegistry returns a registry with a bounded default size.
func NewAggregateStateRegistry() *AggregateStateRegistry {
	return &AggregateStateRegistry{maxCodecs: DefaultMaxAggregateStateRegistryCodecs}
}

// NewAggregateStateRegistryWithLimit returns a registry with an explicit
// bounded codec count.
func NewAggregateStateRegistryWithLimit(limit int) (*AggregateStateRegistry, error) {
	if limit <= 0 || limit > MaxAggregateStateRegistryCodecs {
		return nil, fmt.Errorf("%w: codec limit %d is outside 1..%d", ErrAggregateStateRegistryInvalid, limit, MaxAggregateStateRegistryCodecs)
	}
	return &AggregateStateRegistry{maxCodecs: limit}, nil
}

// Register adds one immutable kind/version codec to the registry.
func (registry *AggregateStateRegistry) Register(codec AggregateStateCodec) error {
	if registry == nil {
		return ErrAggregateStateRegistryInvalid
	}
	if err := validateAggregateStateMetadata(codec.Kind, codec.Version); err != nil {
		return fmt.Errorf("%w: %v", ErrAggregateStateRegistryInvalid, err)
	}
	if codec.Encode == nil || codec.Decode == nil {
		return fmt.Errorf("%w: encode and decode callbacks are required", ErrAggregateStateRegistryInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.codecs == nil {
		registry.codecs = make(map[aggregateStateCodecKey]AggregateStateCodec)
	}
	key := aggregateStateCodecKey{kind: codec.Kind, version: codec.Version}
	if _, exists := registry.codecs[key]; exists {
		return fmt.Errorf("%w: %s/%d", ErrAggregateStateRegistryDuplicate, codec.Kind, codec.Version)
	}
	limit := registry.maxCodecs
	if limit == 0 {
		limit = DefaultMaxAggregateStateRegistryCodecs
	}
	if len(registry.codecs) >= limit {
		return fmt.Errorf("%w: limit %d", ErrAggregateStateRegistryFull, limit)
	}
	registry.codecs[key] = codec
	return nil
}

// Len returns the number of registered codecs.
func (registry *AggregateStateRegistry) Len() int {
	if registry == nil {
		return 0
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.codecs)
}

// Marshal encodes a typed state into a checksummed HAG1 envelope.
func (registry *AggregateStateRegistry) Marshal(kind string, version uint64, value any) ([]byte, error) {
	codec, ok := registry.lookup(kind, version)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%d", ErrAggregateStateRegistryUnknown, kind, version)
	}
	payload, err := codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode %s/%d: %v", ErrAggregateStateRegistryCodec, kind, version, err)
	}
	return MarshalAggregateStateEnvelope(kind, version, payload)
}

// Unmarshal validates a HAG1 envelope and dispatches its payload to the
// registered kind/version decoder.
func (registry *AggregateStateRegistry) Unmarshal(data []byte) (AggregateStateValue, error) {
	envelope, _, value, err := registry.decode(data)
	if err != nil {
		return AggregateStateValue{}, err
	}
	return AggregateStateValue{Kind: envelope.Kind, Version: envelope.Version, Value: value}, nil
}

// Merge decodes two same-kind/version states, calls the registered merge
// callback, and returns a new checksummed HAG1 envelope.
func (registry *AggregateStateRegistry) Merge(left, right []byte) ([]byte, error) {
	leftEnvelope, leftCodec, leftValue, err := registry.decode(left)
	if err != nil {
		return nil, err
	}
	rightEnvelope, _, rightValue, err := registry.decode(right)
	if err != nil {
		return nil, err
	}
	if leftEnvelope.Kind != rightEnvelope.Kind || leftEnvelope.Version != rightEnvelope.Version {
		return nil, fmt.Errorf("%w: %s/%d and %s/%d", ErrAggregateStateKindMismatch, leftEnvelope.Kind, leftEnvelope.Version, rightEnvelope.Kind, rightEnvelope.Version)
	}
	if leftCodec.Merge == nil {
		return nil, fmt.Errorf("%w: %s/%d", ErrAggregateStateRegistryMergeUnsupported, leftEnvelope.Kind, leftEnvelope.Version)
	}
	merged, err := leftCodec.Merge(leftValue, rightValue)
	if err != nil {
		return nil, fmt.Errorf("%w: merge %s/%d: %v", ErrAggregateStateRegistryCodec, leftEnvelope.Kind, leftEnvelope.Version, err)
	}
	return registry.marshalWithCodec(leftCodec, merged)
}

func (registry *AggregateStateRegistry) decode(data []byte) (AggregateStateEnvelope, AggregateStateCodec, any, error) {
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return AggregateStateEnvelope{}, AggregateStateCodec{}, nil, err
	}
	codec, ok := registry.lookup(envelope.Kind, envelope.Version)
	if !ok {
		return AggregateStateEnvelope{}, AggregateStateCodec{}, nil, fmt.Errorf("%w: %s/%d", ErrAggregateStateRegistryUnknown, envelope.Kind, envelope.Version)
	}
	value, err := codec.Decode(envelope.Payload)
	if err != nil {
		return AggregateStateEnvelope{}, AggregateStateCodec{}, nil, fmt.Errorf("%w: decode %s/%d: %v", ErrAggregateStateRegistryCodec, envelope.Kind, envelope.Version, err)
	}
	return envelope, codec, value, nil
}

func (registry *AggregateStateRegistry) marshalWithCodec(codec AggregateStateCodec, value any) ([]byte, error) {
	payload, err := codec.Encode(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode %s/%d: %v", ErrAggregateStateRegistryCodec, codec.Kind, codec.Version, err)
	}
	return MarshalAggregateStateEnvelope(codec.Kind, codec.Version, payload)
}

func (registry *AggregateStateRegistry) lookup(kind string, version uint64) (AggregateStateCodec, bool) {
	if registry == nil {
		return AggregateStateCodec{}, false
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	codec, ok := registry.codecs[aggregateStateCodecKey{kind: kind, version: version}]
	return codec, ok
}
