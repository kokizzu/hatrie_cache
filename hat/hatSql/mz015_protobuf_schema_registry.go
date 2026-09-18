package hatSql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	// DefaultProtobufSchemaRegistryMaxMessageIndexes bounds one nested message
	// path in the Confluent wire prefix.
	DefaultProtobufSchemaRegistryMaxMessageIndexes = 64
	// MaxProtobufSchemaRegistryMaxMessageIndexes rejects pathological paths.
	MaxProtobufSchemaRegistryMaxMessageIndexes = 128
	// MaxProtobufSchemaRegistryMessageIndex bounds one descriptor lookup index.
	MaxProtobufSchemaRegistryMessageIndex = 1 << 20
)

var (
	// ErrProtobufSchemaRegistryNil reports a nil registry receiver.
	ErrProtobufSchemaRegistryNil = errors.New("Protobuf schema registry is nil")
	// ErrProtobufSchemaRegistryInvalid reports malformed registry configuration.
	ErrProtobufSchemaRegistryInvalid = errors.New("Protobuf schema registry is invalid")
	// ErrProtobufSchemaRegistryFetcherRequired reports a missing schema fetcher.
	ErrProtobufSchemaRegistryFetcherRequired = errors.New("Protobuf schema registry fetcher is required")
	// ErrProtobufSchemaNotFound reports an unknown schema ID.
	ErrProtobufSchemaNotFound = errors.New("Protobuf schema was not found")
	// ErrProtobufSchemaTooLarge reports a schema over the configured byte bound.
	ErrProtobufSchemaTooLarge = errors.New("Protobuf schema is too large")
	// ErrProtobufDescriptorInvalid reports malformed FileDescriptorSet bytes.
	ErrProtobufDescriptorInvalid = errors.New("Protobuf descriptor set is invalid")
	// ErrProtobufMessageIndexInvalid reports an invalid root or nested message
	// index path.
	ErrProtobufMessageIndexInvalid = errors.New("Protobuf message index is invalid")
	// ErrConfluentProtobufPayloadInvalid reports malformed Confluent framing.
	ErrConfluentProtobufPayloadInvalid = errors.New("Confluent Protobuf payload is invalid")
	// ErrProtobufSchemaCompatibilityInvalid reports malformed compatibility
	// inputs.
	ErrProtobufSchemaCompatibilityInvalid = errors.New("Protobuf schema compatibility input is invalid")
	// ErrProtobufSchemaIncompatible reports an incompatible field evolution.
	ErrProtobufSchemaIncompatible = errors.New("Protobuf schemas are incompatible")
	// ErrProtobufPayloadDecoderRequired reports a missing payload decoder.
	ErrProtobufPayloadDecoderRequired = errors.New("Protobuf payload decoder is required")
)

// ProtobufSchemaFetcher retrieves a serialized FileDescriptorSet for one
// Schema Registry ID.
type ProtobufSchemaFetcher func(schemaID uint32) ([]byte, error)

// ProtobufSchemaRegistryOptions configures the bounded descriptor cache.
type ProtobufSchemaRegistryOptions struct {
	Fetch             ProtobufSchemaFetcher
	MaxSchemas        int
	MaxSchemaBytes    int
	MaxMessageIndexes int
}

// ProtobufSchemaRegistryStats is the shared schema-byte cache summary.
type ProtobufSchemaRegistryStats = AvroSchemaRegistryStats

type protobufSchemaBundle struct {
	files    *protoregistry.Files
	root     protoreflect.FileDescriptor
	messages map[protobufMessagePath]protoreflect.MessageType
}

type protobufMessagePath struct {
	length  uint8
	indexes [128]uint32
}

// ProtobufSchemaRegistry caches schema bytes and parsed descriptor sets, then
// resolves Confluent message-index paths to dynamic protobuf message types.
type ProtobufSchemaRegistry struct {
	bytes             *AvroSchemaRegistry
	mu                sync.Mutex
	maxMessageIndexes int
	bundles           map[uint32]*protobufSchemaBundle
}

// NewProtobufSchemaRegistry creates a bounded descriptor registry.
func NewProtobufSchemaRegistry(options ProtobufSchemaRegistryOptions) (*ProtobufSchemaRegistry, error) {
	if options.Fetch == nil {
		return nil, ErrProtobufSchemaRegistryFetcherRequired
	}
	maxMessageIndexes := options.MaxMessageIndexes
	if maxMessageIndexes == 0 {
		maxMessageIndexes = DefaultProtobufSchemaRegistryMaxMessageIndexes
	}
	if maxMessageIndexes < 1 || maxMessageIndexes > MaxProtobufSchemaRegistryMaxMessageIndexes {
		return nil, fmt.Errorf("%w: max message indexes %d", ErrProtobufSchemaRegistryInvalid, maxMessageIndexes)
	}
	cache, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{
		Fetch:          AvroSchemaFetcher(options.Fetch),
		MaxSchemas:     options.MaxSchemas,
		MaxSchemaBytes: options.MaxSchemaBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrProtobufSchemaRegistryInvalid, err)
	}
	return &ProtobufSchemaRegistry{
		bytes:             cache,
		maxMessageIndexes: maxMessageIndexes,
		bundles:           make(map[uint32]*protobufSchemaBundle),
	}, nil
}

// Borrow returns immutable cached descriptor-set bytes without copying.
func (registry *ProtobufSchemaRegistry) Borrow(schemaID uint32) ([]byte, error) {
	if registry == nil {
		return nil, ErrProtobufSchemaRegistryNil
	}
	bytes, err := registry.bytes.Borrow(schemaID)
	return bytes, normalizeProtobufSchemaCacheError(err)
}

// Schema returns an owned descriptor-set byte copy.
func (registry *ProtobufSchemaRegistry) Schema(schemaID uint32) ([]byte, error) {
	if registry == nil {
		return nil, ErrProtobufSchemaRegistryNil
	}
	bytes, err := registry.bytes.Schema(schemaID)
	return bytes, normalizeProtobufSchemaCacheError(err)
}

// Stats returns the schema-byte cache statistics.
func (registry *ProtobufSchemaRegistry) Stats() ProtobufSchemaRegistryStats {
	if registry == nil {
		return ProtobufSchemaRegistryStats{}
	}
	return registry.bytes.Stats()
}

// ResolveMessage resolves a Confluent message-index path. The path starts at
// a root message in the first descriptor-set file and then follows nested
// message declarations.
func (registry *ProtobufSchemaRegistry) ResolveMessage(schemaID uint32, indexes []int) (protoreflect.MessageType, error) {
	if registry == nil {
		return nil, ErrProtobufSchemaRegistryNil
	}
	path, err := registry.normalizeMessagePath(indexes)
	if err != nil {
		return nil, err
	}
	bundle, err := registry.bundle(schemaID)
	if err != nil {
		return nil, err
	}
	registry.mu.Lock()
	messageType, found := bundle.messages[path]
	registry.mu.Unlock()
	if !found {
		descriptor, err := resolveProtobufMessageDescriptor(bundle.root, indexes)
		if err != nil {
			return nil, err
		}
		messageType = dynamicpb.NewMessageType(descriptor)
		registry.mu.Lock()
		if existing, alreadyFound := bundle.messages[path]; alreadyFound {
			messageType = existing
		} else {
			bundle.messages[path] = messageType
		}
		registry.mu.Unlock()
	}
	return messageType, nil
}

// NewMessage creates a dynamic protobuf message for a schema ID and message
// index path.
func (registry *ProtobufSchemaRegistry) NewMessage(schemaID uint32, indexes []int) (*dynamicpb.Message, error) {
	messagingType, err := registry.ResolveMessage(schemaID, indexes)
	if err != nil {
		return nil, err
	}
	return dynamicpb.NewMessage(messagingType.Descriptor()), nil
}

func (registry *ProtobufSchemaRegistry) normalizeMessagePath(indexes []int) (protobufMessagePath, error) {
	if len(indexes) == 0 || len(indexes) > registry.maxMessageIndexes || len(indexes) > len(protobufMessagePath{}.indexes) {
		return protobufMessagePath{}, ErrProtobufMessageIndexInvalid
	}
	var path protobufMessagePath
	path.length = uint8(len(indexes))
	for index, messageIndex := range indexes {
		if messageIndex < 0 || messageIndex > MaxProtobufSchemaRegistryMessageIndex {
			return protobufMessagePath{}, ErrProtobufMessageIndexInvalid
		}
		path.indexes[index] = uint32(messageIndex)
	}
	return path, nil
}

func (registry *ProtobufSchemaRegistry) bundle(schemaID uint32) (*protobufSchemaBundle, error) {
	registry.mu.Lock()
	if bundle, found := registry.bundles[schemaID]; found {
		registry.mu.Unlock()
		return bundle, nil
	}
	registry.mu.Unlock()
	raw, err := registry.Borrow(schemaID)
	if err != nil {
		return nil, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if bundle, found := registry.bundles[schemaID]; found {
		return bundle, nil
	}
	bundle, err := parseProtobufSchemaBundle(raw)
	if err != nil {
		return nil, err
	}
	registry.bundles[schemaID] = bundle
	return bundle, nil
}

func parseProtobufSchemaBundle(raw []byte) (*protobufSchemaBundle, error) {
	var descriptorSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &descriptorSet); err != nil || len(descriptorSet.File) == 0 || descriptorSet.File[0].GetName() == "" {
		return nil, fmt.Errorf("%w: FileDescriptorSet", ErrProtobufDescriptorInvalid)
	}
	files, err := protodesc.NewFiles(&descriptorSet)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtobufDescriptorInvalid, err)
	}
	root, err := files.FindFileByPath(descriptorSet.File[0].GetName())
	if err != nil {
		return nil, fmt.Errorf("%w: root file: %v", ErrProtobufDescriptorInvalid, err)
	}
	return &protobufSchemaBundle{
		files:    files,
		root:     root,
		messages: make(map[protobufMessagePath]protoreflect.MessageType),
	}, nil
}

func resolveProtobufMessageDescriptor(root protoreflect.FileDescriptor, indexes []int) (protoreflect.MessageDescriptor, error) {
	if len(indexes) == 0 {
		return nil, ErrProtobufMessageIndexInvalid
	}
	messages := root.Messages()
	var descriptor protoreflect.MessageDescriptor
	for index, messageIndex := range indexes {
		if messageIndex < 0 || messageIndex >= messages.Len() {
			return nil, fmt.Errorf("%w: index %d at depth %d", ErrProtobufMessageIndexInvalid, messageIndex, index)
		}
		descriptor = messages.Get(messageIndex)
		messages = descriptor.Messages()
	}
	return descriptor, nil
}

func normalizeProtobufSchemaCacheError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrAvroSchemaTooLarge) {
		return fmt.Errorf("%w: %w", ErrProtobufSchemaTooLarge, err)
	}
	return err
}

// ProtobufIndexEncoding selects the message-index integer representation.
type ProtobufIndexEncoding uint8

const (
	// ProtobufIndexEncodingZigZag is the current Confluent encoding.
	ProtobufIndexEncodingZigZag ProtobufIndexEncoding = iota + 1
	// ProtobufIndexEncodingUnsigned supports the deprecated serializer format.
	ProtobufIndexEncodingUnsigned
)

// ParseConfluentProtobufPayload parses the current Confluent Protobuf framing.
func ParseConfluentProtobufPayload(payload []byte) (uint32, []int, []byte, error) {
	return ParseConfluentProtobufPayloadWithEncoding(payload, ProtobufIndexEncodingZigZag)
}

// ParseConfluentProtobufPayloadWithEncoding parses schema ID, message indexes,
// and protobuf datum bytes using the selected index encoding.
func ParseConfluentProtobufPayloadWithEncoding(payload []byte, encoding ProtobufIndexEncoding) (uint32, []int, []byte, error) {
	if len(payload) < 6 || payload[0] != 0 || (encoding != ProtobufIndexEncodingZigZag && encoding != ProtobufIndexEncodingUnsigned) {
		return 0, nil, nil, ErrConfluentProtobufPayloadInvalid
	}
	schemaID := binary.BigEndian.Uint32(payload[1:5])
	if payload[5] == 0 {
		return schemaID, []int{0}, payload[6:], nil
	}
	position := 5
	encodedLength, next, ok := readProtobufUvarint(payload, position)
	if !ok {
		return 0, nil, nil, ErrConfluentProtobufPayloadInvalid
	}
	position = next
	length, ok := decodeProtobufIndex(encodedLength, encoding)
	if !ok || length < 1 || length > MaxProtobufSchemaRegistryMaxMessageIndexes {
		return 0, nil, nil, ErrConfluentProtobufPayloadInvalid
	}
	indexes := make([]int, length)
	for index := range indexes {
		encodedIndex, next, ok := readProtobufUvarint(payload, position)
		if !ok {
			return 0, nil, nil, ErrConfluentProtobufPayloadInvalid
		}
		position = next
		messageIndex, ok := decodeProtobufIndex(encodedIndex, encoding)
		if !ok || messageIndex < 0 || messageIndex > MaxProtobufSchemaRegistryMessageIndex {
			return 0, nil, nil, ErrConfluentProtobufPayloadInvalid
		}
		indexes[index] = messageIndex
	}
	return schemaID, indexes, payload[position:], nil
}

func readProtobufUvarint(payload []byte, position int) (uint64, int, bool) {
	if position < 0 || position >= len(payload) {
		return 0, 0, false
	}
	value, width := binary.Uvarint(payload[position:])
	if width <= 0 {
		return 0, 0, false
	}
	return value, position + width, true
}

func decodeProtobufIndex(value uint64, encoding ProtobufIndexEncoding) (int, bool) {
	if encoding == ProtobufIndexEncodingUnsigned {
		if value > uint64(MaxProtobufSchemaRegistryMessageIndex) {
			return 0, false
		}
		return int(value), true
	}
	decoded := int64(value >> 1)
	if value&1 != 0 {
		decoded = ^decoded
	}
	if decoded < 0 || decoded > MaxProtobufSchemaRegistryMessageIndex {
		return 0, false
	}
	return int(decoded), true
}

// ProtobufPayloadDecoder decodes one schema-resolved protobuf datum. The
// message type is borrowed and must not be mutated.
type ProtobufPayloadDecoder func(schemaID uint32, indexes []int, messageType protoreflect.MessageType, payload []byte, message KafkaTableMessage) (KafkaTableChange, error)

// NewProtobufKafkaTableDecoder creates a KafkaTableDecoder using current
// Confluent zigzag message-index encoding.
func NewProtobufKafkaTableDecoder(registry *ProtobufSchemaRegistry, decode ProtobufPayloadDecoder) (KafkaTableDecoder, error) {
	return NewProtobufKafkaTableDecoderWithEncoding(registry, decode, ProtobufIndexEncodingZigZag)
}

// NewProtobufKafkaTableDecoderWithEncoding creates a decoder with explicit
// support for the current or deprecated Confluent index encoding.
func NewProtobufKafkaTableDecoderWithEncoding(registry *ProtobufSchemaRegistry, decode ProtobufPayloadDecoder, encoding ProtobufIndexEncoding) (KafkaTableDecoder, error) {
	if registry == nil {
		return nil, ErrProtobufSchemaRegistryNil
	}
	if decode == nil {
		return nil, ErrProtobufPayloadDecoderRequired
	}
	if encoding != ProtobufIndexEncodingZigZag && encoding != ProtobufIndexEncodingUnsigned {
		return nil, ErrConfluentProtobufPayloadInvalid
	}
	return func(message KafkaTableMessage) (KafkaTableChange, error) {
		if message.Value == nil {
			return KafkaTableChange{Key: message.Key, Operation: KafkaTableDelete}, nil
		}
		schemaID, indexes, payload, err := ParseConfluentProtobufPayloadWithEncoding(message.Value, encoding)
		if err != nil {
			return KafkaTableChange{}, err
		}
		messageType, err := registry.ResolveMessage(schemaID, indexes)
		if err != nil {
			return KafkaTableChange{}, err
		}
		return decode(schemaID, indexes, messageType, payload, message)
	}, nil
}

// ProtobufSchemaCompatibilityMode selects the reader/writer direction checked
// by ValidateProtobufSchemaCompatibility.
type ProtobufSchemaCompatibilityMode uint8

const (
	ProtobufCompatibilityBackward ProtobufSchemaCompatibilityMode = iota + 1
	ProtobufCompatibilityForward
	ProtobufCompatibilityFull
)

// ValidateProtobufSchemaCompatibility validates field-number, cardinality,
// and wire-kind evolution across descriptor sets. It intentionally rejects
// required-field additions and unsafe field-number reuse.
func ValidateProtobufSchemaCompatibility(previous, current []byte, mode ProtobufSchemaCompatibilityMode) error {
	if mode != ProtobufCompatibilityBackward && mode != ProtobufCompatibilityForward && mode != ProtobufCompatibilityFull {
		return ErrProtobufSchemaCompatibilityInvalid
	}
	previousBundle, err := parseProtobufSchemaBundle(previous)
	if err != nil {
		return fmt.Errorf("%w: previous: %v", ErrProtobufSchemaCompatibilityInvalid, err)
	}
	currentBundle, err := parseProtobufSchemaBundle(current)
	if err != nil {
		return fmt.Errorf("%w: current: %v", ErrProtobufSchemaCompatibilityInvalid, err)
	}
	previousMessages := collectProtobufMessages(previousBundle.files)
	currentMessages := collectProtobufMessages(currentBundle.files)
	for name, previousMessage := range previousMessages {
		currentMessage, found := currentMessages[name]
		if !found {
			return fmt.Errorf("%w: message %q removed", ErrProtobufSchemaIncompatible, name)
		}
		if mode == ProtobufCompatibilityBackward || mode == ProtobufCompatibilityFull {
			if err := validateProtobufReaderAgainstWriter(previousMessage, currentMessage); err != nil {
				return fmt.Errorf("backward %q: %w", name, err)
			}
		}
		if mode == ProtobufCompatibilityForward || mode == ProtobufCompatibilityFull {
			if err := validateProtobufReaderAgainstWriter(currentMessage, previousMessage); err != nil {
				return fmt.Errorf("forward %q: %w", name, err)
			}
		}
	}
	return nil
}

func collectProtobufMessages(files *protoregistry.Files) map[protoreflect.FullName]protoreflect.MessageDescriptor {
	messages := make(map[protoreflect.FullName]protoreflect.MessageDescriptor)
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		collectProtobufMessageList(file.Messages(), messages)
		return true
	})
	return messages
}

func collectProtobufMessageList(list protoreflect.MessageDescriptors, messages map[protoreflect.FullName]protoreflect.MessageDescriptor) {
	for index := 0; index < list.Len(); index++ {
		message := list.Get(index)
		messages[message.FullName()] = message
		collectProtobufMessageList(message.Messages(), messages)
	}
}

func validateProtobufReaderAgainstWriter(writer, reader protoreflect.MessageDescriptor) error {
	writerFields := writer.Fields()
	readerFields := reader.Fields()
	for index := 0; index < writerFields.Len(); index++ {
		writerField := writerFields.Get(index)
		readerField := readerFields.ByNumber(writerField.Number())
		if readerField == nil {
			continue
		}
		if !protobufFieldsCompatible(writerField, readerField) {
			return fmt.Errorf("%w: field number %d", ErrProtobufSchemaIncompatible, writerField.Number())
		}
	}
	for index := 0; index < readerFields.Len(); index++ {
		readerField := readerFields.Get(index)
		if writerFields.ByNumber(readerField.Number()) == nil && readerField.Cardinality() == protoreflect.Required {
			return fmt.Errorf("%w: required field number %d was not present", ErrProtobufSchemaIncompatible, readerField.Number())
		}
	}
	return nil
}

func protobufFieldsCompatible(writer, reader protoreflect.FieldDescriptor) bool {
	if writer.IsList() != reader.IsList() || writer.IsMap() != reader.IsMap() || writer.Cardinality() == protoreflect.Required && reader.Cardinality() != protoreflect.Required {
		return false
	}
	if writer.Kind() == protoreflect.MessageKind || writer.Kind() == protoreflect.GroupKind {
		return (reader.Kind() == protoreflect.MessageKind || reader.Kind() == protoreflect.GroupKind) && writer.Message().FullName() == reader.Message().FullName()
	}
	if writer.Kind() == protoreflect.EnumKind || reader.Kind() == protoreflect.EnumKind {
		return (writer.Kind() == protoreflect.EnumKind && reader.Kind() == protoreflect.EnumKind) || protobufIntegerKind(writer.Kind()) && protobufIntegerKind(reader.Kind())
	}
	if writer.Kind() == reader.Kind() {
		return true
	}
	return protobufIntegerKind(writer.Kind()) && protobufIntegerKind(reader.Kind()) ||
		(writer.Kind() == protoreflect.StringKind && reader.Kind() == protoreflect.BytesKind) ||
		(writer.Kind() == protoreflect.BytesKind && reader.Kind() == protoreflect.StringKind) ||
		(writer.Kind() == protoreflect.Fixed32Kind && reader.Kind() == protoreflect.Sfixed32Kind) ||
		(writer.Kind() == protoreflect.Sfixed32Kind && reader.Kind() == protoreflect.Fixed32Kind) ||
		(writer.Kind() == protoreflect.Fixed64Kind && reader.Kind() == protoreflect.Sfixed64Kind) ||
		(writer.Kind() == protoreflect.Sfixed64Kind && reader.Kind() == protoreflect.Fixed64Kind)
}

func protobufIntegerKind(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind, protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind, protoreflect.BoolKind:
		return true
	default:
		return false
	}
}
