package hatSql

import (
	"encoding/binary"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func mz015DescriptorBytes(t *testing.T, requiredExtra bool, reuseType descriptorpb.FieldDescriptorProto_Type) []byte {
	t.Helper()
	syntax := "proto3"
	if requiredExtra {
		syntax = "proto2"
	}
	first := &descriptorpb.DescriptorProto{
		Name: proto.String("Event"),
		Field: []*descriptorpb.FieldDescriptorProto{{
			Name:   proto.String("id"),
			Number: proto.Int32(1),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
		}},
	}
	if requiredExtra {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if requiredExtra {
			label = descriptorpb.FieldDescriptorProto_LABEL_REQUIRED
		}
		first.Field = append(first.Field, &descriptorpb.FieldDescriptorProto{
			Name:   proto.String("extra"),
			Number: proto.Int32(2),
			Label:  label.Enum(),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		})
	}
	if reuseType == descriptorpb.FieldDescriptorProto_TYPE_DOUBLE {
		first.Field[0].Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:        proto.String("events.proto"),
		Package:     proto.String("mz015.test"),
		Syntax:      proto.String(syntax),
		MessageType: []*descriptorpb.DescriptorProto{first},
	}}}
	bytes, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func mz015NestedDescriptorBytes(t *testing.T) []byte {
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("nested.proto"),
		Package: proto.String("mz015.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("First")},
			{
				Name:       proto.String("Second"),
				NestedType: []*descriptorpb.DescriptorProto{{Name: proto.String("Inner")}},
			},
		},
	}}}
	bytes, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func mz015ConfluentProtobufPayload(schemaID uint32, indexes []int, datum []byte) []byte {
	payload := make([]byte, 5)
	binary.BigEndian.PutUint32(payload[1:5], schemaID)
	if len(indexes) == 1 && indexes[0] == 0 {
		payload = append(payload, 0)
	} else {
		var encoded [10]byte
		n := binary.PutUvarint(encoded[:], uint64(len(indexes))<<1)
		payload = append(payload, encoded[:n]...)
		for _, index := range indexes {
			n = binary.PutUvarint(encoded[:], uint64(index)<<1)
			payload = append(payload, encoded[:n]...)
		}
	}
	return append(payload, datum...)
}

func TestMZ015ProtobufSchemaRegistryParsesConfluentIndexes(t *testing.T) {
	payload := mz015ConfluentProtobufPayload(7, []int{1, 0}, []byte{8, 1})
	schemaID, indexes, datum, err := ParseConfluentProtobufPayload(payload)
	if err != nil || schemaID != 7 || !reflect.DeepEqual(indexes, []int{1, 0}) || !reflect.DeepEqual(datum, []byte{8, 1}) {
		t.Fatalf("parsed payload = %d/%v/%v/%v", schemaID, indexes, datum, err)
	}
	legacy := []byte{0, 0, 0, 0, 7, 2, 1, 0, 8, 1}
	_, indexes, datum, err = ParseConfluentProtobufPayloadWithEncoding(legacy, ProtobufIndexEncodingUnsigned)
	if err != nil || !reflect.DeepEqual(indexes, []int{1, 0}) || !reflect.DeepEqual(datum, []byte{8, 1}) {
		t.Fatalf("legacy parsed payload = %v/%v/%v", indexes, datum, err)
	}
	for _, invalid := range [][]byte{nil, {1, 0, 0, 0, 7}, {0, 0, 0, 0, 7, 1}, {0, 0, 0, 0, 7, 2, 1}} {
		if _, _, _, err := ParseConfluentProtobufPayload(invalid); !errors.Is(err, ErrConfluentProtobufPayloadInvalid) {
			t.Fatalf("invalid payload %v error = %v", invalid, err)
		}
	}
}

func TestMZ015ProtobufSchemaRegistryResolvesNestedMessages(t *testing.T) {
	var fetches int32
	registry, err := NewProtobufSchemaRegistry(ProtobufSchemaRegistryOptions{
		Fetch: func(schemaID uint32) ([]byte, error) {
			atomic.AddInt32(&fetches, 1)
			if schemaID != 7 {
				return nil, ErrProtobufSchemaNotFound
			}
			return mz015NestedDescriptorBytes(t), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	messageType, err := registry.ResolveMessage(7, []int{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(messageType.Descriptor().FullName()); got != "mz015.test.Second.Inner" {
		t.Fatalf("message name = %q", got)
	}
	if _, err := registry.ResolveMessage(7, []int{2}); !errors.Is(err, ErrProtobufMessageIndexInvalid) {
		t.Fatalf("bad message index error = %v", err)
	}
	if _, err := registry.ResolveMessage(7, []int{1, 0}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&fetches); got != 1 {
		t.Fatalf("fetches = %d, want one", got)
	}
}

func TestMZ015ProtobufDecoderAndKafkaSource(t *testing.T) {
	var fetches int32
	registry, err := NewProtobufSchemaRegistry(ProtobufSchemaRegistryOptions{
		Fetch: func(uint32) ([]byte, error) {
			atomic.AddInt32(&fetches, 1)
			return mz015DescriptorBytes(t, false, descriptorpb.FieldDescriptorProto_TYPE_INT32), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewProtobufKafkaTableDecoder(registry, func(_ uint32, indexes []int, messageType protoreflect.MessageType, payload []byte, message KafkaTableMessage) (KafkaTableChange, error) {
		if !reflect.DeepEqual(indexes, []int{0}) || message.Key != "event-1" {
			return KafkaTableChange{}, errors.New("unexpected protobuf envelope")
		}
		messageValue := dynamicpb.NewMessage(messageType.Descriptor())
		if err := proto.Unmarshal(payload, messageValue); err != nil {
			return KafkaTableChange{}, err
		}
		idField := messageType.Descriptor().Fields().ByName("id")
		return KafkaTableChange{Operation: KafkaTableUpsert, Row: Row{"id": messageValue.Get(idField).Int(), "key": message.Key}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	messageType, err := registry.ResolveMessage(7, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	value := dynamicpb.NewMessage(messageType.Descriptor())
	value.Set(messageType.Descriptor().Fields().ByName("id"), protoreflect.ValueOfInt32(42))
	protobufBytes, err := proto.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	framed := mz015ConfluentProtobufPayload(7, []int{0}, protobufBytes)
	change, err := decoder(KafkaTableMessage{Key: "event-1", Value: framed})
	if err != nil || change.Row["id"] != int64(42) {
		t.Fatalf("change = %#v, err = %v", change, err)
	}
	deleteChange, err := decoder(KafkaTableMessage{Key: "event-1", Value: nil})
	if err != nil || deleteChange.Operation != KafkaTableDelete || atomic.LoadInt32(&fetches) != 1 {
		t.Fatalf("delete = %#v, err = %v, fetches = %d", deleteChange, err, fetches)
	}

	source, err := NewKafkaTableSource(KafkaTableSourceOptions{Source: "protobuf", Table: "events", Topic: "events", Decoder: decoder})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{TransactionID: "tx-1", Messages: []KafkaTableMessage{{Topic: "events", Partition: "0", Offset: 0, Key: "event-1", Value: framed}}}); err != nil {
		t.Fatal(err)
	}
	rows, err := source.ResolveSQLSource("KAFKA", "events")
	if err != nil || len(rows) != 1 || rows[0]["id"] != int64(42) {
		t.Fatalf("rows = %#v, err = %v", rows, err)
	}
}

func TestMZ015ProtobufSchemaCompatibility(t *testing.T) {
	previous := mz015DescriptorBytes(t, false, descriptorpb.FieldDescriptorProto_TYPE_INT32)
	current := mz015DescriptorBytes(t, false, descriptorpb.FieldDescriptorProto_TYPE_INT32)
	if err := ValidateProtobufSchemaCompatibility(previous, current, ProtobufCompatibilityFull); err != nil {
		t.Fatalf("identical schema error = %v", err)
	}
	added := mz015DescriptorBytes(t, false, descriptorpb.FieldDescriptorProto_TYPE_INT32)
	var addedSet descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(added, &addedSet); err != nil {
		t.Fatal(err)
	}
	addedSet.File[0].MessageType[0].Field = append(addedSet.File[0].MessageType[0].Field, &descriptorpb.FieldDescriptorProto{Name: proto.String("extra"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()})
	added, err := proto.Marshal(&addedSet)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProtobufSchemaCompatibility(previous, added, ProtobufCompatibilityFull); err != nil {
		t.Fatalf("optional addition error = %v", err)
	}
	required := mz015DescriptorBytes(t, true, descriptorpb.FieldDescriptorProto_TYPE_INT32)
	if err := ValidateProtobufSchemaCompatibility(previous, required, ProtobufCompatibilityBackward); !errors.Is(err, ErrProtobufSchemaIncompatible) {
		t.Fatalf("required addition error = %v", err)
	}
	reused := mz015DescriptorBytes(t, false, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE)
	if err := ValidateProtobufSchemaCompatibility(previous, reused, ProtobufCompatibilityBackward); !errors.Is(err, ErrProtobufSchemaIncompatible) {
		t.Fatalf("field reuse error = %v", err)
	}
	if err := ValidateProtobufSchemaCompatibility(nil, current, ProtobufCompatibilityBackward); !errors.Is(err, ErrProtobufSchemaCompatibilityInvalid) {
		t.Fatalf("invalid descriptor error = %v", err)
	}
}

func TestMZ015ProtobufSchemaRegistryLimits(t *testing.T) {
	if _, err := NewProtobufSchemaRegistry(ProtobufSchemaRegistryOptions{}); !errors.Is(err, ErrProtobufSchemaRegistryFetcherRequired) {
		t.Fatalf("missing fetcher error = %v", err)
	}
	registry, err := NewProtobufSchemaRegistry(ProtobufSchemaRegistryOptions{
		MaxSchemaBytes: 4,
		Fetch:          func(uint32) ([]byte, error) { return []byte("large"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Borrow(7); !errors.Is(err, ErrProtobufSchemaTooLarge) {
		t.Fatalf("large schema error = %v", err)
	}
}
