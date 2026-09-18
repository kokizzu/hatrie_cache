package hatSql

import (
	"encoding/binary"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

var mz015ProtobufBenchmarkSink int

func mz015ProtobufBenchmarkDescriptorBytes() []byte {
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("events.proto"),
		Package: proto.String("benchmark"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Event"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
			}},
		}},
	}}}
	bytes, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}
	return bytes
}

func mz015ProtobufBenchmarkPayloads() [][]byte {
	payloads := make([][]byte, 10000)
	for index := range payloads {
		payloads[index] = []byte{0, 0, 0, 0, 7, 0, byte(index), byte(index >> 8)}
	}
	return payloads
}

func mz015ProtobufBenchmarkFetch(schemaID uint32) ([]byte, error) {
	if schemaID != 7 {
		return nil, errors.New("benchmark schema not found")
	}
	return append([]byte(nil), mz015ProtobufBenchmarkDescriptorBytes()...), nil
}

// BenchmarkMZ015ProtobufSchemaRegistryBaseline measures fetching and parsing
// the descriptor set for every record.
func BenchmarkMZ015ProtobufSchemaRegistryBaseline(b *testing.B) {
	payloads := mz015ProtobufBenchmarkPayloads()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := 0
		for _, payload := range payloads {
			var set descriptorpb.FileDescriptorSet
			bytes, err := mz015ProtobufBenchmarkFetch(binary.BigEndian.Uint32(payload[1:5]))
			if err != nil {
				b.Fatal(err)
			}
			if err := proto.Unmarshal(bytes, &set); err != nil {
				b.Fatal(err)
			}
			files, err := protodesc.NewFiles(&set)
			if err != nil {
				b.Fatal(err)
			}
			file, err := files.FindFileByPath("events.proto")
			if err != nil {
				b.Fatal(err)
			}
			sink += file.Messages().Len()
		}
		mz015ProtobufBenchmarkSink = sink
	}
}

// BenchmarkMZ015ProtobufSchemaRegistryCached measures the warmed descriptor
// and message-path lookup used by a source decoder.
func BenchmarkMZ015ProtobufSchemaRegistryCached(b *testing.B) {
	payloads := mz015ProtobufBenchmarkPayloads()
	registry, err := NewProtobufSchemaRegistry(ProtobufSchemaRegistryOptions{Fetch: mz015ProtobufBenchmarkFetch})
	if err != nil {
		b.Fatal(err)
	}
	indexes := []int{0}
	if _, err := registry.ResolveMessage(7, indexes); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := 0
		for range payloads {
			messageType, err := registry.ResolveMessage(7, indexes)
			if err != nil {
				b.Fatal(err)
			}
			sink += messageType.Descriptor().Fields().Len()
		}
		mz015ProtobufBenchmarkSink = sink
	}
}
