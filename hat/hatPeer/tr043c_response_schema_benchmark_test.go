package hatPeer

import (
	"context"
	"net"
	"testing"
)

func BenchmarkCompactResponseSchemaTemplateMarshal(b *testing.B) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	legacy, err := NewCompactRequestTemplate([]byte("GET"))
	if err != nil {
		b.Fatal(err)
	}
	schema, err := NewCompactRequestTemplateWithResponseSchema([]byte("GET"), 17)
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte("benchmark-payload")
	for _, test := range []struct {
		name     string
		template CompactRequestTemplate
	}{
		{name: "legacy", template: legacy},
		{name: "response_schema", template: schema},
	} {
		b.Run(test.name, func(b *testing.B) {
			warmup, err := test.template.Marshal(protocol, 1, payload)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(warmup)))
			buffer := make([]byte, 0, len(warmup))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				buffer, err = test.template.MarshalInto(protocol, uint64(index+1), payload, buffer[:0])
				if err != nil {
					b.Fatal(err)
				}
			}
			compactResponseSchemaBenchmarkSink = buffer
		})
	}
}

func BenchmarkCompactPeerSessionCallTemplateResponseSchema(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		EnableResponseSchemas: true,
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{EnableResponseSchemas: true})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	template, err := NewCompactRequestTemplateWithResponseSchema([]byte("GET"), 17)
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		b.Fatal(err)
	}
	payload := []byte("benchmark-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response, err := client.CallTemplate(context.Background(), template, payload)
		if err != nil {
			b.Fatal(err)
		}
		if response.ResponseSchemaID != 17 || string(response.Payload) != string(payload) {
			b.Fatalf("response = %#v, want schema 17 and payload", response)
		}
	}
	b.StopTimer()
	_ = client.Close()
	_ = server.Close()
}

var compactResponseSchemaBenchmarkSink []byte
