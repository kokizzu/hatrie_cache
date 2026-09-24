package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

func TestCompactResponseSchemaFrameRoundTrip(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	encoded, err := protocol.Marshal(CompactFrame{
		Kind:             CompactResponse,
		RequestID:        9,
		ResponseSchemaID: 17,
		Command:          []byte("ECHO"),
		Payload:          []byte("payload"),
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	decoded, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if decoded.ResponseSchemaID != 17 || decoded.Flags&CompactFrameFlagResponseSchema == 0 {
		t.Fatalf("decoded schema = %d flags=%#x, want 17 and schema flag", decoded.ResponseSchemaID, decoded.Flags)
	}
	if !bytes.Equal(decoded.Payload, []byte("payload")) {
		t.Fatalf("decoded payload = %q", decoded.Payload)
	}
	if _, err := protocol.Marshal(CompactFrame{
		Kind:      CompactResponse,
		RequestID: 9,
		Flags:     CompactFrameFlagResponseSchema,
	}); !errors.Is(err, ErrCompactProtocolResponseSchemaInvalid) {
		t.Fatalf("zero schema error = %v, want %v", err, ErrCompactProtocolResponseSchemaInvalid)
	}
}

func TestCompactResponseSchemaPreparedCallRoundTrip(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		EnableResponseSchemas: true,
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			if request.ResponseSchemaID != 17 {
				t.Fatalf("request schema = %d, want 17", request.ResponseSchemaID)
			}
			return CompactFrame{Payload: append([]byte("reply:"), request.Payload...)}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	defer server.Close()
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{EnableResponseSchemas: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer client.Close()

	template, err := NewCompactRequestTemplateWithResponseSchema([]byte("ECHO"), 17)
	if err != nil {
		t.Fatalf("NewCompactRequestTemplateWithResponseSchema() error = %v", err)
	}
	response, err := client.CallTemplate(context.Background(), template, []byte("payload"))
	if err != nil {
		t.Fatalf("CallTemplate() error = %v", err)
	}
	if response.ResponseSchemaID != 17 || string(response.Payload) != "reply:payload" {
		t.Fatalf("response = %#v, want schema 17 and reply payload", response)
	}
}

func TestCompactResponseSchemaRejectsUnsupportedAndMismatchedPeers(t *testing.T) {
	unsupportedServerConn, unsupportedClientConn := net.Pipe()
	unsupportedServer, err := NewCompactPeerSession(unsupportedServerConn, CompactPeerSessionOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(unsupported server) error = %v", err)
	}
	defer unsupportedServer.Close()
	unsupportedClient, err := NewCompactPeerSession(unsupportedClientConn, CompactPeerSessionOptions{EnableResponseSchemas: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(unsupported client) error = %v", err)
	}
	defer unsupportedClient.Close()
	template, err := NewCompactRequestTemplateWithResponseSchema([]byte("ECHO"), 17)
	if err != nil {
		t.Fatalf("NewCompactRequestTemplateWithResponseSchema() error = %v", err)
	}
	if _, err := unsupportedClient.CallTemplate(context.Background(), template, nil); !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("unsupported schema error = %v, want remote error", err)
	}

	mismatchServerConn, mismatchClientConn := net.Pipe()
	mismatchServer, err := NewCompactPeerSession(mismatchServerConn, CompactPeerSessionOptions{
		EnableResponseSchemas: true,
		Handler: func(_ context.Context, _ CompactFrame) (CompactFrame, error) {
			return CompactFrame{ResponseSchemaID: 18, Payload: []byte("wrong")}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(mismatch server) error = %v", err)
	}
	defer mismatchServer.Close()
	mismatchClient, err := NewCompactPeerSession(mismatchClientConn, CompactPeerSessionOptions{EnableResponseSchemas: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(mismatch client) error = %v", err)
	}
	defer mismatchClient.Close()
	if _, err := mismatchClient.CallTemplate(context.Background(), template, nil); !errors.Is(err, ErrCompactPeerResponseSchemaMismatch) {
		t.Fatalf("mismatched schema error = %v, want %v", err, ErrCompactPeerResponseSchemaMismatch)
	}
}

func TestCompactResponseSchemaNegotiatedHandshakeOption(t *testing.T) {
	options := CompactPeerSessionOptions{
		EnableResponseSchemas: true,
		Protocol:              CompactProtocolOptions{},
	}
	without := CompactPeerSessionOptionsForNegotiatedHandshake(options, CompactPeerHandshake{})
	if without.EnableResponseSchemas {
		t.Fatal("response schemas remained enabled without negotiated feature")
	}
	with := CompactPeerSessionOptionsForNegotiatedHandshake(options, CompactPeerHandshake{Features: CompactPeerFeatureResponseSchemas})
	if !with.EnableResponseSchemas {
		t.Fatal("response schemas disabled after negotiated feature")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()
	server, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{
		Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil },
		Session:   CompactPeerSessionOptions{EnableResponseSchemas: true},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerListener() error = %v", err)
	}
	defer server.Close()
	if server.options.Handshake.Features&CompactPeerFeatureResponseSchemas == 0 {
		t.Fatal("listener did not advertise response-schema feature")
	}
}
