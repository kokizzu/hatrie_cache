package hatPgWire_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"

	"hatrie_cache/hat/hatPgWire"
)

func TestServeConnRunsPostgreSQLSimpleQuery(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	queries := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- hatPgWire.ServeConn(context.Background(), server, hatPgWire.QueryHandlerFunc(func(_ context.Context, query string) (hatPgWire.QueryResult, error) {
			queries <- query
			value := "42"
			return hatPgWire.QueryResult{
				Fields: []hatPgWire.Field{{Name: "answer", DataTypeOID: hatPgWire.OIDInt8}},
				Rows:   [][]*string{{&value}},
			}, nil
		}), hatPgWire.ServerOptions{})
	}()

	writeSSLRequest(t, client)
	sslReply := make([]byte, 1)
	if _, err := io.ReadFull(client, sslReply); err != nil || string(sslReply) != "N" {
		t.Fatalf("SSL request reply = %q, %v, want N", sslReply, err)
	}
	writeStartup(t, client, "user", "analyst")
	messageType, body := readBackendMessage(t, client)
	if messageType != 'R' || len(body) != 4 || binary.BigEndian.Uint32(body) != 0 {
		t.Fatalf("authentication response = %q %v, want AuthenticationOk", messageType, body)
	}
	for {
		messageType, _ = readBackendMessage(t, client)
		if messageType == 'Z' {
			break
		}
	}

	writeFrontendMessage(t, client, 'Q', []byte("SELECT 42\x00"))
	if query := <-queries; query != "SELECT 42" {
		t.Fatalf("query = %q, want SELECT 42", query)
	}
	messageType, body = readBackendMessage(t, client)
	if messageType != 'T' || len(body) < 7 || binary.BigEndian.Uint16(body[:2]) != 1 {
		t.Fatalf("row description = %q %v, want one field", messageType, body)
	}
	messageType, body = readBackendMessage(t, client)
	if messageType != 'D' || len(body) != 8 || binary.BigEndian.Uint16(body[:2]) != 1 || string(body[6:]) != "42" {
		t.Fatalf("data row = %q %v, want 42", messageType, body)
	}
	messageType, body = readBackendMessage(t, client)
	if messageType != 'C' || string(body) != "SELECT 1\x00" {
		t.Fatalf("command complete = %q %q, want SELECT 1", messageType, body)
	}
	messageType, body = readBackendMessage(t, client)
	if messageType != 'Z' || string(body) != "I" {
		t.Fatalf("ready response = %q %q, want idle", messageType, body)
	}

	writeFrontendMessage(t, client, 'X', nil)
	if err := <-errCh; err != nil {
		t.Fatalf("ServeConn() error = %v", err)
	}
}

func TestServeConnAuthenticatesCleartextPassword(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	errCh := make(chan error, 1)
	go func() {
		errCh <- hatPgWire.ServeConn(context.Background(), server, hatPgWire.QueryHandlerFunc(func(context.Context, string) (hatPgWire.QueryResult, error) {
			return hatPgWire.QueryResult{}, nil
		}), hatPgWire.ServerOptions{
			Authenticator: func(_ context.Context, startup hatPgWire.Startup, password string) error {
				if startup.User != "analyst" || password != "correct" {
					return io.EOF
				}
				return nil
			},
		})
	}()

	writeStartup(t, client, "analyst", "analytics")
	messageType, body := readBackendMessage(t, client)
	if messageType != 'R' || len(body) != 4 || binary.BigEndian.Uint32(body) != 3 {
		t.Fatalf("password challenge = %q %v, want AuthenticationCleartextPassword", messageType, body)
	}
	writeFrontendMessage(t, client, 'p', []byte("correct\x00"))
	messageType, body = readBackendMessage(t, client)
	if messageType != 'R' || len(body) != 4 || binary.BigEndian.Uint32(body) != 0 {
		t.Fatalf("authentication response = %q %v, want AuthenticationOk", messageType, body)
	}
	readReadyForQuery(t, client)
	writeFrontendMessage(t, client, 'X', nil)
	if err := <-errCh; err != nil {
		t.Fatalf("ServeConn() error = %v", err)
	}
}

func TestServeConnRejectsInvalidCleartextPassword(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	errCh := make(chan error, 1)
	go func() {
		errCh <- hatPgWire.ServeConn(context.Background(), server, hatPgWire.QueryHandlerFunc(func(context.Context, string) (hatPgWire.QueryResult, error) {
			return hatPgWire.QueryResult{}, nil
		}), hatPgWire.ServerOptions{Authenticator: func(context.Context, hatPgWire.Startup, string) error { return io.EOF }})
	}()

	writeStartup(t, client, "analyst", "analytics")
	readBackendMessage(t, client)
	writeFrontendMessage(t, client, 'p', []byte("wrong\x00"))
	messageType, body := readBackendMessage(t, client)
	if messageType != 'E' || !strings.Contains(string(body), "28P01") {
		t.Fatalf("authentication failure = %q %q, want SQLSTATE 28P01", messageType, body)
	}
	if err := <-errCh; err == nil {
		t.Fatal("ServeConn() accepted invalid password")
	}
}

func TestServeConnRunsPostgreSQLExtendedQuery(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	errCh := make(chan error, 1)
	go func() {
		errCh <- hatPgWire.ServeConn(context.Background(), server, hatPgWire.QueryHandlerFunc(func(_ context.Context, query string) (hatPgWire.QueryResult, error) {
			value := "ok"
			return hatPgWire.QueryResult{Fields: []hatPgWire.Field{{Name: "status"}}, Rows: [][]*string{{&value}}}, nil
		}), hatPgWire.ServerOptions{})
	}()
	writeStartup(t, client, "user", "analytics")
	readBackendMessage(t, client)
	readReadyForQuery(t, client)
	writeFrontendMessage(t, client, 'P', append([]byte("statement\x00FROM VALUES ('ok') AS value(status) SELECT value.status\x00"), 0, 0))
	if messageType, _ := readBackendMessage(t, client); messageType != '1' {
		t.Fatalf("Parse response = %q, want ParseComplete", messageType)
	}
	bind := append([]byte("portal\x00statement\x00"), 0, 0, 0, 0, 0, 0)
	writeFrontendMessage(t, client, 'B', bind)
	if messageType, body := readBackendMessage(t, client); messageType != '2' {
		t.Fatalf("Bind response = %q %q, want BindComplete", messageType, body)
	}
	writeFrontendMessage(t, client, 'E', []byte("portal\x00\x00\x00\x00\x00"))
	if messageType, _ := readBackendMessage(t, client); messageType != 'T' {
		t.Fatalf("Execute response = %q, want RowDescription", messageType)
	}
	readBackendMessage(t, client)
	if messageType, _ := readBackendMessage(t, client); messageType != 'C' {
		t.Fatalf("Execute completion = %q, want CommandComplete", messageType)
	}
	writeFrontendMessage(t, client, 'S', nil)
	if messageType, _ := readBackendMessage(t, client); messageType != 'Z' {
		t.Fatalf("Sync response = %q, want ReadyForQuery", messageType)
	}
	writeFrontendMessage(t, client, 'X', nil)
	if err := <-errCh; err != nil {
		t.Fatalf("ServeConn() error = %v", err)
	}
}

func writeSSLRequest(t *testing.T, connection net.Conn) {
	t.Helper()
	packet := make([]byte, 8)
	binary.BigEndian.PutUint32(packet[:4], 8)
	binary.BigEndian.PutUint32(packet[4:], 80877103)
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
}

func readReadyForQuery(t *testing.T, connection net.Conn) {
	t.Helper()
	for {
		messageType, _ := readBackendMessage(t, connection)
		if messageType == 'Z' {
			return
		}
	}
}

func writeStartup(t *testing.T, connection net.Conn, user string, database string) {
	t.Helper()
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 196608)
	body = append(body, []byte("user\x00"+user+"\x00database\x00"+database+"\x00\x00")...)
	packet := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(packet, uint32(len(packet)))
	copy(packet[4:], body)
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
}

func writeFrontendMessage(t *testing.T, connection net.Conn, messageType byte, body []byte) {
	t.Helper()
	packet := make([]byte, 5+len(body))
	packet[0] = messageType
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	if _, err := connection.Write(packet); err != nil {
		t.Fatal(err)
	}
}

func readBackendMessage(t *testing.T, connection net.Conn) (byte, []byte) {
	t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(connection, header); err != nil {
		t.Fatal(err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 {
		t.Fatalf("invalid backend message length %d", length)
	}
	body := make([]byte, int(length)-4)
	if _, err := io.ReadFull(connection, body); err != nil {
		t.Fatal(err)
	}
	return header[0], body
}
