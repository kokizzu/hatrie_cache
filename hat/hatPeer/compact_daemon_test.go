package hatPeer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCompactPeerDaemonMutualTLSRoundTrip(t *testing.T) {
	caCertificate, caKey := newCompactPeerTestCertificate(t, nil, nil, 100, "compact-peer-ca", nil, true)
	caParsed, err := x509.ParseCertificate(caCertificate.Certificate[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate(CA) error = %v", err)
	}
	serverCertificate, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 101, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	clientCertificate, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 102, "client.example", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)
	roots := x509.NewCertPool()
	roots.AddCert(caParsed)

	daemon, err := NewCompactPeerDaemon(CompactPeerDaemonOptions{
		Network:                  "tcp",
		Address:                  "127.0.0.1:0",
		RequireTLS:               true,
		RequireClientCertificate: true,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{serverCertificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    roots,
		},
		Listener: CompactPeerListenerOptions{
			Handshake: CompactPeerHandshakeOptions{Features: CompactPeerFeaturePayloadCompression},
			Authorize: func(_ context.Context, conn net.Conn, _ CompactPeerHandshake) error {
				state, ok := conn.(interface{ ConnectionState() tls.ConnectionState })
				if !ok || len(state.ConnectionState().PeerCertificates) != 1 || state.ConnectionState().PeerCertificates[0].Subject.CommonName != "client.example" {
					return errors.New("unexpected client certificate")
				}
				return nil
			},
			Session: CompactPeerSessionOptions{
				Protocol: CompactProtocolOptions{CompressPayloadsAbove: 1},
				Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
					return CompactFrame{Kind: CompactResponse, RequestID: request.RequestID, Command: request.Command, Payload: append([]byte(nil), request.Payload...)}, nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerDaemon() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()

	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		ServerName:   "peer.example",
		Certificates: []tls.Certificate{clientCertificate},
	}
	session, negotiated, err := daemon.Dial(context.Background(), CompactPeerDaemonDialOptions{
		TLSConfig: clientTLS,
		Handshake: CompactPeerHandshakeOptions{Features: CompactPeerFeaturePayloadCompression, Timeout: time.Second},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer session.Close()
	if negotiated.Features&CompactPeerFeaturePayloadCompression == 0 {
		t.Fatalf("negotiated features = %#x, want compression", negotiated.Features)
	}
	response, err := session.Call(context.Background(), []byte("echo"), []byte("payload"))
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if response.Kind != CompactResponse || string(response.Command) != "echo" || string(response.Payload) != "payload" {
		t.Fatalf("response = %#v, want echo/payload response", response)
	}
	if stats := daemon.Stats(); stats.Negotiated != 1 || stats.AuthFailures != 0 {
		t.Fatalf("daemon stats = %#v, want one authenticated negotiation", stats)
	}
	if err := daemon.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, ErrCompactPeerListenerClosed) {
			t.Fatalf("Serve() error = %v, want listener closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after Close()")
	}
}

func TestCompactPeerDaemonValidatesTLSPolicy(t *testing.T) {
	_, err := NewCompactPeerDaemon(CompactPeerDaemonOptions{
		Address:    "127.0.0.1:0",
		RequireTLS: true,
		Listener:   CompactPeerListenerOptions{Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil }},
	})
	if !errors.Is(err, ErrCompactPeerDaemonTLSRequired) {
		t.Fatalf("missing TLS config error = %v, want ErrCompactPeerDaemonTLSRequired", err)
	}

	_, err = NewCompactPeerDaemon(CompactPeerDaemonOptions{
		Address:                  "127.0.0.1:0",
		RequireClientCertificate: true,
		TLSConfig:                &tls.Config{Certificates: []tls.Certificate{{}}},
		Listener:                 CompactPeerListenerOptions{Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil }},
	})
	if !errors.Is(err, ErrCompactPeerDaemonClientCertificateRequired) {
		t.Fatalf("missing client certificate policy error = %v, want ErrCompactPeerDaemonClientCertificateRequired", err)
	}
}

func TestCompactPeerDaemonPlaintextRequiresExplicitTLSOptIn(t *testing.T) {
	daemon, err := NewCompactPeerDaemon(CompactPeerDaemonOptions{
		Address: "127.0.0.1:0",
		Listener: CompactPeerListenerOptions{
			Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil },
			Session: CompactPeerSessionOptions{Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
				return CompactFrame{Kind: CompactResponse, RequestID: request.RequestID, Command: request.Command}, nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerDaemon() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()
	session, _, err := daemon.Dial(context.Background(), CompactPeerDaemonDialOptions{})
	if err != nil {
		t.Fatalf("plaintext opt-in Dial() error = %v", err)
	}
	_ = session.Close()
	if err := daemon.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after plaintext daemon close")
	}
}

func BenchmarkCompactPeerDaemonDial(b *testing.B) {
	caCertificate, caKey := newCompactPeerTestCertificate(b, nil, nil, 200, "compact-peer-ca", nil, true)
	caParsed, err := x509.ParseCertificate(caCertificate.Certificate[0])
	if err != nil {
		b.Fatal(err)
	}
	serverCertificate, _ := newCompactPeerTestCertificate(b, caParsed, caKey, 201, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	clientCertificate, _ := newCompactPeerTestCertificate(b, caParsed, caKey, 202, "client.example", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)
	roots := x509.NewCertPool()
	roots.AddCert(caParsed)
	daemon, err := NewCompactPeerDaemon(CompactPeerDaemonOptions{
		Address:                  "127.0.0.1:0",
		RequireTLS:               true,
		RequireClientCertificate: true,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{serverCertificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    roots,
		},
		Listener: CompactPeerListenerOptions{
			Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil },
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()
	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		ServerName:   "peer.example",
		Certificates: []tls.Certificate{clientCertificate},
	}
	dialOptions := CompactPeerDaemonDialOptions{TLSConfig: clientTLS}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		session, _, err := daemon.Dial(context.Background(), dialOptions)
		if err != nil {
			b.Fatal(err)
		}
		if err := session.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := daemon.Close(); err != nil {
		b.Fatal(err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		b.Fatal("daemon Serve() did not stop")
	}
}
