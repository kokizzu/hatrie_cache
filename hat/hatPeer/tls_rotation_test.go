package hatPeer

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestCompactPeerMutualTLSConfigRotatesServerCertificate(t *testing.T) {
	caCertificate, caKey := newCompactPeerTestCertificate(t, nil, nil, 1, "compact-peer-ca", nil, true)
	caParsed, err := x509.ParseCertificate(caCertificate.Certificate[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate(CA) error = %v", err)
	}
	serverOne, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 2, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	serverTwo, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 3, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	clientCertificate, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 4, "client.example", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)

	clientRoots := x509.NewCertPool()
	clientRoots.AddCert(caParsed)
	provider, err := NewCompactPeerTLSCertificateProvider(serverOne)
	if err != nil {
		t.Fatalf("NewCompactPeerTLSCertificateProvider() error = %v", err)
	}
	serverConfig, err := NewCompactPeerMutualTLSConfig(provider, clientRoots)
	if err != nil {
		t.Fatalf("NewCompactPeerMutualTLSConfig() error = %v", err)
	}
	if serverConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth = %v, want RequireAndVerifyClientCert", serverConfig.ClientAuth)
	}

	clientConfig := &tls.Config{
		RootCAs:      clientRoots,
		ServerName:   "peer.example",
		Certificates: []tls.Certificate{clientCertificate},
		MinVersion:   tls.VersionTLS13,
	}
	state, serverErr, clientErr := compactPeerTLSHandshake(t, serverConfig, clientConfig)
	if serverErr != nil || clientErr != nil {
		t.Fatalf("initial handshake errors = server %v, client %v", serverErr, clientErr)
	}
	if got := state.PeerCertificates[0].SerialNumber.Int64(); got != 2 {
		t.Fatalf("initial server certificate serial = %d, want 2", got)
	}

	if err := provider.Rotate(serverTwo); err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	state, serverErr, clientErr = compactPeerTLSHandshake(t, serverConfig, clientConfig)
	if serverErr != nil || clientErr != nil {
		t.Fatalf("rotated handshake errors = server %v, client %v", serverErr, clientErr)
	}
	if got := state.PeerCertificates[0].SerialNumber.Int64(); got != 3 {
		t.Fatalf("rotated server certificate serial = %d, want 3", got)
	}
}

func TestCompactPeerMutualTLSRequiresClientCertificate(t *testing.T) {
	caCertificate, caKey := newCompactPeerTestCertificate(t, nil, nil, 10, "compact-peer-ca", nil, true)
	caParsed, err := x509.ParseCertificate(caCertificate.Certificate[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate(CA) error = %v", err)
	}
	serverCertificate, _ := newCompactPeerTestCertificate(t, caParsed, caKey, 11, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	roots := x509.NewCertPool()
	roots.AddCert(caParsed)
	provider, err := NewCompactPeerTLSCertificateProvider(serverCertificate)
	if err != nil {
		t.Fatalf("NewCompactPeerTLSCertificateProvider() error = %v", err)
	}
	serverConfig, err := NewCompactPeerMutualTLSConfig(provider, roots)
	if err != nil {
		t.Fatalf("NewCompactPeerMutualTLSConfig() error = %v", err)
	}
	clientConfig := &tls.Config{RootCAs: roots, ServerName: "peer.example", MinVersion: tls.VersionTLS13}
	_, serverErr, clientErr := compactPeerTLSHandshake(t, serverConfig, clientConfig)
	if serverErr == nil || clientErr == nil {
		t.Fatalf("missing-client handshake errors = server %v, client %v, want both errors", serverErr, clientErr)
	}
}

func TestCompactPeerTLSCertificateProviderRejectsInvalidRotation(t *testing.T) {
	certificate, _ := newCompactPeerTestCertificate(t, nil, nil, 20, "peer.example", nil, true)
	provider, err := NewCompactPeerTLSCertificateProvider(certificate)
	if err != nil {
		t.Fatalf("NewCompactPeerTLSCertificateProvider() error = %v", err)
	}
	if err := provider.Rotate(tls.Certificate{}); err == nil {
		t.Fatal("Rotate(invalid) error = nil, want validation error")
	}
	current, err := provider.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate() error = %v", err)
	}
	if got := current.Certificate[0]; string(got) != string(certificate.Certificate[0]) {
		t.Fatal("invalid rotation replaced the active certificate")
	}
}

func BenchmarkCompactPeerTLSCertificateLookup(b *testing.B) {
	certificate, _ := newCompactPeerTestCertificate(b, nil, nil, 30, "peer.example", nil, true)
	provider, err := NewCompactPeerTLSCertificateProvider(certificate)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("static_callback", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var selected *tls.Certificate
		for index := 0; index < b.N; index++ {
			selected = &certificate
		}
		runtime.KeepAlive(selected)
	})
	b.Run("rotating_provider", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var selected *tls.Certificate
		for index := 0; index < b.N; index++ {
			selected, err = provider.GetCertificate(nil)
			if err != nil {
				b.Fatal(err)
			}
		}
		runtime.KeepAlive(selected)
	})
}

func BenchmarkCompactPeerTLSHandshake(b *testing.B) {
	caCertificate, caKey := newCompactPeerTestCertificate(b, nil, nil, 40, "compact-peer-ca", nil, true)
	caParsed, err := x509.ParseCertificate(caCertificate.Certificate[0])
	if err != nil {
		b.Fatal(err)
	}
	serverCertificate, _ := newCompactPeerTestCertificate(b, caParsed, caKey, 41, "peer.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false)
	clientCertificate, _ := newCompactPeerTestCertificate(b, caParsed, caKey, 42, "client.example", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)
	roots := x509.NewCertPool()
	roots.AddCert(caParsed)
	provider, err := NewCompactPeerTLSCertificateProvider(serverCertificate)
	if err != nil {
		b.Fatal(err)
	}
	rotatingConfig, err := NewCompactPeerMutualTLSConfig(provider, roots)
	if err != nil {
		b.Fatal(err)
	}
	staticConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    roots,
	}
	clientConfig := &tls.Config{
		RootCAs:      roots,
		ServerName:   "peer.example",
		Certificates: []tls.Certificate{clientCertificate},
		MinVersion:   tls.VersionTLS13,
	}
	for _, benchmark := range []struct {
		name         string
		serverConfig *tls.Config
	}{
		{name: "static_certificate", serverConfig: staticConfig},
		{name: "rotating_certificate", serverConfig: rotatingConfig},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				_, serverErr, clientErr := compactPeerTLSHandshake(b, benchmark.serverConfig, clientConfig)
				if serverErr != nil || clientErr != nil {
					b.Fatalf("handshake errors = server %v, client %v", serverErr, clientErr)
				}
			}
		})
	}
}

func compactPeerTLSHandshake(t testing.TB, serverConfig, clientConfig *tls.Config) (tls.ConnectionState, error, error) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	server := tls.Server(serverConn, serverConfig)
	client := tls.Client(clientConn, clientConfig)
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Handshake() }()
	clientErr := client.Handshake()
	serverErr := <-serverErrors
	return client.ConnectionState(), serverErr, clientErr
}

func newCompactPeerTestCertificate(t testing.TB, parent *x509.Certificate, parentKey crypto.Signer, serial int64, commonName string, usages []x509.ExtKeyUsage, ca bool) (tls.Certificate, crypto.Signer) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           usages,
		DNSNames:              []string{commonName},
		BasicConstraintsValid: true,
		IsCA:                  ca,
	}
	if ca {
		template.KeyUsage |= x509.KeyUsageCertSign
	}
	if parent == nil {
		parent = template
		parentKey = key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, key
}
