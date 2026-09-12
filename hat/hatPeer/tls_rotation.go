package hatPeer

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrCompactPeerTLSCertificateInvalid = errors.New("hatPeer: TLS certificate is invalid")
	ErrCompactPeerTLSProviderRequired   = errors.New("hatPeer: TLS certificate provider is required")
	ErrCompactPeerTLSClientCAsRequired  = errors.New("hatPeer: mutual TLS client CAs are required")
)

// CompactPeerTLSCertificateProvider publishes one immutable server
// certificate to concurrent TLS handshakes. Rotate validates the replacement
// before atomically making it visible to new connections. Existing
// connections continue using the certificate selected during their handshake.
type CompactPeerTLSCertificateProvider struct {
	certificate atomic.Pointer[tls.Certificate]
}

// NewCompactPeerTLSCertificateProvider validates and stores the initial
// certificate. The certificate's private key and DER chain are retained by
// the provider and must not be mutated by the caller after this call.
func NewCompactPeerTLSCertificateProvider(certificate tls.Certificate) (*CompactPeerTLSCertificateProvider, error) {
	provider := &CompactPeerTLSCertificateProvider{}
	if err := provider.Rotate(certificate); err != nil {
		return nil, err
	}
	return provider, nil
}

// Rotate validates certificate and atomically publishes it for subsequent
// handshakes. A failed rotation leaves the currently active certificate
// unchanged.
func (provider *CompactPeerTLSCertificateProvider) Rotate(certificate tls.Certificate) error {
	if provider == nil {
		return ErrCompactPeerTLSProviderRequired
	}
	if err := validateCompactPeerTLSCertificate(certificate); err != nil {
		return err
	}
	copy := cloneCompactPeerTLSCertificate(certificate)
	provider.certificate.Store(&copy)
	return nil
}

// GetCertificate implements tls.Config.GetCertificate. The returned
// certificate is immutable provider state and is safe for the TLS package to
// use concurrently.
func (provider *CompactPeerTLSCertificateProvider) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	if provider == nil {
		return nil, ErrCompactPeerTLSProviderRequired
	}
	certificate := provider.certificate.Load()
	if certificate == nil {
		return nil, ErrCompactPeerTLSCertificateInvalid
	}
	return certificate, nil
}

// NewCompactPeerMutualTLSConfig returns a TLS 1.3 server configuration that
// requires and verifies a client certificate against clientCAs. The returned
// config reads the provider on every new handshake, so Rotate takes effect
// without restarting a listener.
func NewCompactPeerMutualTLSConfig(provider *CompactPeerTLSCertificateProvider, clientCAs *x509.CertPool) (*tls.Config, error) {
	if provider == nil {
		return nil, ErrCompactPeerTLSProviderRequired
	}
	if clientCAs == nil || len(clientCAs.Subjects()) == 0 {
		return nil, ErrCompactPeerTLSClientCAsRequired
	}
	if _, err := provider.GetCertificate(nil); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: provider.GetCertificate,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      clientCAs,
	}, nil
}

func validateCompactPeerTLSCertificate(certificate tls.Certificate) error {
	if len(certificate.Certificate) == 0 || certificate.PrivateKey == nil {
		return ErrCompactPeerTLSCertificateInvalid
	}
	for index, der := range certificate.Certificate {
		if len(der) == 0 {
			return fmt.Errorf("%w: certificate chain entry %d is empty", ErrCompactPeerTLSCertificateInvalid, index)
		}
		if _, err := x509.ParseCertificate(der); err != nil {
			return fmt.Errorf("%w: parse certificate chain entry %d: %v", ErrCompactPeerTLSCertificateInvalid, index, err)
		}
	}
	return nil
}

func cloneCompactPeerTLSCertificate(certificate tls.Certificate) tls.Certificate {
	cloned := certificate
	cloned.Certificate = make([][]byte, len(certificate.Certificate))
	for index, der := range certificate.Certificate {
		cloned.Certificate[index] = append([]byte(nil), der...)
	}
	cloned.OCSPStaple = append([]byte(nil), certificate.OCSPStaple...)
	cloned.SignedCertificateTimestamps = make([][]byte, len(certificate.SignedCertificateTimestamps))
	for index, timestamp := range certificate.SignedCertificateTimestamps {
		cloned.SignedCertificateTimestamps[index] = append([]byte(nil), timestamp...)
	}
	return cloned
}
