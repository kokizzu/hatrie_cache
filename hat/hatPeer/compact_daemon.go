package hatPeer

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
)

var (
	// ErrCompactPeerDaemonRequired indicates a nil daemon receiver.
	ErrCompactPeerDaemonRequired = errors.New("hatPeer: compact peer daemon is required")
	// ErrCompactPeerDaemonAddressRequired indicates a missing bind address.
	ErrCompactPeerDaemonAddressRequired = errors.New("hatPeer: compact peer daemon address is required")
	// ErrCompactPeerDaemonTLSRequired indicates that TLS was required but no
	// server TLS configuration was supplied, or a TLS client was omitted.
	ErrCompactPeerDaemonTLSRequired = errors.New("hatPeer: compact peer daemon TLS is required")
	// ErrCompactPeerDaemonTLSConfigInvalid indicates an unusable server TLS
	// configuration when the daemon owns the TLS listener.
	ErrCompactPeerDaemonTLSConfigInvalid = errors.New("hatPeer: compact peer daemon TLS configuration is invalid")
	// ErrCompactPeerDaemonClientCertificateRequired indicates that the daemon
	// requested mutual TLS without an explicit verifying client policy.
	ErrCompactPeerDaemonClientCertificateRequired = errors.New("hatPeer: compact peer daemon client certificate policy is required")
	// ErrCompactPeerDaemonContextRequired indicates a nil dial context.
	ErrCompactPeerDaemonContextRequired = errors.New("hatPeer: compact peer daemon context is required")
)

// CompactPeerDaemonOptions configures a listener-owning compact peer daemon.
// The listener's Authorize callback remains mandatory. TLSConfig wraps the
// bound listener when present; RequireTLS makes that choice explicit and
// rejects accidental plaintext configuration.
type CompactPeerDaemonOptions struct {
	Network                  string
	Address                  string
	TLSConfig                *tls.Config
	RequireTLS               bool
	RequireClientCertificate bool
	Listener                 CompactPeerListenerOptions
}

// CompactPeerDaemonDialOptions configures one client connection to a daemon.
// TLSConfig is cloned before use and must contain the client's trust and
// identity policy when the daemon requires TLS.
type CompactPeerDaemonDialOptions struct {
	TLSConfig *tls.Config
	Handshake CompactPeerHandshakeOptions
	Session   CompactPeerSessionOptions
}

// CompactPeerDaemon owns a network listener and the bounded authenticated
// compact-peer listener behind it. It is opt-in and does not start serving
// until Serve is called.
type CompactPeerDaemon struct {
	server     *CompactPeerListener
	network    string
	address    string
	addr       net.Addr
	requireTLS bool
}

// NewCompactPeerDaemon binds a compact-peer listener. An empty Network uses
// TCP. A non-nil TLSConfig enables a TLS listener; RequireClientCertificate
// additionally requires tls.RequireAndVerifyClientCert so a caller cannot
// accidentally request client certificates without verification.
func NewCompactPeerDaemon(options CompactPeerDaemonOptions) (*CompactPeerDaemon, error) {
	if strings.TrimSpace(options.Address) == "" {
		return nil, ErrCompactPeerDaemonAddressRequired
	}
	if options.Network == "" {
		options.Network = "tcp"
	}
	requireTLS := options.RequireTLS || options.Listener.RequireTLS || options.TLSConfig != nil || options.RequireClientCertificate
	if requireTLS && options.TLSConfig == nil {
		return nil, ErrCompactPeerDaemonTLSRequired
	}
	var tlsConfig *tls.Config
	if options.TLSConfig != nil {
		tlsConfig = options.TLSConfig.Clone()
		if len(tlsConfig.Certificates) == 0 && tlsConfig.GetCertificate == nil && tlsConfig.GetConfigForClient == nil {
			return nil, ErrCompactPeerDaemonTLSConfigInvalid
		}
		if options.RequireClientCertificate && tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
			return nil, ErrCompactPeerDaemonClientCertificateRequired
		}
	}
	listener, err := net.Listen(options.Network, options.Address)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	listenerOptions := options.Listener
	listenerOptions.RequireTLS = requireTLS
	server, err := NewCompactPeerListener(listener, listenerOptions)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	return &CompactPeerDaemon{
		server:     server,
		network:    options.Network,
		address:    listener.Addr().String(),
		addr:       listener.Addr(),
		requireTLS: requireTLS,
	}, nil
}

// Serve runs the daemon until the context or Close stops it.
func (daemon *CompactPeerDaemon) Serve(ctx context.Context) error {
	if daemon == nil || daemon.server == nil {
		return ErrCompactPeerDaemonRequired
	}
	return daemon.server.Serve(ctx)
}

// Close stops accepting connections and terminates active sessions.
func (daemon *CompactPeerDaemon) Close() error {
	if daemon == nil || daemon.server == nil {
		return ErrCompactPeerDaemonRequired
	}
	return daemon.server.Close()
}

// Done returns a channel closed after Serve and all active sessions stop.
func (daemon *CompactPeerDaemon) Done() <-chan struct{} {
	if daemon == nil || daemon.server == nil {
		return nil
	}
	return daemon.server.Done()
}

// Addr returns the actual bound address, including an ephemeral port.
func (daemon *CompactPeerDaemon) Addr() net.Addr {
	if daemon == nil {
		return nil
	}
	return daemon.addr
}

// Stats returns the underlying bounded listener counters.
func (daemon *CompactPeerDaemon) Stats() CompactPeerListenerStats {
	if daemon == nil || daemon.server == nil {
		return CompactPeerListenerStats{}
	}
	return daemon.server.Stats()
}

// Dial connects to the daemon, performs TLS when configured, negotiates the
// compact protocol, and starts a client session. The dial context covers both
// TCP and TLS handshakes; session lifetime is controlled by Session.Context.
func (daemon *CompactPeerDaemon) Dial(ctx context.Context, options CompactPeerDaemonDialOptions) (*CompactPeerSession, CompactPeerHandshake, error) {
	if daemon == nil || daemon.server == nil {
		return nil, CompactPeerHandshake{}, ErrCompactPeerDaemonRequired
	}
	if ctx == nil {
		return nil, CompactPeerHandshake{}, ErrCompactPeerDaemonContextRequired
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, daemon.network, daemon.address)
	if err != nil {
		return nil, CompactPeerHandshake{}, err
	}
	if options.TLSConfig != nil {
		tlsConn := tls.Client(conn, options.TLSConfig.Clone())
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, CompactPeerHandshake{}, err
		}
		conn = tlsConn
	} else if daemon.requireTLS {
		_ = conn.Close()
		return nil, CompactPeerHandshake{}, ErrCompactPeerDaemonTLSRequired
	}
	negotiated, err := PerformCompactPeerHandshake(ctx, conn, options.Handshake)
	if err != nil {
		_ = conn.Close()
		return nil, CompactPeerHandshake{}, err
	}
	sessionOptions := CompactPeerSessionOptionsForNegotiatedHandshake(options.Session, negotiated)
	session, err := NewCompactPeerSession(conn, sessionOptions)
	if err != nil {
		_ = conn.Close()
		return nil, CompactPeerHandshake{}, err
	}
	return session, negotiated, nil
}
