package hatGrpc

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatCommand"
)

const (
	// ProtocolVersionMetadataKey carries an exact version or inclusive range
	// accepted by a gRPC client.
	ProtocolVersionMetadataKey = "x-hatrie-protocol-version"
	// ProtocolSupportedMetadataKey advertises the selected and supported
	// versions in gRPC response metadata.
	ProtocolSupportedMetadataKey = "x-hatrie-protocol-supported"
)

// AppendProtocolVersionRange adds a client compatibility range to outgoing
// gRPC metadata. A nil context is replaced with context.Background.
func AppendProtocolVersionRange(ctx context.Context, versions hatCommand.ProtocolVersionRange) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return metadata.AppendToOutgoingContext(ctx, ProtocolVersionMetadataKey, versions.String())
}

// IncomingProtocolVersionRange reads the optional client compatibility range.
// Missing metadata retains backward compatibility with the current version;
// malformed or repeated values are rejected.
func IncomingProtocolVersionRange(ctx context.Context) (hatCommand.ProtocolVersionRange, error) {
	defaultRange := hatCommand.ProtocolVersionRange{Min: hatCommand.CurrentProtocolVersion, Max: hatCommand.CurrentProtocolVersion}
	if ctx == nil {
		return defaultRange, nil
	}
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return defaultRange, nil
	}
	values := incoming.Get(ProtocolVersionMetadataKey)
	if len(values) == 0 {
		return defaultRange, nil
	}
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return hatCommand.ProtocolVersionRange{}, hatCommand.ErrInvalidProtocolVersion
	}
	return hatCommand.ParseProtocolVersionRange(values[0])
}

// NegotiateProtocolVersion selects the highest version shared by the gRPC
// client metadata and server range.
func NegotiateProtocolVersion(ctx context.Context, server hatCommand.ProtocolVersionRange) (hatCommand.ProtocolVersion, error) {
	client, err := IncomingProtocolVersionRange(ctx)
	if err != nil {
		return 0, err
	}
	return hatCommand.NegotiateProtocolVersion(client, server)
}

// ProtocolResponseMetadata returns response headers for a selected gRPC
// protocol version and the server's complete supported range.
func ProtocolResponseMetadata(version hatCommand.ProtocolVersion, supported hatCommand.ProtocolVersionRange) metadata.MD {
	return metadata.Pairs(
		ProtocolVersionMetadataKey, strconv.FormatUint(uint64(version), 10),
		ProtocolSupportedMetadataKey, supported.String(),
	)
}
