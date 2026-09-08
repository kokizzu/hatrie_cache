package hatGrpc

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatCommand"
)

func TestGRPCProtocolMetadataDefaultsAndNegotiates(t *testing.T) {
	defaultRange, err := IncomingProtocolVersionRange(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if defaultRange != (hatCommand.ProtocolVersionRange{Min: hatCommand.CurrentProtocolVersion, Max: hatCommand.CurrentProtocolVersion}) {
		t.Fatalf("default protocol range = %#v, want current version", defaultRange)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(ProtocolVersionMetadataKey, "1-3"))
	clientRange, err := IncomingProtocolVersionRange(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if clientRange != (hatCommand.ProtocolVersionRange{Min: 1, Max: 3}) {
		t.Fatalf("protocol range = %#v, want 1-3", clientRange)
	}
	selected, err := NegotiateProtocolVersion(ctx, hatCommand.ProtocolVersionRange{Min: 2, Max: 4})
	if err != nil {
		t.Fatal(err)
	}
	if selected != 3 {
		t.Fatalf("selected protocol version = %d, want 3", selected)
	}
	outgoing := AppendProtocolVersionRange(context.Background(), hatCommand.ProtocolVersionRange{Min: 2, Max: 4})
	outgoingMetadata, ok := metadata.FromOutgoingContext(outgoing)
	if !ok || outgoingMetadata.Get(ProtocolVersionMetadataKey)[0] != "2-4" {
		t.Fatalf("outgoing protocol metadata = %#v, want 2-4", outgoingMetadata)
	}
	responseMetadata := ProtocolResponseMetadata(3, hatCommand.ProtocolVersionRange{Min: 2, Max: 4})
	if responseMetadata.Get(ProtocolVersionMetadataKey)[0] != "3" || responseMetadata.Get(ProtocolSupportedMetadataKey)[0] != "2-4" {
		t.Fatalf("response protocol metadata = %#v, want selected 3 and supported 2-4", responseMetadata)
	}
}

func TestGRPCProtocolMetadataRejectsMalformedAndIncompatibleRanges(t *testing.T) {
	malformed := metadata.NewIncomingContext(context.Background(), metadata.Pairs(ProtocolVersionMetadataKey, "1-x"))
	if _, err := IncomingProtocolVersionRange(malformed); !errors.Is(err, hatCommand.ErrInvalidProtocolVersion) {
		t.Fatalf("malformed protocol error = %v, want invalid version", err)
	}
	incompatible := metadata.NewIncomingContext(context.Background(), metadata.Pairs(ProtocolVersionMetadataKey, "2-3"))
	if _, err := NegotiateProtocolVersion(incompatible, hatCommand.ProtocolVersionRange{Min: 1, Max: 1}); !errors.Is(err, hatCommand.ErrIncompatibleProtocolVersion) {
		t.Fatalf("incompatible protocol error = %v, want incompatible version", err)
	}
}

func BenchmarkGRPCProtocolVersionNegotiation(b *testing.B) {
	server := hatCommand.ProtocolVersionRange{Min: 2, Max: 4}
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := NegotiateProtocolVersion(context.Background(), hatCommand.ProtocolVersionRange{Min: 1, Max: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("explicit", func(b *testing.B) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(ProtocolVersionMetadataKey, "1-3"))
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := NegotiateProtocolVersion(ctx, server); err != nil {
				b.Fatal(err)
			}
		}
	})
}
