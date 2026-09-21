package hatPipeline_test

import (
	"context"
	"testing"

	hatPipeline "hatrie_cache/hat/hatPipeline"
)

// BenchmarkMZ048InPlaceRotation measures credential publication without a
// lifecycle interruption.
func BenchmarkMZ048InPlaceRotation(b *testing.B) {
	registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register("source", &mz048RotatingConnector{}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.Start(ctx, "source"); err != nil {
		b.Fatal(err)
	}
	rotation := hatPipeline.ConnectorCredentialRotation{
		Version: 1,
		Value:   []byte("rotated-secret"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rotation.Version++
		if err := registry.RotateCredentials(ctx, "source", rotation); err != nil {
			b.Fatal(err)
		}
	}
}
