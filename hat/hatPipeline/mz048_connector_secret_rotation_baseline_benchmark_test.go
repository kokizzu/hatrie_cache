package hatPipeline_test

import (
	"context"
	"testing"

	hatPipeline "hatrie_cache/hat/hatPipeline"
)

type mz048BaselineConnector struct{}

func (mz048BaselineConnector) Start(context.Context) error  { return nil }
func (mz048BaselineConnector) Pause(context.Context) error  { return nil }
func (mz048BaselineConnector) Resume(context.Context) error { return nil }
func (mz048BaselineConnector) Stop(context.Context) error   { return nil }

// BenchmarkMZ048ExistingPauseResume measures the current interruption-based
// credential-refresh fallback before in-place rotation is available.
func BenchmarkMZ048ExistingPauseResume(b *testing.B) {
	registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	connector := mz048BaselineConnector{}
	if err := registry.Register("source", connector); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.Start(ctx, "source"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Pause(ctx, "source"); err != nil {
			b.Fatal(err)
		}
		if err := registry.Resume(ctx, "source"); err != nil {
			b.Fatal(err)
		}
	}
}
