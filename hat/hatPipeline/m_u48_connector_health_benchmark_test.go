package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

type healthBenchmarkConnector struct {
	failures int
}

func (c *healthBenchmarkConnector) Start(context.Context) error {
	if c.failures > 0 {
		c.failures--
		return errors.New("temporary benchmark failure")
	}
	return nil
}

func (c *healthBenchmarkConnector) Pause(context.Context) error  { return nil }
func (c *healthBenchmarkConnector) Resume(context.Context) error { return nil }
func (c *healthBenchmarkConnector) Stop(context.Context) error   { return nil }

func BenchmarkConnectorStartLifecycle(b *testing.B) {
	for index := 0; index < b.N; index++ {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", &healthBenchmarkConnector{}); err != nil {
			b.Fatal(err)
		}
		if err := registry.Start(context.Background(), "source"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorStartWithHealthPolicySuccess(b *testing.B) {
	policy := ConnectorHealthPolicy{MaxAttempts: 1, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}
	for index := 0; index < b.N; index++ {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", &healthBenchmarkConnector{}); err != nil {
			b.Fatal(err)
		}
		if _, err := registry.StartWithHealthPolicy(context.Background(), "source", policy); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnectorStartWithHealthPolicyOneRetry(b *testing.B) {
	policy := ConnectorHealthPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}
	for index := 0; index < b.N; index++ {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Register("source", &healthBenchmarkConnector{failures: 1}); err != nil {
			b.Fatal(err)
		}
		if _, err := registry.StartWithHealthPolicy(context.Background(), "source", policy); err != nil {
			b.Fatal(err)
		}
	}
}
