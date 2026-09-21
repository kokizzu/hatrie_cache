package hatPipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	hatPipeline "hatrie_cache/hat/hatPipeline"
)

type mz048RotatingConnector struct {
	mu      sync.Mutex
	version uint64
	value   []byte
}

func (connector *mz048RotatingConnector) Start(context.Context) error  { return nil }
func (connector *mz048RotatingConnector) Pause(context.Context) error  { return nil }
func (connector *mz048RotatingConnector) Resume(context.Context) error { return nil }
func (connector *mz048RotatingConnector) Stop(context.Context) error   { return nil }

func (connector *mz048RotatingConnector) RotateCredentials(ctx context.Context, rotation hatPipeline.ConnectorCredentialRotation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	connector.mu.Lock()
	defer connector.mu.Unlock()
	connector.version = rotation.Version
	connector.value = append(connector.value[:0], rotation.Value...)
	return nil
}

func (connector *mz048RotatingConnector) credential() (uint64, string) {
	connector.mu.Lock()
	defer connector.mu.Unlock()
	return connector.version, string(connector.value)
}

func TestMZ048RotateCredentialsKeepsConnectorRunning(t *testing.T) {
	registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	connector := &mz048RotatingConnector{}
	if err := registry.Register("source", connector); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.Start(ctx, "source"); err != nil {
		t.Fatal(err)
	}
	value := []byte("rotated-secret")
	if err := registry.RotateCredentials(ctx, "source", hatPipeline.ConnectorCredentialRotation{
		Version: 2,
		Value:   value,
	}); err != nil {
		t.Fatalf("RotateCredentials() error = %v", err)
	}
	value[0] = 'X'
	version, current := connector.credential()
	if version != 2 || current != "rotated-secret" {
		t.Fatalf("credential = version %d value %q, want copied version 2", version, current)
	}
	status, ok := registry.Status("source")
	if !ok || status.State != hatPipeline.ConnectorRunning {
		t.Fatalf("Status() = %#v/%v, want running", status, ok)
	}
}

func TestMZ048RotateCredentialsValidatesSupportAndBounds(t *testing.T) {
	ctx := context.Background()
	if _, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{MaxCredentialBytes: 64<<20 + 1}); !errors.Is(err, hatPipeline.ErrConnectorCredentialLimitInvalid) {
		t.Fatalf("oversized registry credential limit error = %v, want invalid", err)
	}
	registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{MaxCredentialBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("plain", mz048BaselineConnector{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(ctx, "plain"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RotateCredentials(ctx, "plain", hatPipeline.ConnectorCredentialRotation{Version: 1, Value: []byte("x")}); !errors.Is(err, hatPipeline.ErrConnectorCredentialRotationUnsupported) {
		t.Fatalf("unsupported RotateCredentials() error = %v, want unsupported", err)
	}
	if err := registry.Register("rotating", &mz048RotatingConnector{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(ctx, "rotating"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RotateCredentials(ctx, "rotating", hatPipeline.ConnectorCredentialRotation{Value: []byte("x")}); !errors.Is(err, hatPipeline.ErrConnectorCredentialRotationInvalid) {
		t.Fatalf("zero-version RotateCredentials() error = %v, want invalid", err)
	}
	if err := registry.RotateCredentials(ctx, "rotating", hatPipeline.ConnectorCredentialRotation{Version: 1, Value: []byte("12345")}); !errors.Is(err, hatPipeline.ErrConnectorCredentialRotationInvalid) {
		t.Fatalf("oversized RotateCredentials() error = %v, want invalid", err)
	}
}

func TestMZ048RotateCredentialsHonorsCancellation(t *testing.T) {
	registry, err := hatPipeline.NewConnectorRegistry(hatPipeline.ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("source", &mz048RotatingConnector{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := registry.Start(ctx, "source"); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := registry.RotateCredentials(canceled, "source", hatPipeline.ConnectorCredentialRotation{Version: 1, Value: []byte("secret")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RotateCredentials() error = %v, want context.Canceled", err)
	}
}
