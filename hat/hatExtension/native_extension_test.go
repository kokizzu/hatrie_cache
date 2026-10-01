package hatExtension

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

type testNativeExtension struct {
	manifest Manifest
}

func (extension testNativeExtension) Manifest() Manifest { return extension.manifest }

func (extension testNativeExtension) Invoke(_ context.Context, request []byte) ([]byte, error) {
	return append([]byte(nil), request...), nil
}

type testNativeLoader struct {
	extension NativeExtension
}

func (loader testNativeLoader) Load(_ context.Context, _ Manifest) (NativeExtension, error) {
	return loader.extension, nil
}

func testManifest() Manifest {
	return Manifest{
		Name:           "geo",
		Version:        "1.2.3",
		ABI:            NativeExtensionABIV1,
		EntryPoint:     "hatrie_extension_init_v1",
		Platform:       "linux/amd64",
		ChecksumSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Capabilities:   []Capability{CapabilitySQLFunction, CapabilitySQLIndex},
	}
}

func TestNativeExtensionManifestNormalizesWithoutAliasing(t *testing.T) {
	manifest := testManifest()
	manifest.Capabilities[0] = Capability(" sql.function ")

	normalized, err := manifest.Normalize()
	if err != nil {
		t.Fatalf("normalize manifest: %v", err)
	}
	if normalized.Name != manifest.Name || normalized.ABI != manifest.ABI {
		t.Fatalf("unexpected normalized identity: %#v", normalized)
	}
	if len(normalized.Capabilities) != 2 || normalized.Capabilities[0] != CapabilitySQLFunction || normalized.Capabilities[1] != CapabilitySQLIndex {
		t.Fatalf("unexpected capabilities: %#v", normalized.Capabilities)
	}
	normalized.Capabilities[0] = Capability("mutated")
	if manifest.Capabilities[0] != Capability(" sql.function ") {
		t.Fatalf("normalization aliased caller capabilities")
	}
}

func TestNativeExtensionRegistryLoadResolveAndVersionFence(t *testing.T) {
	registry := NewRegistry()
	first := testNativeExtension{manifest: testManifest()}
	metadata, err := registry.Register(first, "")
	if err != nil {
		t.Fatalf("register first extension: %v", err)
	}
	if metadata.Generation != 1 {
		t.Fatalf("first generation = %d", metadata.Generation)
	}

	resolved, ok := registry.Resolve("geo")
	if !ok || resolved == nil {
		t.Fatalf("resolve failed: ok=%v extension=%#v", ok, resolved)
	}
	resolvedMetadata, ok := registry.Metadata("geo")
	if !ok || !reflect.DeepEqual(resolvedMetadata, metadata) {
		t.Fatalf("resolved metadata mismatch: got %#v want %#v", resolvedMetadata, metadata)
	}
	response, err := resolved.Invoke(context.Background(), []byte("request"))
	if err != nil || string(response) != "request" {
		t.Fatalf("invoke result = %q, %v", response, err)
	}

	replacementManifest := testManifest()
	replacementManifest.Version = "1.2.4"
	replacement := testNativeExtension{manifest: replacementManifest}
	if _, err := registry.Register(replacement, "stale"); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale replacement error = %v", err)
	}
	replacementMetadata, err := registry.Register(replacement, "1.2.3")
	if err != nil {
		t.Fatalf("replace extension: %v", err)
	}
	if replacementMetadata.Generation != 2 {
		t.Fatalf("replacement generation = %d", replacementMetadata.Generation)
	}
	if err := registry.Unregister("geo", "1.2.3"); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale unregister error = %v", err)
	}
	if err := registry.Unregister("geo", "1.2.4"); err != nil {
		t.Fatalf("unregister extension: %v", err)
	}
}

func TestNativeExtensionRegistryLoaderVerifiesManifest(t *testing.T) {
	manifest := testManifest()
	registry := NewRegistry()
	loaded := testNativeExtension{manifest: manifest}
	metadata, err := registry.Load(context.Background(), testNativeLoader{extension: loaded}, manifest, "")
	if err != nil {
		t.Fatalf("load extension: %v", err)
	}
	if metadata.Manifest.Name != manifest.Name {
		t.Fatalf("loaded metadata = %#v", metadata)
	}

	mismatched := manifest
	mismatched.Version = "9.9.9"
	if _, err := registry.Load(context.Background(), testNativeLoader{extension: loaded}, mismatched, ""); !errors.Is(err, ErrManifestMismatch) {
		t.Fatalf("mismatched loader error = %v", err)
	}
}

func TestNativeExtensionManifestRejectsUnsafeOrUnsupportedValues(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Manifest)
		target error
	}{
		{name: "abi", mutate: func(manifest *Manifest) { manifest.ABI = "native/v0" }, target: ErrABIMismatch},
		{name: "checksum", mutate: func(manifest *Manifest) { manifest.ChecksumSHA256 = "bad" }, target: ErrManifestInvalid},
		{name: "entry point", mutate: func(manifest *Manifest) { manifest.EntryPoint = "init;load" }, target: ErrManifestInvalid},
		{name: "duplicate capability", mutate: func(manifest *Manifest) {
			manifest.Capabilities = []Capability{CapabilitySQLFunction, CapabilitySQLFunction}
		}, target: ErrManifestInvalid},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			manifest := testManifest()
			testCase.mutate(&manifest)
			if _, err := manifest.Normalize(); !errors.Is(err, testCase.target) {
				t.Fatalf("normalize error = %v, want %v", err, testCase.target)
			}
		})
	}
}

func TestNativeExtensionRegistryDefensiveMetadataAndOrdering(t *testing.T) {
	registry := NewRegistry()
	firstManifest := testManifest()
	firstManifest.Name = "zeta"
	secondManifest := testManifest()
	secondManifest.Name = "alpha"
	if _, err := registry.Register(testNativeExtension{manifest: firstManifest}, ""); err != nil {
		t.Fatalf("register zeta: %v", err)
	}
	if _, err := registry.Register(testNativeExtension{manifest: secondManifest}, ""); err != nil {
		t.Fatalf("register alpha: %v", err)
	}

	metadata, ok := registry.Metadata("zeta")
	if !ok {
		t.Fatal("missing zeta metadata")
	}
	metadata.Manifest.Capabilities[0] = Capability("mutated")
	unchanged, ok := registry.Metadata("zeta")
	if !ok || unchanged.Manifest.Capabilities[0] == Capability("mutated") {
		t.Fatalf("metadata escaped registry ownership: %#v", unchanged)
	}

	snapshot := registry.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Manifest.Name != "alpha" || snapshot[1].Manifest.Name != "zeta" {
		t.Fatalf("snapshot order = %#v", snapshot)
	}
}

func TestNativeExtensionRegistryRejectsNilAndUnsafeVersionTransitions(t *testing.T) {
	registry := NewRegistry()
	var nilExtension *testNativeExtension
	if _, err := registry.Register(nilExtension, ""); !errors.Is(err, ErrExtensionNil) {
		t.Fatalf("nil extension error = %v", err)
	}
	if _, err := registry.Load(context.Background(), nil, testManifest(), ""); !errors.Is(err, ErrLoaderNil) {
		t.Fatalf("nil loader error = %v", err)
	}

	manifest := testManifest()
	if _, err := registry.Register(testNativeExtension{manifest: manifest}, ""); err != nil {
		t.Fatalf("register initial extension: %v", err)
	}
	if _, err := registry.Register(testNativeExtension{manifest: manifest}, "1.2.3"); !errors.Is(err, ErrVersionUnchanged) {
		t.Fatalf("unchanged version error = %v", err)
	}
	manifest.Version = "1.2.4"
	if _, err := registry.Register(testNativeExtension{manifest: manifest}, ""); !errors.Is(err, ErrVersionRequired) {
		t.Fatalf("unfenced replacement error = %v", err)
	}
}

func TestNativeExtensionRegistryConcurrentReadersAndWriters(t *testing.T) {
	registry := NewRegistry()
	manifest := testManifest()
	if _, err := registry.Register(testNativeExtension{manifest: manifest}, ""); err != nil {
		t.Fatalf("register initial extension: %v", err)
	}

	var failed atomic.Bool
	var readers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for iteration := 0; iteration < 2000; iteration++ {
				if extension, ok := registry.Resolve("geo"); !ok || extension == nil {
					failed.Store(true)
				}
				if metadata, ok := registry.Metadata("geo"); !ok || metadata.Manifest.Name != "geo" {
					failed.Store(true)
				}
			}
		}()
	}

	previousVersion := manifest.Version
	for version := 4; version < 24; version++ {
		manifest.Version = "1.2." + strconv.Itoa(version)
		metadata, err := registry.Register(testNativeExtension{manifest: manifest}, previousVersion)
		if err != nil {
			t.Fatalf("replace version %s: %v", manifest.Version, err)
		}
		previousVersion = metadata.Manifest.Version
	}
	readers.Wait()
	if failed.Load() {
		t.Fatal("concurrent registry access observed an invalid result")
	}
}
