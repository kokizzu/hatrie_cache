package hatExtension

import (
	"context"
	"fmt"
)

func ExampleRegistry_Load() {
	manifest := testManifest()
	registry := NewRegistry()
	_, err := registry.Load(context.Background(), LoaderFunc(func(_ context.Context, requested Manifest) (NativeExtension, error) {
		return testNativeExtension{manifest: requested}, nil
	}), manifest, "")
	if err != nil {
		panic(err)
	}
	metadata, _ := registry.Metadata("geo")
	fmt.Println(metadata.Manifest.Name, metadata.Generation)
	// Output: geo 1
}
