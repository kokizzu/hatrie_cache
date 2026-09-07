package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type registryPlugin struct {
	name    string
	version string
}

func (plugin registryPlugin) PluginName() string    { return plugin.name }
func (plugin registryPlugin) PluginVersion() string { return plugin.version }

func TestPluginRegistryLoadsReplacesAndUnloadsWithExpectedVersions(t *testing.T) {
	registry := hatSql.NewPluginRegistry()
	first := registryPlugin{name: "geo", version: "1.0.0"}
	second := registryPlugin{name: "geo", version: "2.0.0"}

	loaded, err := registry.Load(first, "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "geo" || loaded.Version != "1.0.0" || loaded.Generation != 1 {
		t.Fatalf("initial load = %#v", loaded)
	}
	if plugin, ok := registry.Resolve("geo"); !ok || plugin.PluginVersion() != "1.0.0" {
		t.Fatalf("initial Resolve() = %#v/%t", plugin, ok)
	}

	if _, err := registry.Load(second, "stale"); !errors.Is(err, hatSql.ErrPluginVersionConflict) {
		t.Fatalf("stale replacement error = %v", err)
	}
	if loaded, err := registry.Load(second, "1.0.0"); err != nil || loaded.Generation != 2 || loaded.Version != "2.0.0" {
		t.Fatalf("replacement = %#v/%v", loaded, err)
	}
	if _, err := registry.Load(second, "2.0.0"); !errors.Is(err, hatSql.ErrPluginVersionUnchanged) {
		t.Fatalf("same-version replacement error = %v", err)
	}
	if err := registry.Unload("geo", "1.0.0"); !errors.Is(err, hatSql.ErrPluginVersionConflict) {
		t.Fatalf("stale unload error = %v", err)
	}
	if err := registry.Unload("geo", "2.0.0"); err != nil {
		t.Fatalf("unload error = %v", err)
	}
	if _, ok := registry.Resolve("geo"); ok {
		t.Fatal("Resolve() found unloaded plugin")
	}
}

func TestPluginRegistryValidatesPluginsAndMissingExpectedVersions(t *testing.T) {
	registry := hatSql.NewPluginRegistry()
	invalid := []hatSql.Plugin{
		registryPlugin{version: "1.0.0"},
		registryPlugin{name: "geo"},
		nil,
	}
	for _, plugin := range invalid {
		if _, err := registry.Load(plugin, ""); !errors.Is(err, hatSql.ErrPluginInvalid) {
			t.Fatalf("invalid plugin %#v error = %v", plugin, err)
		}
	}
	if _, err := registry.Load(registryPlugin{name: "geo", version: "1.0.0"}, "unexpected"); !errors.Is(err, hatSql.ErrPluginVersionConflict) {
		t.Fatalf("unexpected initial version error = %v", err)
	}
	if _, err := registry.Load(registryPlugin{name: "geo", version: "1.0.0"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Load(registryPlugin{name: "geo", version: "2.0.0"}, ""); !errors.Is(err, hatSql.ErrPluginVersionRequired) {
		t.Fatalf("missing replacement version error = %v", err)
	}
	var nilRegistry *hatSql.PluginRegistry
	if _, ok := nilRegistry.Resolve("geo"); ok {
		t.Fatal("nil registry Resolve() found a plugin")
	}
}
