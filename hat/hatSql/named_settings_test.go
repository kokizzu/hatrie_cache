package hatSql

import (
	"errors"
	"strings"
	"testing"
)

func TestSQLNamedSettingsRegistryPublishesImmutableVersionedSnapshots(t *testing.T) {
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	input := map[string]string{"max_rows": "1000", "timeout": "2s"}
	profile, err := registry.Put("oltp", input)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if profile.Name != "oltp" || profile.Revision != 1 || len(profile.Values) != 2 {
		t.Fatalf("published profile = %#v", profile)
	}
	input["max_rows"] = "mutated"
	profile.Values["timeout"] = "mutated"

	got, ok := registry.Lookup("oltp")
	if !ok || got.Values["max_rows"] != "1000" || got.Values["timeout"] != "2s" {
		t.Fatalf("stored profile = %#v, found %v", got, ok)
	}
	got.Values["max_rows"] = "caller mutation"
	again, ok := registry.Lookup("oltp")
	if !ok || again.Values["max_rows"] != "1000" {
		t.Fatalf("lookup returned internal map: %#v", again)
	}

	resolved, err := registry.Resolve("oltp", map[string]string{"timeout": "5s", "readonly": "true"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Revision != 1 || resolved.Values["max_rows"] != "1000" || resolved.Values["timeout"] != "5s" || resolved.Values["readonly"] != "true" {
		t.Fatalf("resolved profile = %#v", resolved)
	}

	snapshot := registry.Snapshot()
	if snapshot.Revision != 1 || len(snapshot.Collections) != 1 || snapshot.Collections[0].Name != "oltp" {
		t.Fatalf("registry snapshot = %#v", snapshot)
	}
	snapshot.Collections[0].Values["max_rows"] = "snapshot mutation"
	again, ok = registry.Lookup("oltp")
	if !ok || again.Values["max_rows"] != "1000" {
		t.Fatalf("snapshot returned internal map: %#v", again)
	}
	stats := registry.Stats()
	if stats.Revision != 1 || stats.CollectionCount != 1 || stats.SettingCount != 2 {
		t.Fatalf("registry stats = %#v", stats)
	}
}

func TestSQLNamedSettingsRegistryUsesRevisionCompareAndSwap(t *testing.T) {
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	first, err := registry.Put("analytics", map[string]string{"format": "columnar"})
	if err != nil {
		t.Fatalf("initial Put() error = %v", err)
	}
	second, err := registry.PutIfRevision("analytics", first.Revision, map[string]string{"format": "row"})
	if err != nil {
		t.Fatalf("PutIfRevision() error = %v", err)
	}
	if second.Revision != 2 || second.Values["format"] != "row" {
		t.Fatalf("updated profile = %#v", second)
	}
	if _, err := registry.PutIfRevision("analytics", first.Revision, map[string]string{"format": "stale"}); !errors.Is(err, ErrSQLNamedSettingsConflict) {
		t.Fatalf("stale PutIfRevision() error = %v", err)
	}
	if err := registry.DeleteIfRevision("analytics", first.Revision); !errors.Is(err, ErrSQLNamedSettingsConflict) {
		t.Fatalf("stale DeleteIfRevision() error = %v", err)
	}
	if err := registry.DeleteIfRevision("analytics", second.Revision); err != nil {
		t.Fatalf("DeleteIfRevision() error = %v", err)
	}
	if _, ok := registry.Lookup("analytics"); ok {
		t.Fatal("deleted collection still exists")
	}
	if err := registry.DeleteIfRevision("analytics", 3); !errors.Is(err, ErrSQLNamedSettingsCollectionNotFound) {
		t.Fatalf("missing DeleteIfRevision() error = %v", err)
	}
}

func TestSQLNamedSettingsRegistryRejectsInvalidOrExcessiveSettings(t *testing.T) {
	if _, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{MaxCollections: -1}); !errors.Is(err, ErrSQLNamedSettingsLimitInvalid) {
		t.Fatalf("invalid options error = %v", err)
	}
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{
		MaxCollections:           1,
		MaxSettingsPerCollection: 1,
		MaxSettingValueBytes:     4,
	})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	tests := []struct {
		label      string
		collection string
		values     map[string]string
		want       error
	}{
		{label: "missing name", collection: "", values: map[string]string{"ok": "yes"}, want: ErrSQLNamedSettingsNameRequired},
		{label: "missing setting", collection: "invalid", values: map[string]string{"": "yes"}, want: ErrSQLNamedSettingsSettingInvalid},
		{label: "oversized value", collection: "invalid", values: map[string]string{"ok": "longer"}, want: ErrSQLNamedSettingsSettingInvalid},
	}
	for _, test := range tests {
		if _, err := registry.Put(test.collection, test.values); !errors.Is(err, test.want) {
			t.Errorf("Put(%s) error = %v, want %v", test.label, err, test.want)
		}
	}
	if _, err := registry.Put("one", map[string]string{"ok": "yes"}); err != nil {
		t.Fatalf("first bounded Put() error = %v", err)
	}
	if _, err := registry.Put("two", map[string]string{"ok": "yes"}); !errors.Is(err, ErrSQLNamedSettingsLimitExceeded) {
		t.Fatalf("collection limit error = %v", err)
	}
	if _, err := registry.Resolve("missing", nil); !errors.Is(err, ErrSQLNamedSettingsCollectionNotFound) {
		t.Fatalf("missing Resolve() error = %v", err)
	}
	if _, err := registry.Put(" ", map[string]string{"ok": "yes"}); err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("space-only collection name error = %v", err)
	}
}

func TestSQLNamedSettingsRegistryZeroValueIsUsable(t *testing.T) {
	var registry SQLNamedSettingsRegistry
	profile, err := registry.Put("default", map[string]string{"format": "json"})
	if err != nil {
		t.Fatalf("zero-value Put() error = %v", err)
	}
	if profile.Revision != 1 {
		t.Fatalf("zero-value revision = %d", profile.Revision)
	}
}

func TestSQLNamedSettingsRegistryLooksUpOneValueWithoutExposingState(t *testing.T) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"timeout": "2s"}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	value, ok := registry.LookupValue("runtime", "timeout")
	if !ok || value != "2s" {
		t.Fatalf("LookupValue() = %q, %v", value, ok)
	}
	if _, ok := registry.LookupValue("runtime", "missing"); ok {
		t.Fatal("missing LookupValue() reported present")
	}
}
