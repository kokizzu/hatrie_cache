package hatSql

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestSQLNamedSettingsProfilesInheritAndValidate(t *testing.T) {
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{
		ValidateSetting: func(key, value string) error {
			switch key {
			case "max_rows":
				_, err := strconv.ParseUint(value, 10, 64)
				return err
			case "timeout":
				_, err := time.ParseDuration(value)
				return err
			default:
				return errors.New("unsupported setting")
			}
		},
	})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	if _, err := registry.PutProfile("base", SQLNamedSettingsProfile{
		Values: map[string]string{"max_rows": "1000", "timeout": "2s"},
	}); err != nil {
		t.Fatalf("PutProfile(base) error = %v", err)
	}
	profile, err := registry.PutProfile("analytics", SQLNamedSettingsProfile{
		Parent: "base",
		Values: map[string]string{"max_rows": "2000"},
	})
	if err != nil {
		t.Fatalf("PutProfile(analytics) error = %v", err)
	}
	if profile.Parent != "base" {
		t.Fatalf("published parent = %q", profile.Parent)
	}

	resolved, err := registry.Resolve("analytics", map[string]string{"timeout": "5s"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved.Revision != profile.Revision || resolved.Parent != "base" ||
		resolved.Values["max_rows"] != "2000" || resolved.Values["timeout"] != "5s" {
		t.Fatalf("resolved profile = %#v", resolved)
	}

	updated, err := registry.PutProfile("base", SQLNamedSettingsProfile{
		Values: map[string]string{"max_rows": "3000", "timeout": "3s"},
	})
	if err != nil {
		t.Fatalf("PutProfile(base update) error = %v", err)
	}
	resolved, err = registry.Resolve("analytics", nil)
	if err != nil {
		t.Fatalf("Resolve() after parent update error = %v", err)
	}
	if resolved.Revision != updated.Revision || resolved.Values["max_rows"] != "2000" || resolved.Values["timeout"] != "3s" {
		t.Fatalf("resolved profile after parent update = %#v", resolved)
	}

	if _, err := registry.PutProfile("invalid", SQLNamedSettingsProfile{
		Values: map[string]string{"max_rows": "not-a-number"},
	}); !errors.Is(err, ErrSQLNamedSettingsSettingInvalid) {
		t.Fatalf("invalid profile error = %v", err)
	}
	if _, err := registry.Resolve("analytics", map[string]string{"timeout": "not-a-duration"}); !errors.Is(err, ErrSQLNamedSettingsSettingInvalid) {
		t.Fatalf("invalid override error = %v", err)
	}
}

func TestSQLNamedSettingsProfilesRejectInvalidParentGraphs(t *testing.T) {
	if _, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{MaxInheritanceDepth: maxSQLNamedSettingsInheritanceDepth + 1}); !errors.Is(err, ErrSQLNamedSettingsLimitInvalid) {
		t.Fatalf("invalid inheritance depth error = %v", err)
	}
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{MaxInheritanceDepth: 1})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	if _, err := registry.PutProfile("missing-child", SQLNamedSettingsProfile{Parent: "missing"}); !errors.Is(err, ErrSQLNamedSettingsParentInvalid) {
		t.Fatalf("missing parent error = %v", err)
	}
	if _, err := registry.PutProfile("base", SQLNamedSettingsProfile{Values: map[string]string{"format": "json"}}); err != nil {
		t.Fatalf("PutProfile(base) error = %v", err)
	}
	child, err := registry.PutProfile("child", SQLNamedSettingsProfile{Parent: "base"})
	if err != nil {
		t.Fatalf("PutProfile(child) error = %v", err)
	}
	if _, err := registry.PutProfile("grandchild", SQLNamedSettingsProfile{Parent: "child"}); !errors.Is(err, ErrSQLNamedSettingsParentInvalid) {
		t.Fatalf("inheritance depth error = %v", err)
	}
	if _, err := registry.PutProfile("base", SQLNamedSettingsProfile{Parent: "child"}); !errors.Is(err, ErrSQLNamedSettingsParentInvalid) {
		t.Fatalf("cycle error = %v", err)
	}
	if err := registry.DeleteIfRevision("base", child.Revision); !errors.Is(err, ErrSQLNamedSettingsParentInUse) {
		t.Fatalf("parent deletion error = %v", err)
	}
}

func TestSQLNamedSettingsProfilesRejectExcessiveEffectiveSettings(t *testing.T) {
	registry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{MaxSettingsPerCollection: 2})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry() error = %v", err)
	}
	if _, err := registry.PutProfile("base", SQLNamedSettingsProfile{
		Values: map[string]string{"one": "1", "two": "2"},
	}); err != nil {
		t.Fatalf("PutProfile(base) error = %v", err)
	}
	if _, err := registry.PutProfile("child", SQLNamedSettingsProfile{
		Parent: "base",
		Values: map[string]string{"three": "3"},
	}); !errors.Is(err, ErrSQLNamedSettingsLimitExceeded) {
		t.Fatalf("effective settings limit error = %v", err)
	}

	atomicRegistry, err := NewSQLNamedSettingsRegistry(SQLNamedSettingsRegistryOptions{MaxSettingsPerCollection: 2})
	if err != nil {
		t.Fatalf("NewSQLNamedSettingsRegistry(atomic) error = %v", err)
	}
	if _, err := atomicRegistry.PutProfile("base", SQLNamedSettingsProfile{Values: map[string]string{"one": "1"}}); err != nil {
		t.Fatalf("PutProfile(atomic base) error = %v", err)
	}
	if _, err := atomicRegistry.PutProfile("child", SQLNamedSettingsProfile{Parent: "base", Values: map[string]string{"two": "2"}}); err != nil {
		t.Fatalf("PutProfile(atomic child) error = %v", err)
	}
	if _, err := atomicRegistry.PutProfile("base", SQLNamedSettingsProfile{Values: map[string]string{"one": "1", "three": "3"}}); !errors.Is(err, ErrSQLNamedSettingsLimitExceeded) {
		t.Fatalf("invalid parent update error = %v", err)
	}
	resolved, err := atomicRegistry.Resolve("child", nil)
	if err != nil || len(resolved.Values) != 2 || resolved.Values["one"] != "1" {
		t.Fatalf("child after rejected parent update = %#v, error %v", resolved, err)
	}
}
