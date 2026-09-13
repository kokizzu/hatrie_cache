package hatSql

import "testing"

func BenchmarkCH001BaselineManualInheritedResolve(b *testing.B) {
	parent := map[string]string{"max_rows": "1000", "timeout": "2s"}
	child := map[string]string{"max_rows": "2000", "format": "json"}
	overrides := map[string]string{"timeout": "5s"}
	var resolved map[string]string
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		resolved = make(map[string]string, len(parent)+len(child))
		for key, value := range parent {
			resolved[key] = value
		}
		for key, value := range child {
			resolved[key] = value
		}
		for key, value := range overrides {
			resolved[key] = value
		}
	}
	b.StopTimer()
	if resolved["max_rows"] != "2000" || resolved["timeout"] != "5s" || resolved["format"] != "json" {
		b.Fatalf("manual resolve = %#v", resolved)
	}
}

func BenchmarkCH001BaselineNamedSettingsResolveParentless(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"max_rows": "1000", "timeout": "2s"}); err != nil {
		b.Fatal(err)
	}
	overrides := map[string]string{"timeout": "5s"}
	var profile SQLNamedSettingsCollection
	var err error
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		profile, err = registry.Resolve("runtime", overrides)
	}
	b.StopTimer()
	if err != nil || profile.Values["timeout"] != "5s" {
		b.Fatalf("Resolve() = %#v, error %v", profile, err)
	}
}

func BenchmarkCH001NamedSettingsResolveInherited(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.PutProfile("base", SQLNamedSettingsProfile{
		Values: map[string]string{"max_rows": "1000", "timeout": "2s"},
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := registry.PutProfile("analytics", SQLNamedSettingsProfile{
		Parent: "base",
		Values: map[string]string{"max_rows": "2000", "format": "json"},
	}); err != nil {
		b.Fatal(err)
	}
	overrides := map[string]string{"timeout": "5s"}
	var profile SQLNamedSettingsCollection
	var err error
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		profile, err = registry.Resolve("analytics", overrides)
	}
	b.StopTimer()
	if err != nil || profile.Values["max_rows"] != "2000" || profile.Values["timeout"] != "5s" || profile.Values["format"] != "json" {
		b.Fatalf("Resolve() = %#v, error %v", profile, err)
	}
}
