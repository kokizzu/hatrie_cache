package hatSql

import "testing"

func BenchmarkCH050BaselineNamedSettingsMapLookup(b *testing.B) {
	values := map[string]string{"max_rows": "1000", "timeout": "2s"}
	var value string
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value = values["max_rows"]
	}
	b.StopTimer()
	if value == "" {
		b.Fatal("baseline was optimized away")
	}
}

func BenchmarkCH050NamedSettingsLookupValue(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"max_rows": "1000", "timeout": "2s"}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var value string
	var ok bool
	for index := 0; index < b.N; index++ {
		value, ok = registry.LookupValue("runtime", "max_rows")
	}
	b.StopTimer()
	if !ok || value == "" {
		b.Fatal("LookupValue() did not return a value")
	}
}

func BenchmarkCH050NamedSettingsLookupCollection(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"max_rows": "1000", "timeout": "2s"}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var profile SQLNamedSettingsCollection
	var ok bool
	for index := 0; index < b.N; index++ {
		profile, ok = registry.Lookup("runtime")
	}
	b.StopTimer()
	if !ok || len(profile.Values) != 2 {
		b.Fatal("Lookup() did not return a profile")
	}
}

func BenchmarkCH050NamedSettingsResolve(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"max_rows": "1000", "timeout": "2s"}); err != nil {
		b.Fatal(err)
	}
	overrides := map[string]string{"timeout": "5s"}
	b.ResetTimer()
	var profile SQLNamedSettingsCollection
	var err error
	for index := 0; index < b.N; index++ {
		profile, err = registry.Resolve("runtime", overrides)
	}
	b.StopTimer()
	if err != nil || profile.Values["timeout"] != "5s" {
		b.Fatalf("Resolve() = %#v, error %v", profile, err)
	}
}

func BenchmarkCH050NamedSettingsPut(b *testing.B) {
	var registry SQLNamedSettingsRegistry
	if _, err := registry.Put("runtime", map[string]string{"max_rows": "1000", "timeout": "2s"}); err != nil {
		b.Fatal(err)
	}
	values := map[string]string{"max_rows": "1000", "timeout": "5s"}
	b.ResetTimer()
	var err error
	for index := 0; index < b.N; index++ {
		_, err = registry.Put("runtime", values)
	}
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
}
