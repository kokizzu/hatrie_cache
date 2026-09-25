package hatStorage

import "testing"

var ch020BaselineRegistrationSink RemotePartReference
var ch020RegistryRegistrationSink RemotePartRegistration

func BenchmarkCH020BaselineMapLookup(b *testing.B) {
	reference := ch020BenchmarkReference(b)
	entries := make(map[string]RemotePartReference, 1024)
	for index := 0; index < 1024; index++ {
		entries["part-"+stringIntCH020(index)] = reference
	}
	key := "part-512"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := entries[key]
		if !ok {
			b.Fatal("baseline map lookup missed")
		}
		ch020BaselineRegistrationSink = value
	}
}

func BenchmarkCH020RegistryLookup(b *testing.B) {
	reference := ch020BenchmarkReference(b)
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: 1024})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := registry.Register(RemotePartRegistration{
			Key:        "part-" + stringIntCH020(index),
			Reference:  reference,
			Generation: 1,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := registry.Lookup("part-512")
		if !ok {
			b.Fatal("registry lookup missed")
		}
		ch020RegistryRegistrationSink = value
	}
}

func BenchmarkCH020BaselineMapReplace(b *testing.B) {
	reference := ch020BenchmarkReference(b)
	entries := make(map[string]RemotePartReference, 1024)
	for index := 0; index < 1024; index++ {
		entries["part-"+stringIntCH020(index)] = reference
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 1; index <= b.N; index++ {
		entries["part-512"] = reference
	}
}

func BenchmarkCH020RegistryReplace(b *testing.B) {
	reference := ch020BenchmarkReference(b)
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: 1024})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := registry.Register(RemotePartRegistration{
			Key:        "part-" + stringIntCH020(index),
			Reference:  reference,
			Generation: 1,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := uint64(2); index <= uint64(b.N)+1; index++ {
		if _, err := registry.Register(RemotePartRegistration{
			Key:        "part-512",
			Reference:  reference,
			Generation: index,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func ch020BenchmarkReference(b *testing.B) RemotePartReference {
	b.Helper()
	reference, err := NewRemotePartReference(
		"https://objects.example.test/parts/ch020.bin",
		"parts/ch020.bin",
		"sha256:ch020-benchmark",
		4096,
	)
	if err != nil {
		b.Fatal(err)
	}
	return reference
}

func stringIntCH020(value int) string {
	if value == 0 {
		return "0"
	}
	var reversed [20]byte
	index := len(reversed)
	for value > 0 {
		index--
		reversed[index] = byte('0' + value%10)
		value /= 10
	}
	return string(reversed[index:])
}
