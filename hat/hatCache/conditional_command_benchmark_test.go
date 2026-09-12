package hatCache

import "testing"

func BenchmarkCompareAndSwapString(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("bench:cas", "old")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		expected, replacement := "old", "new"
		if iteration&1 == 1 {
			expected, replacement = replacement, expected
		}
		swapped, err := ht.CompareAndSwapString("bench:cas", expected, replacement)
		if err != nil || !swapped {
			b.Fatalf("CompareAndSwapString() = %v/%v, want true/nil", swapped, err)
		}
	}
}

func BenchmarkGetThenSetString(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("bench:cas", "old")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		expected, replacement := "old", "new"
		if iteration&1 == 1 {
			expected, replacement = replacement, expected
		}
		if got := ht.GetString("bench:cas"); got != expected {
			b.Fatalf("GetString() = %q, want %q", got, expected)
		}
		ht.UpsertString("bench:cas", replacement)
	}
}

func BenchmarkExecuteCommandCASString(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("bench:cas", "old")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		expected, replacement := "old", "new"
		if iteration&1 == 1 {
			expected, replacement = replacement, expected
		}
		response := ht.ExecuteCommand(CacheCommandRequest{
			Command:       "CAS",
			Key:           "bench:cas",
			ExpectedValue: expected,
			Value:         replacement,
		})
		if !response.OK || response.Value != "1" {
			b.Fatalf("CAS response = %#v, want ok/1", response)
		}
	}
}

func BenchmarkExecuteCommandGetThenSetString(b *testing.B) {
	ht := CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("bench:cas", "old")

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		expected, replacement := "old", "new"
		if iteration&1 == 1 {
			expected, replacement = replacement, expected
		}
		read := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "bench:cas"})
		if !read.OK || read.Value != expected {
			b.Fatalf("GET response = %#v, want ok/%q", read, expected)
		}
		stored := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "bench:cas", Value: replacement})
		if !stored.OK {
			b.Fatalf("SETSTR response = %#v, want success", stored)
		}
	}
}
