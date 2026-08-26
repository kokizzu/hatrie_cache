package hatCache

import (
	"fmt"
	"reflect"
	"testing"
)

func TestExecuteExactFastCommandGenericGetTopKMatchesGeneric(t *testing.T) {
	for _, structured := range []bool{false, true} {
		name := "strings"
		if structured {
			name = "structured"
		}
		t.Run(name, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			seedTopKGenericGet(t, fast, 16, structured)
			seedTopKGenericGet(t, generic, 16, structured)

			got, ok := fast.executeExactFastCommand(CacheCommandRequest{Command: "GET", Key: "topk"})
			if !ok {
				t.Fatal("executeExactFastCommand(GET Top-K) ok = false, want true")
			}
			want := generic.ExecuteCommand(CacheCommandRequest{Command: " GET", Key: "topk"})
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fast response = %#v, generic response = %#v", got, want)
			}
			gotStats, wantStats := fast.Stats(), generic.Stats()
			if gotStats.Reads != wantStats.Reads || gotStats.Hits != wantStats.Hits || gotStats.Misses != wantStats.Misses || gotStats.Writes != wantStats.Writes {
				t.Fatalf("fast stats = %#v, generic stats = %#v", gotStats, wantStats)
			}
		})
	}
}

func BenchmarkTopKGenericGetCommand(b *testing.B) {
	for _, size := range []int{16, 100} {
		for _, structured := range []bool{false, true} {
			kind := "Strings"
			if structured {
				kind = "Structured"
			}
			b.Run(fmt.Sprintf("%s%d", kind, size), func(b *testing.B) {
				for _, mode := range []struct {
					name    string
					command string
				}{
					{name: "Generic", command: " GET"},
					{name: "Exact", command: "GET"},
				} {
					b.Run(mode.name, func(b *testing.B) {
						ht := CreateHatTrie()
						defer ht.Destroy()
						seedTopKGenericGet(b, ht, size, structured)
						request := CacheCommandRequest{Command: mode.command, Key: "topk"}
						b.ReportAllocs()
						b.ResetTimer()
						for idx := 0; idx < b.N; idx++ {
							response := ht.ExecuteCommand(request)
							if !response.OK {
								b.Fatalf("ExecuteCommand() = %#v, want ok", response)
							}
						}
					})
				}
			})
		}
	}
}

type topKGenericGetTestHelper interface {
	Helper()
	Fatalf(string, ...interface{})
}

func seedTopKGenericGet(tb topKGenericGetTestHelper, ht *HatTrie, size int, structured bool) {
	tb.Helper()
	if err := ht.UpsertTopK("topk", uint64(size)); err != nil {
		tb.Fatalf("UpsertTopK() error = %v", err)
	}
	stringItems := size
	if structured {
		stringItems--
	}
	for idx := 0; idx < stringItems; idx++ {
		if _, err := ht.AddTopKChecked("topk", fmt.Sprintf("value:%03d", idx), uint64(size-idx)); err != nil {
			tb.Fatalf("AddTopKChecked(%d) error = %v", idx, err)
		}
	}
	if structured {
		if _, err := ht.AddTopKChecked("topk", Map{"nested": Slice{"value"}}, 1); err != nil {
			tb.Fatalf("AddTopKChecked(structured) error = %v", err)
		}
	}
}
