package hatriecache

import (
	"fmt"
	"reflect"
	"testing"
)

func TestExecuteExactFastCommandGenericGetReservoirSampleMatchesGeneric(t *testing.T) {
	for _, fixture := range []struct {
		name       string
		structured bool
		escaped    bool
	}{
		{name: "strings"},
		{name: "escaped", escaped: true},
		{name: "structured", structured: true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			seedReservoirSampleGenericGet(t, fast, 16, fixture.structured, fixture.escaped)
			seedReservoirSampleGenericGet(t, generic, 16, fixture.structured, fixture.escaped)

			got, ok := fast.executeExactFastCommand(CacheCommandRequest{Command: "GET", Key: "sample"})
			if !ok {
				t.Fatal("executeExactFastCommand(GET reservoir sample) ok = false, want true")
			}
			want := generic.ExecuteCommand(CacheCommandRequest{Command: " GET", Key: "sample"})
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

func BenchmarkReservoirSampleGenericGetCommand(b *testing.B) {
	for _, size := range []int{16, int(DefaultReservoirSampleCapacity)} {
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
						seedReservoirSampleGenericGet(b, ht, size, structured, false)
						request := CacheCommandRequest{Command: mode.command, Key: "sample"}
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

type reservoirSampleGenericGetTestHelper interface {
	Helper()
	Fatalf(string, ...interface{})
}

func seedReservoirSampleGenericGet(tb reservoirSampleGenericGetTestHelper, ht *HatTrie, size int, structured bool, escaped bool) {
	tb.Helper()
	if err := ht.UpsertReservoirSample("sample", uint64(size)); err != nil {
		tb.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	stringItems := size
	if structured {
		stringItems--
	}
	for idx := 0; idx < stringItems; idx++ {
		value := fmt.Sprintf("value:%03d", idx)
		if escaped {
			value = fmt.Sprintf("value:\"%03d\\\n", idx)
		}
		if update, err := ht.AddReservoirSampleChecked("sample", value); err != nil || !update.Accepted {
			tb.Fatalf("AddReservoirSampleChecked(%d) = %#v/%v, want accepted", idx, update, err)
		}
	}
	if structured {
		if update, err := ht.AddReservoirSampleChecked("sample", Map{"nested": Slice{"value"}}); err != nil || !update.Accepted {
			tb.Fatalf("AddReservoirSampleChecked(structured) = %#v/%v, want accepted", update, err)
		}
	}
}
