package hatriecache

import (
	"reflect"
	"testing"
)

func TestReservoirSampleFastJSONMatchesGenericEncoding(t *testing.T) {
	items := []reservoirSampleItem{
		{Value: "", Priority: 1, Sequence: 2},
		{Value: "alpha / beta", Priority: ^uint64(0), Sequence: 3},
	}
	got, ok := commandFastReservoirSampleItemsJSON(items)
	if !ok {
		t.Fatal("commandFastReservoirSampleItemsJSON() ok = false, want true")
	}
	want, err := jsonEncodedString([]ReservoirSampleItem{
		{Value: "", Priority: 1, Sequence: 2},
		{Value: "alpha / beta", Priority: ^uint64(0), Sequence: 3},
	})
	if err != nil {
		t.Fatalf("jsonEncodedString() error = %v", err)
	}
	if got != want {
		t.Fatalf("fast JSON = %q, generic JSON = %q", got, want)
	}
}

func TestExecuteExactFastCommandReservoirSampleGetAliases(t *testing.T) {
	for _, command := range []string{"GETRS", "RSGET", "SAMPLE"} {
		t.Run(command, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			seedReservoirSampleReadFastPath(t, fast)
			seedReservoirSampleReadFastPath(t, generic)

			request := CacheCommandRequest{Command: command, Key: "sample", Values: Slice{"ignored"}}
			got, ok := fast.executeExactFastCommand(request)
			if !ok {
				t.Fatalf("executeExactFastCommand(%s) ok = false, want true", command)
			}
			request.Command = " " + command
			want := generic.ExecuteCommand(request)
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

func TestExecuteExactFastCommandReservoirSampleGetStatesAndOrdering(t *testing.T) {
	for _, state := range []struct {
		name  string
		setup func(*testing.T, *HatTrie)
	}{
		{name: "missing", setup: func(*testing.T, *HatTrie) {}},
		{name: "empty", setup: func(t *testing.T, ht *HatTrie) {
			if err := ht.UpsertReservoirSample("sample", 4); err != nil {
				t.Fatalf("UpsertReservoirSample() error = %v", err)
			}
		}},
		{name: "priority and sequence ties", setup: seedReservoirSampleReadFastPath},
	} {
		t.Run(state.name, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			state.setup(t, fast)
			state.setup(t, generic)
			got, ok := fast.executeExactFastCommand(CacheCommandRequest{Command: "GETRS", Key: "sample"})
			if !ok {
				t.Fatal("executeExactFastCommand(GETRS) ok = false, want true")
			}
			want := generic.ExecuteCommand(CacheCommandRequest{Command: " GETRS", Key: "sample"})
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

func TestExecuteExactFastCommandReservoirSampleGetUsesGenericEncodingForEncodedValues(t *testing.T) {
	values := []interface{}{
		`quote"value`,
		`slash\value`,
		"line\nvalue",
		"unicode-" + string(rune(0x2603)),
		"<html>&",
		Map{"nested": "value"},
	}
	for _, value := range values {
		t.Run(valueName(value), func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			if err := fast.UpsertReservoirSample("sample", 1); err != nil {
				t.Fatalf("UpsertReservoirSample() error = %v", err)
			}
			if err := generic.UpsertReservoirSample("sample", 1); err != nil {
				t.Fatalf("generic UpsertReservoirSample() error = %v", err)
			}
			if update := fast.AddReservoirSample("sample", value); !update.Accepted {
				t.Fatalf("AddReservoirSample(%#v) = %#v, want accepted", value, update)
			}
			if update := generic.AddReservoirSample("sample", value); !update.Accepted {
				t.Fatalf("generic AddReservoirSample(%#v) = %#v, want accepted", value, update)
			}
			fast.mu.Lock()
			idx := fast.peekLocked("sample").Index
			payload, direct := commandFastReservoirSampleItemsJSON(fast.reservoirSamples.array[idx].items)
			fast.mu.Unlock()
			if direct || payload != "" {
				t.Fatalf("commandFastReservoirSampleItemsJSON(%#v) = %q/%v, want generic encoding", value, payload, direct)
			}
			got, ok := fast.executeExactFastCommand(CacheCommandRequest{Command: "GETRS", Key: "sample"})
			if !ok {
				t.Fatalf("executeExactFastCommand(%#v) ok = false, want handled generic encoding", value)
			}
			want := generic.ExecuteCommand(CacheCommandRequest{Command: " GETRS", Key: "sample"})
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

func seedReservoirSampleReadFastPath(t *testing.T, ht *HatTrie) {
	t.Helper()
	if err := ht.UpsertReservoirSample("sample", 4); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	idx := ht.Get("sample").Index
	ht.reservoirSamples.array[idx].seen = 4
	ht.reservoirSamples.array[idx].items = []reservoirSampleItem{
		{Value: "fourth", Priority: 9, Sequence: 4},
		{Value: "second", Priority: 5, Sequence: 2},
		{Value: "first", Priority: 5, Sequence: 1},
		{Value: "", Priority: 1, Sequence: 3},
	}
}

func valueName(value interface{}) string {
	if text, ok := value.(string); ok {
		return text
	}
	return "structured"
}
