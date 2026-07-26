package hatriecache

import (
	"fmt"
	"reflect"
	"testing"
)

func TestExecuteExactFastCommandPriorityQueueGetMatchesGeneric(t *testing.T) {
	for _, command := range []string{"GETPQ", "GETPRIORITY"} {
		t.Run(command, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			seedPriorityQueueReadFastPath(t, fast, 16)
			seedPriorityQueueReadFastPath(t, generic, 16)

			request := CacheCommandRequest{Command: command, Key: "queue", Values: Slice{"ignored"}}
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

func TestExecuteExactFastCommandPriorityQueueGetStates(t *testing.T) {
	for _, state := range []struct {
		name  string
		setup func(*testing.T, *HatTrie)
	}{
		{name: "missing", setup: func(*testing.T, *HatTrie) {}},
		{name: "wrong type", setup: func(t *testing.T, ht *HatTrie) {
			ht.UpsertString("queue", "value")
		}},
		{name: "empty", setup: func(t *testing.T, ht *HatTrie) {
			ht.UpsertPriorityQueue("queue", PriorityQueue{})
		}},
		{name: "ordered strings", setup: func(t *testing.T, ht *HatTrie) {
			values := []string{"", `quote\"value`, "line\nvalue", "<html>&", "unicode-\u2603", string([]byte{'a', 0xff, 'b'}), "\u2028\u2029"}
			for idx, value := range values {
				if _, err := ht.PushPriorityQueueChecked("queue", int64(idx%3), value); err != nil {
					t.Fatalf("PushPriorityQueueChecked(%d) error = %v", idx, err)
				}
			}
		}},
		{name: "priority bounds", setup: func(t *testing.T, ht *HatTrie) {
			if _, err := ht.PushPriorityQueueChecked("queue", -1<<63, "minimum"); err != nil {
				t.Fatalf("PushPriorityQueueChecked(minimum) error = %v", err)
			}
			if _, err := ht.PushPriorityQueueChecked("queue", 1<<63-1, "maximum"); err != nil {
				t.Fatalf("PushPriorityQueueChecked(maximum) error = %v", err)
			}
		}},
	} {
		t.Run(state.name, func(t *testing.T) {
			fast := newTestTrie(t)
			generic := newTestTrie(t)
			state.setup(t, fast)
			state.setup(t, generic)
			got, ok := fast.executeExactFastCommand(CacheCommandRequest{Command: "GETPQ", Key: "queue"})
			if !ok {
				t.Fatal("executeExactFastCommand(GETPQ) ok = false, want true")
			}
			want := generic.ExecuteCommand(CacheCommandRequest{Command: " GETPQ", Key: "queue"})
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fast response = %#v, generic response = %#v", got, want)
			}
		})
	}
}

func TestExecuteExactFastCommandPriorityQueueGetFallsBackForStructuredValues(t *testing.T) {
	ht := newTestTrie(t)
	if _, err := ht.PushPriorityQueueChecked("queue", 1, Map{"nested": Slice{"value"}}); err != nil {
		t.Fatalf("PushPriorityQueueChecked() error = %v", err)
	}
	if response, ok := ht.executeExactFastCommand(CacheCommandRequest{Command: "GETPQ", Key: "queue"}); ok {
		t.Fatalf("executeExactFastCommand(GETPQ) = %#v/true, want generic fallback", response)
	}
	want := `[{"priority":1,"value":{"nested":["value"]}}]`
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETPQ", Key: "queue"}); !got.OK || got.Value != want {
		t.Fatalf("GETPQ structured response = %#v, want %q", got, want)
	}
}

func TestCommandFastPriorityQueueItemsJSONCapacityMatchesOutput(t *testing.T) {
	items := []priorityQueueItem{
		newPriorityQueueStringItem(-1<<63, 0, ""),
		newPriorityQueueStringItem(0, 1, `quote\"line\n<html>&`),
		newPriorityQueueStringItem(1<<63-1, 2, string([]byte{'a', 0xff, 'b'})),
		newPriorityQueueStringItem(1, 3, "unicode-\u2603-\u2028-\u2029"),
	}
	data := priorityQueueData{items: append([]priorityQueueItem(nil), items...)}
	for idx := len(data.items)/2 - 1; idx >= 0; idx-- {
		data.siftDown(idx)
	}
	capacity, ok := commandFastPriorityQueueItemsJSONCapacity(data.items)
	if !ok {
		t.Fatal("commandFastPriorityQueueItemsJSONCapacity() ok = false, want true")
	}
	want, err := jsonEncodedString(data.Items())
	if err != nil {
		t.Fatalf("jsonEncodedString() error = %v", err)
	}
	payload := commandFastPriorityQueueItemsJSON(data.items, capacity)
	if len(payload) != capacity {
		t.Fatalf("direct JSON length = %d, capacity = %d", len(payload), capacity)
	}
	if payload != want {
		t.Fatalf("direct JSON = %q, generic JSON = %q", payload, want)
	}
}

func BenchmarkPriorityQueueGetCommand(b *testing.B) {
	for _, size := range []int{0, 1, 16, 100} {
		b.Run(fmt.Sprintf("Items%d", size), func(b *testing.B) {
			for _, mode := range []struct {
				name    string
				command string
			}{
				{name: "Generic", command: " GETPQ"},
				{name: "Exact", command: "GETPQ"},
			} {
				b.Run(mode.name, func(b *testing.B) {
					ht := CreateHatTrie()
					defer ht.Destroy()
					seedPriorityQueueReadFastPath(b, ht, size)
					request := CacheCommandRequest{Command: mode.command, Key: "queue"}
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

type priorityQueueReadTestHelper interface {
	Helper()
	Fatalf(string, ...interface{})
}

func seedPriorityQueueReadFastPath(tb priorityQueueReadTestHelper, ht *HatTrie, size int) {
	tb.Helper()
	if size == 0 {
		ht.UpsertPriorityQueue("queue", PriorityQueue{})
		return
	}
	for idx := 0; idx < size; idx++ {
		value := fmt.Sprintf("value:%02d", idx)
		if _, err := ht.PushPriorityQueueChecked("queue", int64(idx%5), value); err != nil {
			tb.Fatalf("PushPriorityQueueChecked(%d) error = %v", idx, err)
		}
	}
}
