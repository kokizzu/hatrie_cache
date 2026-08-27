package hatCache

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

const (
	commandConcurrencyWorkers = 12
	commandConcurrencyRounds  = 16
)

func requireCommandOK(t *testing.T, ht *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	t.Helper()
	response := ht.ExecuteCommand(request)
	if !response.OK {
		t.Fatalf("%s response = %#v, want OK", request.Command, response)
	}
	return response
}

func runConcurrentCommands(t *testing.T, workers int, action func(worker int) error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			<-start
			if err := action(worker); err != nil {
				errs <- err
			}
		}(worker)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func commandMustSucceed(ht *HatTrie, request CacheCommandRequest) error {
	response := ht.ExecuteCommand(request)
	if !response.OK {
		return fmt.Errorf("%s(%q): %#v", request.Command, request.Key, response)
	}
	return nil
}

func TestExecuteCommandConcurrentScalarAndBatchIntegrity(t *testing.T) {
	for _, striped := range []bool{false, true} {
		t.Run(strconv.FormatBool(striped), func(t *testing.T) {
			ht := newTestTrie(t)
			if striped {
				if err := ht.ConfigureCounterWriteStripes(16); err != nil {
					t.Fatalf("ConfigureCounterWriteStripes() error = %v", err)
				}
			}
			requireCommandOK(t, ht, CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "0"})

			runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
				for round := 0; round < commandConcurrencyRounds; round++ {
					for _, request := range []CacheCommandRequest{
						{Command: "INC", Key: "counter"},
						{Command: "GET", Key: "counter"},
						{Command: "GETSTR", Key: "counter"},
						{Command: "DUMP", Key: "counter"},
						{Command: "EXISTS", Key: "counter"},
					} {
						if err := commandMustSucceed(ht, request); err != nil {
							return err
						}
					}
				}
				return nil
			})

			want := int32(commandConcurrencyWorkers * commandConcurrencyRounds)
			if got := ht.GetCounter("counter"); got != want {
				t.Fatalf("counter after concurrent INC = %d, want %d", got, want)
			}
			if striped && ht.CounterWriteStripingStats().FastPathWrites == 0 {
				t.Fatal("striped concurrent INC did not use the configured fast path")
			}
		})
	}

	t.Run("independent scalar keys and batches", func(t *testing.T) {
		ht := newTestTrie(t)
		ttl := int64(300)
		expiresAt := int64(4_102_444_800)
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			key := "scalar-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := strconv.Itoa(round)
				requests := []CacheCommandRequest{
					{Command: "SET", Key: key, Value: value},
					{Command: "SETSTR", Key: key, Value: value},
					{Command: "SETX", Key: key, Value: value, TTLSeconds: &ttl},
					{Command: "SETSTRX", Key: key, Value: value, TTLSeconds: &ttl},
					{Command: "SETINT", Key: key, Value: value},
					{Command: "SETINTX", Key: key, Value: value, TTLSeconds: &ttl},
					{Command: "EXPIRE", Key: key, TTLSeconds: &ttl},
					{Command: "EXPIREAT", Key: key, UnixSeconds: &expiresAt},
					{Command: "TTL", Key: key},
					{Command: "PERSIST", Key: key},
					{Command: "BATCH", Key: key, Batch: []CacheCommandRequest{
						{Command: "SET", Key: key, Value: value},
						{Command: "GET", Key: key},
					}},
					{Command: "BATCH", Atomic: true, Key: key, Batch: []CacheCommandRequest{
						{Command: "SETINT", Key: key, Value: value},
						{Command: "GET", Key: key},
					}},
					{Command: "DEL", Key: key},
				}
				for _, request := range requests {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			if ht.Exists("scalar-" + strconv.Itoa(worker)) {
				t.Fatalf("deleted scalar key %d remains present", worker)
			}
		}
	})
}

func TestExecuteCommandConcurrentCollectionIntegrity(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		ht := newTestTrie(t)
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			field := "field-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := strconv.Itoa(round)
				for _, request := range []CacheCommandRequest{
					{Command: "PUTMAP", Key: "map", Subkey: field, Value: value},
					{Command: "PEEKMAP", Key: "map", Subkey: field},
					{Command: "GET", Key: "map"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			field := "field-" + strconv.Itoa(worker)
			response := requireCommandOK(t, ht, CacheCommandRequest{Command: "PEEKMAP", Key: "map", Subkey: field})
			if response.Value != strconv.Itoa(commandConcurrencyRounds-1) {
				t.Fatalf("PEEKMAP(%s) = %q, want final value", field, response.Value)
			}
			requireCommandOK(t, ht, CacheCommandRequest{Command: "PUTMAP", Key: "take-map", Subkey: field, Value: field})
		}
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			field := "field-" + strconv.Itoa(worker)
			response := ht.ExecuteCommand(CacheCommandRequest{Command: "TAKEMAP", Key: "take-map", Subkey: field})
			if !response.OK || response.Value != field {
				return fmt.Errorf("TAKEMAP(%s): %#v", field, response)
			}
			return nil
		})
	})

	t.Run("slice", func(t *testing.T) {
		ht := newTestTrie(t)
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := fmt.Sprintf("%d-%d", worker, round)
				for _, request := range []CacheCommandRequest{
					{Command: "PUSHSLICE", Key: "slice", Value: value},
					{Command: "HEADSLICE", Key: "slice"},
					{Command: "TAILSLICE", Key: "slice"},
					{Command: "GET", Key: "slice"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		want := commandConcurrencyWorkers * commandConcurrencyRounds
		if values := ht.GetSlice("slice"); len(values) != want {
			t.Fatalf("slice length = %d, want %d", len(values), want)
		}
		values := make(chan string, want)
		runConcurrentCommands(t, want, func(worker int) error {
			command := "POPSLICE"
			if worker%2 == 1 {
				command = "SHIFTSLICE"
			}
			response := ht.ExecuteCommand(CacheCommandRequest{Command: command, Key: "slice"})
			if !response.OK || response.Value == "" {
				return fmt.Errorf("%s: %#v", command, response)
			}
			values <- response.Value
			return nil
		})
		close(values)
		seen := make(map[string]struct{}, want)
		for value := range values {
			if _, exists := seen[value]; exists {
				t.Fatalf("concurrent slice removal returned duplicate %q", value)
			}
			seen[value] = struct{}{}
		}
		if len(seen) != want || len(ht.GetSlice("slice")) != 0 {
			t.Fatalf("concurrent slice removal retained %d unique values and %d values in storage, want %d and 0", len(seen), len(ht.GetSlice("slice")), want)
		}
	})

	t.Run("set", func(t *testing.T) {
		ht := newTestTrie(t)
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			value := "member-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "ADDSET", Key: "set", Value: value},
					{Command: "HASSET", Key: "set", Value: value},
					{Command: "GETSET", Key: "set"},
					{Command: "GET", Key: "set"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			value := "member-" + strconv.Itoa(worker)
			if !ht.HasSet("set", value) {
				t.Fatalf("set lost %q", value)
			}
		}
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			return commandMustSucceed(ht, CacheCommandRequest{Command: "REMSET", Key: "set", Value: "member-" + strconv.Itoa(worker)})
		})
		if values := ht.GetSet("set"); len(values) != 0 {
			t.Fatalf("set retains %d values after concurrent removal", len(values))
		}
	})

	t.Run("priority queue", func(t *testing.T) {
		ht := newTestTrie(t)
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := fmt.Sprintf("%d-%d", worker, round)
				priority := int64(worker*commandConcurrencyRounds + round)
				for _, request := range []CacheCommandRequest{
					{Command: "PUSHPQ", Key: "pq", Value: value, Priority: &priority},
					{Command: "PEEKPQ", Key: "pq"},
					{Command: "GETPQ", Key: "pq"},
					{Command: "GET", Key: "pq"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		want := commandConcurrencyWorkers * commandConcurrencyRounds
		values := make(chan string, want)
		runConcurrentCommands(t, want, func(int) error {
			response := ht.ExecuteCommand(CacheCommandRequest{Command: "POPPQ", Key: "pq"})
			if !response.OK || response.Value == "" {
				return fmt.Errorf("POPPQ: %#v", response)
			}
			values <- response.Value
			return nil
		})
		close(values)
		seen := make(map[string]struct{}, want)
		for value := range values {
			if _, exists := seen[value]; exists {
				t.Fatalf("concurrent priority-queue removal returned duplicate %q", value)
			}
			seen[value] = struct{}{}
		}
		if len(seen) != want {
			t.Fatalf("concurrent priority-queue removal returned %d values, want %d", len(seen), want)
		}
	})
}

func TestExecuteCommandConcurrentProbabilisticAndSketchIntegrity(t *testing.T) {
	t.Run("bloom filter", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATEBF", Key: "bloom", Value: "1024", Subkey: "0.0001"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			value := "item-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "ADDBF", Key: "bloom", Value: value},
					{Command: "HASBF", Key: "bloom", Value: value},
					{Command: "INFOBF", Key: "bloom"},
					{Command: "GET", Key: "bloom"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			response := requireCommandOK(t, ht, CacheCommandRequest{Command: "HASBF", Key: "bloom", Value: "item-" + strconv.Itoa(worker)})
			if response.Value != "1" {
				t.Fatalf("HASBF lost item %d: %#v", worker, response)
			}
		}
	})

	t.Run("cuckoo filter", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATECF", Key: "cuckoo", Value: "1024", Subkey: "0.001"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			value := "item-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "ADDCF", Key: "cuckoo", Value: value},
					{Command: "HASCF", Key: "cuckoo", Value: value},
					{Command: "INFOCF", Key: "cuckoo"},
					{Command: "GET", Key: "cuckoo"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			response := requireCommandOK(t, ht, CacheCommandRequest{Command: "HASCF", Key: "cuckoo", Value: "item-" + strconv.Itoa(worker)})
			if response.Value != "1" {
				t.Fatalf("HASCF lost item %d: %#v", worker, response)
			}
		}
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			return commandMustSucceed(ht, CacheCommandRequest{Command: "DELCF", Key: "cuckoo", Value: "item-" + strconv.Itoa(worker)})
		})
		if info, ok := ht.CuckooFilterInfo("cuckoo"); !ok || info.Count != 0 {
			t.Fatalf("CuckooFilterInfo after concurrent DELCF = %#v/%v, want count 0", info, ok)
		}
	})

	t.Run("xor filter", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATEXF", Key: "xor", Value: "256"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			return commandMustSucceed(ht, CacheCommandRequest{Command: "ADDXF", Key: "xor", Value: "item-" + strconv.Itoa(worker)})
		})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			if worker == 0 {
				return commandMustSucceed(ht, CacheCommandRequest{Command: "BUILDXF", Key: "xor"})
			}
			return commandMustSucceed(ht, CacheCommandRequest{Command: "INFOXF", Key: "xor"})
		})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			value := "item-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "HASXF", Key: "xor", Value: value},
					{Command: "INFOXF", Key: "xor"},
					{Command: "GET", Key: "xor"},
				} {
					response := ht.ExecuteCommand(request)
					if !response.OK {
						return fmt.Errorf("%s(%q): %#v", request.Command, request.Key, response)
					}
					if request.Command == "HASXF" && response.Value != "1" {
						return fmt.Errorf("HASXF lost %q", value)
					}
				}
			}
			return nil
		})
	})

	t.Run("count-min sketch", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATECMS", Key: "cms", Value: "2048", Subkey: "4"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			value := "item-" + strconv.Itoa(worker)
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "INCRCMS", Key: "cms", Value: value, Subkey: "1"},
					{Command: "ESTCMS", Key: "cms", Value: value},
					{Command: "INFOCMS", Key: "cms"},
					{Command: "GET", Key: "cms"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if info, ok := ht.CountMinSketchInfo("cms"); !ok || info.TotalCount != uint64(commandConcurrencyWorkers*commandConcurrencyRounds) {
			t.Fatalf("CountMinSketchInfo = %#v/%v, want exact total", info, ok)
		}
	})

	t.Run("hyperloglog", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATEHLL", Key: "hll", Value: "12"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := fmt.Sprintf("%d-%d", worker, round)
				for _, request := range []CacheCommandRequest{
					{Command: "ADDHLL", Key: "hll", Value: value},
					{Command: "COUNTHLL", Key: "hll"},
					{Command: "INFOHLL", Key: "hll"},
					{Command: "GET", Key: "hll"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if info, ok := ht.HyperLogLogInfo("hll"); !ok || info.Observations != uint64(commandConcurrencyWorkers*commandConcurrencyRounds) {
			t.Fatalf("HyperLogLogInfo = %#v/%v, want exact observations", info, ok)
		}
	})

	t.Run("top-k", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "8"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				for _, request := range []CacheCommandRequest{
					{Command: "ADDTOPK", Key: "top", Value: "dominant", Subkey: "1"},
					{Command: "ESTTOPK", Key: "top", Value: "dominant"},
					{Command: "GETTOPK", Key: "top"},
					{Command: "INFOTOPK", Key: "top"},
					{Command: "GET", Key: "top"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if info, ok := ht.TopKInfo("top"); !ok || info.Total != uint64(commandConcurrencyWorkers*commandConcurrencyRounds) {
			t.Fatalf("TopKInfo = %#v/%v, want exact total", info, ok)
		}
	})

	t.Run("reservoir sample", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "32"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := fmt.Sprintf("%d-%d", worker, round)
				for _, request := range []CacheCommandRequest{
					{Command: "ADDRS", Key: "sample", Value: value},
					{Command: "GETRS", Key: "sample"},
					{Command: "INFORS", Key: "sample"},
					{Command: "GET", Key: "sample"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if info, ok := ht.ReservoirSampleInfo("sample"); !ok || info.Seen != uint64(commandConcurrencyWorkers*commandConcurrencyRounds) || info.Tracked != 32 {
			t.Fatalf("ReservoirSampleInfo = %#v/%v, want exact seen and bounded sample", info, ok)
		}
	})

	t.Run("quantile sketch", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATEQ", Key: "quantile", Value: "0.01"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				value := strconv.Itoa(worker*commandConcurrencyRounds + round)
				for _, request := range []CacheCommandRequest{
					{Command: "ADDQ", Key: "quantile", Value: value},
					{Command: "ESTQ", Key: "quantile", Value: "0.5"},
					{Command: "INFOQ", Key: "quantile"},
					{Command: "GET", Key: "quantile"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if info, ok := ht.QuantileSketchInfo("quantile"); !ok || info.Count != uint64(commandConcurrencyWorkers*commandConcurrencyRounds) {
			t.Fatalf("QuantileSketchInfo = %#v/%v, want exact count", info, ok)
		}
	})

	t.Run("fenwick tree", func(t *testing.T) {
		ht := newTestTrie(t)
		entries := commandConcurrencyWorkers * commandConcurrencyRounds
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATEFW", Key: "fenwick", Value: strconv.Itoa(entries + 1)})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				index := strconv.Itoa(worker*commandConcurrencyRounds + round + 1)
				for _, request := range []CacheCommandRequest{
					{Command: "ADDFW", Key: "fenwick", Value: index, Subkey: "1"},
					{Command: "GETFW", Key: "fenwick", Value: index},
					{Command: "SUMFW", Key: "fenwick", Value: index},
					{Command: "RANGEFW", Key: "fenwick", Value: index, Subkey: index},
					{Command: "INFOFW", Key: "fenwick"},
					{Command: "GET", Key: "fenwick"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		response := requireCommandOK(t, ht, CacheCommandRequest{Command: "SUMFW", Key: "fenwick", Value: strconv.Itoa(entries)})
		if response.Value != strconv.Itoa(entries) {
			t.Fatalf("SUMFW final = %q, want %d", response.Value, entries)
		}
	})
}

func TestExecuteCommandConcurrentBitmapAndRadixIntegrity(t *testing.T) {
	for _, test := range []struct {
		name   string
		create string
		add    string
		remove string
		has    string
		count  string
		get    string
		info   string
	}{
		{name: "roaring bitmap", create: "CREATERB", add: "ADDRB", remove: "REMRB", has: "HASRB", count: "COUNTRB", get: "GETRB", info: "INFORB"},
		{name: "sparse bitset", create: "CREATESB", add: "ADDSB", remove: "REMSB", has: "HASSB", count: "COUNTSB", get: "GETSB", info: "INFOSB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ht := newTestTrie(t)
			requireCommandOK(t, ht, CacheCommandRequest{Command: test.create, Key: "bits"})
			runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
				for round := 0; round < commandConcurrencyRounds; round++ {
					value := strconv.Itoa(worker*commandConcurrencyRounds + round)
					for _, request := range []CacheCommandRequest{
						{Command: test.add, Key: "bits", Value: value},
						{Command: test.has, Key: "bits", Value: value},
						{Command: test.count, Key: "bits"},
						{Command: test.get, Key: "bits"},
						{Command: test.info, Key: "bits"},
						{Command: "GET", Key: "bits"},
					} {
						if err := commandMustSucceed(ht, request); err != nil {
							return err
						}
					}
				}
				return nil
			})
			want := strconv.Itoa(commandConcurrencyWorkers * commandConcurrencyRounds)
			if response := requireCommandOK(t, ht, CacheCommandRequest{Command: test.count, Key: "bits"}); response.Value != want {
				t.Fatalf("%s final count = %q, want %s", test.count, response.Value, want)
			}
			runConcurrentCommands(t, commandConcurrencyWorkers*commandConcurrencyRounds, func(index int) error {
				return commandMustSucceed(ht, CacheCommandRequest{Command: test.remove, Key: "bits", Value: strconv.Itoa(index)})
			})
			if response := requireCommandOK(t, ht, CacheCommandRequest{Command: test.count, Key: "bits"}); response.Value != "0" {
				t.Fatalf("%s final count = %q, want 0", test.count, response.Value)
			}
		})
	}

	t.Run("radix tree", func(t *testing.T) {
		ht := newTestTrie(t)
		requireCommandOK(t, ht, CacheCommandRequest{Command: "CREATERT", Key: "radix"})
		runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
			for round := 0; round < commandConcurrencyRounds; round++ {
				subkey := fmt.Sprintf("worker:%02d:%02d", worker, round)
				for _, request := range []CacheCommandRequest{
					{Command: "PUTRT", Key: "radix", Subkey: subkey, Value: subkey},
					{Command: "GETRT", Key: "radix", Subkey: subkey},
					{Command: "HASRT", Key: "radix", Subkey: subkey},
					{Command: "PREFIXRT", Key: "radix", Subkey: "worker:"},
					{Command: "INFORT", Key: "radix"},
					{Command: "GET", Key: "radix"},
				} {
					if err := commandMustSucceed(ht, request); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for worker := 0; worker < commandConcurrencyWorkers; worker++ {
			for round := 0; round < commandConcurrencyRounds; round++ {
				subkey := fmt.Sprintf("worker:%02d:%02d", worker, round)
				response := requireCommandOK(t, ht, CacheCommandRequest{Command: "GETRT", Key: "radix", Subkey: subkey})
				if response.Value != subkey {
					t.Fatalf("GETRT(%q) = %q, want stored value", subkey, response.Value)
				}
			}
		}
		runConcurrentCommands(t, commandConcurrencyWorkers*commandConcurrencyRounds, func(index int) error {
			worker := index / commandConcurrencyRounds
			round := index % commandConcurrencyRounds
			subkey := fmt.Sprintf("worker:%02d:%02d", worker, round)
			return commandMustSucceed(ht, CacheCommandRequest{Command: "DELRT", Key: "radix", Subkey: subkey})
		})
		response := requireCommandOK(t, ht, CacheCommandRequest{Command: "INFORT", Key: "radix"})
		if response.Value == "" {
			t.Fatal("INFORT returned an empty response after concurrent deletion")
		}
	})
}

func TestExecuteCommandConcurrentStructureCreationIntegrity(t *testing.T) {
	createRequests := []CacheCommandRequest{
		{Command: "CREATEBF", Value: "64", Subkey: "0.01"},
		{Command: "CREATECF", Value: "64", Subkey: "0.01"},
		{Command: "CREATEXF", Value: "16"},
		{Command: "CREATERB"},
		{Command: "CREATESB"},
		{Command: "CREATERT"},
		{Command: "CREATECMS", Value: "32", Subkey: "3"},
		{Command: "CREATEHLL", Value: "8"},
		{Command: "CREATETOPK", Value: "4"},
		{Command: "CREATERS", Value: "4"},
		{Command: "CREATEQ", Value: "0.01"},
		{Command: "CREATEFW", Value: "8"},
	}
	ht := newTestTrie(t)
	runConcurrentCommands(t, commandConcurrencyWorkers, func(worker int) error {
		for _, template := range createRequests {
			request := template
			request.Key = fmt.Sprintf("%s-%d", template.Command, worker)
			if err := commandMustSucceed(ht, request); err != nil {
				return err
			}
			if err := commandMustSucceed(ht, CacheCommandRequest{Command: "GET", Key: request.Key}); err != nil {
				return err
			}
		}
		return nil
	})
	if got, want := ht.Size(), len(createRequests)*commandConcurrencyWorkers; got != want {
		t.Fatalf("trie size after concurrent creates = %d, want %d", got, want)
	}
}

func TestExecuteCommandConcurrentInternalReplicationIntegrity(t *testing.T) {
	entries := commandConcurrencyWorkers * commandConcurrencyRounds
	source := newTestTrie(t)
	payloads := make([]string, entries)
	for index := 0; index < entries; index++ {
		key := "internal-" + strconv.Itoa(index)
		value := "value-" + strconv.Itoa(index)
		requireCommandOK(t, source, CacheCommandRequest{Command: "SET", Key: key, Value: value})
		payloads[index] = requireCommandOK(t, source, CacheCommandRequest{Command: "DUMP", Key: key}).Value
		if payloads[index] == "" {
			t.Fatalf("DUMP(%q) returned an empty snapshot", key)
		}
	}

	target := newTestTrie(t)
	runConcurrentCommands(t, entries, func(index int) error {
		key := "internal-" + strconv.Itoa(index)
		if err := commandMustSucceed(target, CacheCommandRequest{Command: "INTERNALSET", Key: key, Value: payloads[index]}); err != nil {
			return err
		}
		response := target.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: key})
		if !response.OK || response.Value != "value-"+strconv.Itoa(index) {
			return fmt.Errorf("GET(%q) after INTERNALSET: %#v", key, response)
		}
		return commandMustSucceed(target, CacheCommandRequest{Command: "DUMP", Key: key})
	})
	for index := 0; index < entries; index++ {
		key := "internal-" + strconv.Itoa(index)
		if value := target.GetString(key); value != "value-"+strconv.Itoa(index) {
			t.Fatalf("INTERNALSET lost %q: got %q", key, value)
		}
	}
	runConcurrentCommands(t, entries, func(index int) error {
		return commandMustSucceed(target, CacheCommandRequest{Command: "INTERNALDEL", Key: "internal-" + strconv.Itoa(index)})
	})
	if size := target.Size(); size != 0 {
		t.Fatalf("concurrent INTERNALDEL left %d entries", size)
	}
}
