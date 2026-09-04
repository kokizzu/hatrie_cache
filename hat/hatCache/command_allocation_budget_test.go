package hatCache

import "testing"

type commandAllocationBudget struct {
	name      string
	request   CacheCommandRequest
	setup     func(*HatTrie)
	maxAllocs float64
}

func commandAllocationBudgets() []commandAllocationBudget {
	ttlSeconds := int64(3600)
	return []commandAllocationBudget{
		{
			name:      "GET",
			request:   CacheCommandRequest{Command: "GET", Key: "alloc:get"},
			setup:     func(trie *HatTrie) { trie.UpsertString("alloc:get", "value") },
			maxAllocs: 0,
		},
		{
			name:      "EXISTS",
			request:   CacheCommandRequest{Command: "EXISTS", Key: "alloc:exists"},
			setup:     func(trie *HatTrie) { trie.UpsertString("alloc:exists", "value") },
			maxAllocs: 0,
		},
		{
			name:      "EXPIRE",
			request:   CacheCommandRequest{Command: "EXPIRE", Key: "alloc:expire", TTLSeconds: &ttlSeconds},
			setup:     func(trie *HatTrie) { trie.UpsertString("alloc:expire", "value") },
			maxAllocs: 0,
		},
		{
			name:      "SETSTR",
			request:   CacheCommandRequest{Command: "SETSTR", Key: "alloc:set", Value: "value"},
			maxAllocs: 0,
		},
		{
			name:    "ADDSET",
			request: CacheCommandRequest{Command: "ADDSET", Key: "alloc:set-value", Value: "value"},
			setup: func(trie *HatTrie) {
				trie.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "alloc:set-value", Value: "value"})
			},
			maxAllocs: 0,
		},
		{
			name:    "HASSET",
			request: CacheCommandRequest{Command: "HASSET", Key: "alloc:has-set", Value: "value"},
			setup: func(trie *HatTrie) {
				trie.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "alloc:has-set", Value: "value"})
			},
			maxAllocs: 0,
		},
		{
			name:      "INCRCMS",
			request:   CacheCommandRequest{Command: "INCRCMS", Key: "alloc:counter", Value: "value", Subkey: "1"},
			maxAllocs: 1,
		},
	}
}

func TestCommandAllocationBudgets(t *testing.T) {
	budgets := commandAllocationBudgets()
	if len(budgets) == 0 {
		t.Fatal("commandAllocationBudgets() returned no command budgets")
	}
	for _, budget := range budgets {
		budget := budget
		t.Run(budget.name, func(t *testing.T) {
			trie := newTestTrie(t)
			if budget.setup != nil {
				budget.setup(trie)
			}
			if response := trie.ExecuteCommand(budget.request); !response.OK {
				t.Fatalf("warm-up response = %#v, want success", response)
			}
			allocs := testing.AllocsPerRun(1000, func() {
				if response := trie.ExecuteCommand(budget.request); !response.OK {
					t.Fatalf("command response = %#v, want success", response)
				}
			})
			t.Logf("allocations/op = %.2f, budget = %.2f", allocs, budget.maxAllocs)
			if allocs > budget.maxAllocs {
				t.Fatalf("allocations/op = %.2f, want <= %.2f", allocs, budget.maxAllocs)
			}
		})
	}
}

func BenchmarkCommandAllocationBudgets(b *testing.B) {
	for _, budget := range commandAllocationBudgets() {
		budget := budget
		b.Run(budget.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			if budget.setup != nil {
				budget.setup(trie)
			}
			if response := trie.ExecuteCommand(budget.request); !response.OK {
				b.Fatalf("warm-up response = %#v, want success", response)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if response := trie.ExecuteCommand(budget.request); !response.OK {
					b.Fatalf("command response = %#v, want success", response)
				}
			}
		})
	}
}
