package hatCache

import (
	"os"
	"strings"
	"testing"
)

func TestDataStructureDocumentationCoversValueFamiliesAndCommands(t *testing.T) {
	data, err := os.ReadFile("DATA_STRUCTURE.md")
	if err != nil {
		t.Fatalf("ReadFile(DATA_STRUCTURE.md) error = %v", err)
	}
	doc := string(data)
	for _, family := range []string{
		"Counter", "Bytes", "String", "Map", "Slice/deque", "Set", "Priority queue",
		"Bloom filter", "Count-Min Sketch", "HyperLogLog", "Top-K", "Cuckoo filter",
		"Roaring bitmap", "Quantile sketch", "Fenwick tree", "Sparse uint64 bitset",
		"Reservoir sample", "XOR filter", "Radix-tree prefix index",
	} {
		if !strings.Contains(doc, family) {
			t.Fatalf("DATA_STRUCTURE.md does not document value family %q", family)
		}
	}
	for _, group := range executeCommandCases(t) {
		canonical := group[0]
		if !strings.Contains(doc, "`"+canonical+"`") {
			t.Fatalf("DATA_STRUCTURE.md does not document canonical command %s", canonical)
		}
	}
	for _, token := range []string{"POST /api/commands", "Request fields", "Response fields", "Input", "Output", "`PERSIST`"} {
		if !strings.Contains(doc, token) {
			t.Fatalf("DATA_STRUCTURE.md missing %q", token)
		}
	}
}

func TestDataStructureDocumentationHasStateTransitionForEveryCommand(t *testing.T) {
	data, err := os.ReadFile("DATA_STRUCTURE.md")
	if err != nil {
		t.Fatalf("ReadFile(DATA_STRUCTURE.md) error = %v", err)
	}
	doc := string(data)
	start := strings.Index(doc, "## Command-by-command state transitions")
	if start < 0 {
		t.Fatal("DATA_STRUCTURE.md missing command state-transition section")
	}
	transitions := doc[start:]
	for _, token := range []string{"Before state", "Request", "Reply", "After state", "TestDataStructureGuideExamples"} {
		if !strings.Contains(transitions, token) {
			t.Fatalf("DATA_STRUCTURE.md state-transition section missing %q", token)
		}
	}
	for _, group := range executeCommandCases(t) {
		canonical := group[0]
		if !strings.Contains(transitions, "| `"+canonical+"`") {
			t.Fatalf("DATA_STRUCTURE.md has no state transition for %s", canonical)
		}
	}
}

func TestDataStructureDocumentationListsEveryAcceptedCommandName(t *testing.T) {
	data, err := os.ReadFile("DATA_STRUCTURE.md")
	if err != nil {
		t.Fatalf("ReadFile(DATA_STRUCTURE.md) error = %v", err)
	}
	doc := string(data)
	for _, group := range executeCommandCases(t) {
		for _, command := range group {
			if !strings.Contains(doc, "`"+command+"`") {
				t.Fatalf("DATA_STRUCTURE.md does not document accepted command %s", command)
			}
		}
	}
}

func TestDSSplitProposalDocumentsCurrentDesignAndDecision(t *testing.T) {
	data, err := os.ReadFile("DS_SPLIT_PROPOSAL.md")
	if err != nil {
		t.Fatalf("ReadFile(DS_SPLIT_PROPOSAL.md) error = %v", err)
	}
	doc := string(data)
	for _, token := range []string{
		"one shared C HAT-trie", "typed backing pools", "Recommendation", "Do not split",
		"cross-type", "backup", "replication", "prefix", "lock",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("DS_SPLIT_PROPOSAL.md missing %q", token)
		}
	}
}

func TestIndexProposalDocumentsCurrentAndProposedIndexes(t *testing.T) {
	data, err := os.ReadFile("INDEX_PROPOSAL.md")
	if err != nil {
		t.Fatalf("ReadFile(INDEX_PROPOSAL.md) error = %v", err)
	}
	doc := string(data)
	for _, token := range []string{
		"CreateSQLJSONFieldIndex", "full rebuild", "String equality", "HAT-trie",
		"Integer", "date", "datetime", "sorted vector", "posting list",
		"Recommendation", "benchmark", "not implemented",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("INDEX_PROPOSAL.md missing %q", token)
		}
	}
}
