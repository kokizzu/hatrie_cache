package hatriecache

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
	for _, token := range []string{"POST /api/commands", "Request fields", "Response fields", "Input", "Output"} {
		if !strings.Contains(doc, token) {
			t.Fatalf("DATA_STRUCTURE.md missing %q", token)
		}
	}
}

func TestDSSplitProposalDocumentsCurrentDesignAndDecision(t *testing.T) {
	data, err := os.ReadFile("DS_SPLIT_proposal.md")
	if err != nil {
		t.Fatalf("ReadFile(DS_SPLIT_proposal.md) error = %v", err)
	}
	doc := string(data)
	for _, token := range []string{
		"one shared C HAT-trie", "typed backing pools", "Recommendation", "Do not split",
		"cross-type", "backup", "replication", "prefix", "lock",
	} {
		if !strings.Contains(doc, token) {
			t.Fatalf("DS_SPLIT_proposal.md missing %q", token)
		}
	}
}
