package hatSql

import (
	"strings"
	"testing"
)

func TestMarshalExplainJSONAndDOT(t *testing.T) {
	t.Parallel()
	steps := []ExplainStep{{Node: "SCAN", Detail: "VALUES"}, {Node: "FILTER", Detail: "id > 1"}}
	encoded, err := MarshalExplainJSON(steps)
	if err != nil || !strings.Contains(string(encoded), `"format":"hatrie-cache-explain/v1"`) || !strings.Contains(string(encoded), `"node":"FILTER"`) {
		t.Fatalf("MarshalExplainJSON() = %s, %v", encoded, err)
	}
	dot := ExplainDOT(steps)
	if !strings.Contains(dot, "digraph hatrie_cache_explain") || !strings.Contains(dot, `op0 -> op1`) || !strings.Contains(dot, `FILTER\\nid > 1`) {
		t.Fatalf("ExplainDOT() = %q", dot)
	}
}
