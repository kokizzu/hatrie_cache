package hatSql

import "testing"

func TestTextContainsFastPathSemantics(t *testing.T) {
	tests := []struct {
		name  string
		value string
		query string
		want  bool
	}{
		{name: "all terms", value: "The hat trie cache", query: "trie HAT", want: true},
		{name: "duplicate query terms", value: "go once", query: "go go", want: true},
		{name: "punctuation and unicode", value: "Hát-trie 東京", query: "HÁT 東京", want: true},
		{name: "missing term", value: "hat cache", query: "hat trie", want: false},
		{name: "empty query", value: "hat trie", query: "---", want: false},
		{name: "empty value", value: "", query: "hat", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := textContains(test.value, test.query); got != test.want {
				t.Fatalf("textContains(%q, %q) = %t, want %t", test.value, test.query, got, test.want)
			}
		})
	}
}
