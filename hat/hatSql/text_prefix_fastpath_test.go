package hatSql

import "testing"

func TestTextContainsPrefixFastPathSemantics(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		prefix string
		want   bool
	}{
		{name: "case insensitive prefix", value: "The Hat-trie cache", prefix: "HAT", want: true},
		{name: "unicode prefix", value: "Hát-trie 東京", prefix: "HÁT", want: true},
		{name: "missing prefix", value: "hat cache", prefix: "trie", want: false},
		{name: "multi-token prefix", value: "hat trie", prefix: "hat trie", want: false},
		{name: "empty prefix", value: "hat trie", prefix: "---", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := textContainsPrefix(test.value, test.prefix); got != test.want {
				t.Fatalf("textContainsPrefix(%q, %q) = %t, want %t", test.value, test.prefix, got, test.want)
			}
		})
	}
}
