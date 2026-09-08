package hatPartition

import "testing"

func TestPrefixRouterUsesLongestPrefixAndNoImplicitDefault(t *testing.T) {
	router, err := NewPrefixRouter([]PrefixRule{
		{Prefix: "sg:", Partition: "sg"},
		{Prefix: "sg:vip:", Partition: "sg-vip"},
		{Prefix: "us:", Partition: "us"},
	})
	if err != nil {
		t.Fatalf("NewPrefixRouter() error = %v", err)
	}
	cases := []struct {
		key       string
		partition string
		ok        bool
	}{
		{key: "sg:tenant:42", partition: "sg", ok: true},
		{key: "sg:vip:tenant:42", partition: "sg-vip", ok: true},
		{key: "us:tenant:42", partition: "us", ok: true},
		{key: "eu:tenant:42", ok: false},
	}
	for _, test := range cases {
		partition, ok := router.Route(test.key)
		if partition != test.partition || ok != test.ok {
			t.Errorf("Route(%q) = %q/%t, want %q/%t", test.key, partition, ok, test.partition, test.ok)
		}
	}
}

func TestPrefixRouterNormalizesRulesAndReturnsIndependentSnapshot(t *testing.T) {
	router, err := NewPrefixRouter([]PrefixRule{{Prefix: " sg:", Partition: " sg "}})
	if err != nil {
		t.Fatalf("NewPrefixRouter() error = %v", err)
	}
	rules := router.Rules()
	if want := []PrefixRule{{Prefix: "sg:", Partition: "sg"}}; len(rules) != 1 || rules[0] != want[0] {
		t.Fatalf("Rules() = %#v, want %#v", rules, want)
	}
	rules[0].Partition = "mutated"
	if partition, ok := router.Route("sg:tenant"); partition != "sg" || !ok {
		t.Fatalf("Route() after snapshot mutation = %q/%t, want sg/true", partition, ok)
	}
}

func TestPrefixRouterRejectsInvalidRules(t *testing.T) {
	for name, rules := range map[string][]PrefixRule{
		"empty prefix":      {{Partition: "sg"}},
		"empty partition":   {{Prefix: "sg:"}},
		"duplicate prefix":  {{Prefix: "sg:", Partition: "sg"}, {Prefix: "sg:", Partition: "other"}},
		"trimmed duplicate": {{Prefix: "sg:", Partition: "sg"}, {Prefix: " sg: ", Partition: "other"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPrefixRouter(rules); err == nil {
				t.Fatal("NewPrefixRouter() error = nil, want error")
			}
		})
	}
}

func TestPrefixRouterZeroValueIsDisabled(t *testing.T) {
	var router PrefixRouter
	if partition, ok := router.Route("sg:tenant"); partition != "" || ok {
		t.Fatalf("zero Route() = %q/%t, want empty/false", partition, ok)
	}
	if rules := router.Rules(); rules != nil {
		t.Fatalf("zero Rules() = %#v, want nil", rules)
	}
}

func BenchmarkPrefixRouterRoute(b *testing.B) {
	rules := make([]PrefixRule, 16)
	for index := range rules {
		rules[index] = PrefixRule{Prefix: "region-" + string(rune('a'+index)) + ":", Partition: "region-" + string(rune('a'+index))}
	}
	router, err := NewPrefixRouter(rules)
	if err != nil {
		b.Fatal(err)
	}
	keys := []string{"region-a:tenant:1", "region-h:tenant:2", "region-p:tenant:3", "unknown:tenant:4"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, ok := router.Route(keys[index%len(keys)]); index%len(keys) != 3 && !ok || index%len(keys) == 3 && ok {
			b.Fatal("unexpected route result")
		}
	}
}
