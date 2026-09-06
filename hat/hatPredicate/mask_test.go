package hatPredicate_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPredicate"
)

func TestMatchInt64BuildsReusableSelectionMask(t *testing.T) {
	values := []int64{-2, 0, 1, 1, 4, 8, 9}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	for index := range mask {
		mask[index] = ^uint64(0)
	}
	count, err := hatPredicate.MatchInt64(mask, values, hatPredicate.Int64Equal, 1)
	if err != nil {
		t.Fatalf("MatchInt64() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("MatchInt64() count = %d, want 2", count)
	}
	if got, want := mask, []uint64{(1 << 2) | (1 << 3)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MatchInt64() mask = %#v, want %#v", got, want)
	}

	count, err = hatPredicate.MatchInt64(mask, values, hatPredicate.Int64GreaterEqual, 8)
	if err != nil {
		t.Fatalf("MatchInt64() second error = %v", err)
	}
	if count != 2 || mask[0] != (1<<5)|(1<<6) {
		t.Fatalf("MatchInt64() second result count=%d mask=%#v, want count=2 bits 5,6", count, mask)
	}
}

func TestMatchStringSupportsEqualityPrefixAndContains(t *testing.T) {
	values := []string{"alpha", "alphabet", "beta", "", "alpine"}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	for _, test := range []struct {
		name  string
		op    hatPredicate.StringPredicate
		value string
		want  uint64
		count int
	}{
		{name: "equal", op: hatPredicate.StringEqual, value: "alpha", want: 1 << 0, count: 1},
		{name: "prefix", op: hatPredicate.StringPrefix, value: "alp", want: (1 << 0) | (1 << 1) | (1 << 4), count: 3},
		{name: "contains", op: hatPredicate.StringContains, value: "bet", want: (1 << 1) | (1 << 2), count: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			count, err := hatPredicate.MatchString(mask, values, test.op, test.value)
			if err != nil {
				t.Fatalf("MatchString() error = %v", err)
			}
			if count != test.count || mask[0] != test.want {
				t.Fatalf("MatchString() count=%d mask=%#x, want count=%d mask=%#x", count, mask[0], test.count, test.want)
			}
		})
	}
}

func TestMatchPredicatesRejectInvalidMaskAndOperator(t *testing.T) {
	values := []int64{1, 2}
	if _, err := hatPredicate.MatchInt64(nil, values, hatPredicate.Int64Equal, 1); !errors.Is(err, hatPredicate.ErrMaskTooSmall) {
		t.Fatalf("short mask error = %v, want ErrMaskTooSmall", err)
	}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	if _, err := hatPredicate.MatchInt64(mask, values, hatPredicate.Int64Predicate(99), 1); !errors.Is(err, hatPredicate.ErrInvalidPredicate) {
		t.Fatalf("invalid numeric operator error = %v, want ErrInvalidPredicate", err)
	}
	if _, err := hatPredicate.MatchString(mask, []string{"a"}, hatPredicate.StringPredicate(99), "a"); !errors.Is(err, hatPredicate.ErrInvalidPredicate) {
		t.Fatalf("invalid string operator error = %v, want ErrInvalidPredicate", err)
	}
}

func BenchmarkMatchInt64(b *testing.B) {
	values := make([]int64, 100000)
	for i := range values {
		values[i] = int64(i % 17)
	}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hatPredicate.MatchInt64(mask, values, hatPredicate.Int64Equal, 7); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMatchString(b *testing.B) {
	values := make([]string, 100000)
	for i := range values {
		values[i] = "region-" + string(rune('a'+i%26))
	}
	mask := make([]uint64, hatPredicate.MaskWords(len(values)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := hatPredicate.MatchString(mask, values, hatPredicate.StringPrefix, "region-"); err != nil {
			b.Fatal(err)
		}
	}
}
