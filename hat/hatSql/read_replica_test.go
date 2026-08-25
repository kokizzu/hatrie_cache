package hatSql

import (
	"context"
	"testing"
)

func TestReadReplicaSetRoundRobinsResolvers(t *testing.T) {
	first := SourceResolverFunc(func(_, _ string) ([]Row, error) { return []Row{{"name": "first"}}, nil })
	second := SourceResolverFunc(func(_, _ string) ([]Row, error) { return []Row{{"name": "second"}}, nil })
	set, err := NewReadReplicaSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"first", "second", "first"} {
		result, err := set.Execute(context.Background(), "FROM CACHE('people') SELECT name", nil, QueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := result.Rows[0]["name"]; got != want {
			t.Fatalf("replica result = %q, want %q", got, want)
		}
	}
}
