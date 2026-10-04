package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestCrossIndexUniqueSetPublicAPI(t *testing.T) {
	type record struct {
		Email string
		Code  string
	}
	set, err := hatDataStructure.NewCrossIndexUniqueSet([]hatDataStructure.CrossIndexUniqueConstraint[record]{
		{Name: "email", Key: func(value record) (string, bool, error) { return value.Email, value.Email != "", nil }},
		{Name: "code", Key: func(value record) (string, bool, error) { return value.Code, value.Code != "", nil }},
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(1, record{Email: "public@example.test", Code: "P1"}); err != nil {
		t.Fatal(err)
	}
	if owner, ok := set.LookupID("email", "public@example.test"); !ok || owner != 1 {
		t.Fatalf("LookupID() = %d/%v, want 1/true", owner, ok)
	}
}
