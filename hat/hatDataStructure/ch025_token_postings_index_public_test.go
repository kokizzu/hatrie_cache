package hatDataStructure_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestCH025TokenPostingsIndexPublicAPI(t *testing.T) {
	index := hatDataStructure.NewTokenPostingsIndex()
	index.Upsert(42, "public API")
	if got := index.MatchAll("PUBLIC api"); !reflect.DeepEqual(got, []uint32{42}) {
		t.Fatalf("public MatchAll = %v, want [42]", got)
	}
	if info := index.Info(); info.Rows != 1 || info.Terms != 2 {
		t.Fatalf("public Info = %+v, want one row and two terms", info)
	}
}
