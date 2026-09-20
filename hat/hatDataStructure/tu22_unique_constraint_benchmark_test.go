package hatDataStructure

import "testing"

func BenchmarkTU22UniqueConstraintSetLookup(b *testing.B) {
	set := tu22BenchmarkSet(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := uint64(index%256) + 1
		if set.Contains(id) {
			tu22BenchmarkSink = id
		}
	}
}

func BenchmarkTU22UniqueConstraintSetInsert(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		set := tu22BenchmarkSet(b)
		tu22BenchmarkSink = uint64(set.Len())
	}
}

func tu22BenchmarkSet(b *testing.B) *UniqueConstraintSet[tu22User, string] {
	b.Helper()
	set, err := NewUniqueConstraintSet[tu22User, string]([]UniqueConstraint[tu22User, string]{
		{Name: "email", Key: func(user tu22User) (string, bool) { return user.Email, true }},
		{Name: "handle", Key: func(user tu22User) (string, bool) { return user.Handle, true }},
	})
	if err != nil {
		b.Fatal(err)
	}
	for id := uint64(1); id <= 256; id++ {
		if err := set.Insert(id, tu22User{Email: tu22Email(id), Handle: tu22Handle(id)}); err != nil {
			b.Fatal(err)
		}
	}
	return set
}
