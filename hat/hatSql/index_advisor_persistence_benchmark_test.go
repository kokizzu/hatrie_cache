package hatSql

import (
	"bytes"
	"strconv"
	"testing"
)

var sqlIndexAdvisorPersistenceBenchmarkBytes []byte

func BenchmarkSQLIndexAdvisorPersistence(b *testing.B) {
	advisor := NewSQLIndexAdvisor(128)
	for index := 0; index < 64; index++ {
		advisor.counts[sqlIndexAdvisorKey{key: "table_" + strconv.Itoa(index), field: "field_" + strconv.Itoa(index%8)}] = uint64(index + 1)
	}
	var encoded bytes.Buffer
	if err := advisor.Save(&encoded); err != nil {
		b.Fatal(err)
	}
	payload := append([]byte(nil), encoded.Bytes()...)

	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoded.Reset()
			if err := advisor.Save(&encoded); err != nil {
				b.Fatal(err)
			}
			sqlIndexAdvisorPersistenceBenchmarkBytes = encoded.Bytes()
		}
	})
	b.Run("load", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := advisor.Load(bytes.NewReader(payload)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
