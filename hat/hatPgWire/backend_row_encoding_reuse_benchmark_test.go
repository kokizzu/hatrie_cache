package hatPgWire

import "testing"

func BenchmarkWriteDataRowAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	first := "alpha"
	second := "beta"
	row := []*string{&first, nil, &second}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeDataRow(connection, row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteDataRowInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	first := "alpha"
	second := "beta"
	row := []*string{&first, nil, &second}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeDataRow(connection, row); err != nil {
			b.Fatal(err)
		}
	}
}
