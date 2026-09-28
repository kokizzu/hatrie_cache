package hatPgWire

import "testing"

func BenchmarkWriteRowDescriptionAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	fields := benchmarkRowDescriptionFields()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeRowDescription(connection, fields); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteRowDescriptionInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	fields := benchmarkRowDescriptionFields()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeRowDescription(connection, fields); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRowDescriptionFields() []Field {
	return []Field{
		{Name: "id", DataTypeOID: OIDInt8},
		{Name: "name", DataTypeOID: OIDText},
		{Name: "created_at", DataTypeOID: OIDText},
	}
}
