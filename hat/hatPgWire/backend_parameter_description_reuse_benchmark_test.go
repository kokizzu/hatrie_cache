package hatPgWire

import "testing"

func BenchmarkWriteParameterDescriptionAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	parameterTypes := benchmarkParameterTypes()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeParameterDescription(connection, parameterTypes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteParameterDescriptionInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	parameterTypes := benchmarkParameterTypes()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := connection.writeParameterDescription(parameterTypes); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkParameterTypes() []uint32 {
	return []uint32{OIDInt8, OIDText, OIDFloat8, OIDInt4, OIDBool, OIDText}
}
