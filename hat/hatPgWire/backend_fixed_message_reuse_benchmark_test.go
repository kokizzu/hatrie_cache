package hatPgWire

import "testing"

func BenchmarkWriteAuthenticationOKAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeAuthenticationOK(connection); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteAuthenticationOKInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := connection.writeAuthenticationOK(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReadyForQueryAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeReadyForQuery(connection); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReadyForQueryInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := connection.writeReadyForQuery(); err != nil {
			b.Fatal(err)
		}
	}
}
