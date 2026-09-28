package hatPgWire

import "testing"

func BenchmarkWriteParameterStatusAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeParameterStatus(connection, "server_version", "16.0"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteParameterStatusInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := connection.writeParameterStatus("server_version", "16.0"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteCommandTagAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeMessage(connection, 'C', appendCString(nil, "SELECT 1")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteCommandTagInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := connection.writeCStringMessage('C', "SELECT 1"); err != nil {
			b.Fatal(err)
		}
	}
}
