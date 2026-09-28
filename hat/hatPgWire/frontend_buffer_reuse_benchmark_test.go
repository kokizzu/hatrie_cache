package hatPgWire

import (
	"bytes"
	"testing"
)

func pgWireReuseMessages() []byte {
	data := make([]byte, 0, 128*261)
	for index := 0; index < 128; index++ {
		data = appendFrontendTestMessage(data, 'Q', bytes.Repeat([]byte{byte(index)}, 256))
	}
	return data
}

var pgWireReuseSink int

func BenchmarkReadFrontendMessageAllocating(b *testing.B) {
	data := pgWireReuseMessages()
	connection := &frontendMessageTestConn{reader: bytes.NewReader(data)}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		connection.reader.Reset(data)
		bytesRead := 0
		for message := 0; message < 128; message++ {
			_, body, err := readFrontendMessage(connection, 1<<20)
			if err != nil {
				b.Fatal(err)
			}
			bytesRead += len(body)
		}
		pgWireReuseSink = bytesRead
	}
}

func BenchmarkReadFrontendMessageInto(b *testing.B) {
	data := pgWireReuseMessages()
	connection := &frontendMessageTestConn{reader: bytes.NewReader(data)}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		connection.reader.Reset(data)
		var buffer []byte
		bytesRead := 0
		for message := 0; message < 128; message++ {
			_, body, nextBuffer, err := readFrontendMessageInto(connection, 1<<20, buffer)
			if err != nil {
				b.Fatal(err)
			}
			buffer = nextBuffer
			bytesRead += len(body)
		}
		pgWireReuseSink = bytesRead
	}
}
