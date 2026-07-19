package hatriecache

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

type benchmarkCommandExecutor func(CacheCommandRequest) CacheCommandResponse

type commandTransportBenchmark struct {
	name  string
	setup func(*testing.B, benchmarkCommandExecutor)
	run   func(*testing.B, benchmarkCommandExecutor, int)
}

func BenchmarkCommandTransportFeature(b *testing.B) {
	benchmarks := []commandTransportBenchmark{
		{name: "StringSet", run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "SETSTR", Key: "string:key", Value: "value"})
		}},
		{name: "StringGet", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "SETSTR", Key: "string:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "GET", Key: "string:key"})
		}},
		{name: "CounterInc", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "SETINT", Key: "counter:key", Value: "0"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "INC", Key: "counter:key", Value: "1"})
		}},
		{name: "MapPut", run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}},
		{name: "MapPeek", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUTMAP", Key: "map:key", Subkey: "field", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PEEKMAP", Key: "map:key", Subkey: "field"})
		}},
		{name: "SlicePushPop", run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice:key", Value: "value"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "POPSLICE", Key: "slice:key"})
		}},
		{name: "SetAddHas", run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDSET", Key: "set:key", Value: "value"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "HASSET", Key: "set:key", Value: "value"})
		}},
		{name: "PriorityQueuePushPop", run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			priority := int64(10)
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUSHPQ", Key: "priority:key", Value: "value", Priority: &priority})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "POPPQ", Key: "priority:key"})
		}},
		{name: "BloomAdd", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATEBF", Key: "bloom:key", Value: "32768", Subkey: "0.001"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDBF", Key: "bloom:key", Value: "value"})
		}},
		{name: "BloomHas", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATEBF", Key: "bloom:key", Value: "32768", Subkey: "0.001"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDBF", Key: "bloom:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "HASBF", Key: "bloom:key", Value: "value"})
		}},
		{name: "CuckooDeleteAdd", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATECF", Key: "cuckoo:key", Value: "32768", Subkey: "0.001"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "DELCF", Key: "cuckoo:key", Value: "value"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:key", Value: "value"})
		}},
		{name: "CuckooHas", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATECF", Key: "cuckoo:key", Value: "32768", Subkey: "0.001"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDCF", Key: "cuckoo:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "HASCF", Key: "cuckoo:key", Value: "value"})
		}},
		{name: "RoaringAdd", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATERB", Key: "roaring:key"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDRB", Key: "roaring:key", Value: "65543"})
		}},
		{name: "RoaringHas", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATERB", Key: "roaring:key"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDRB", Key: "roaring:key", Value: "65543"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "HASRB", Key: "roaring:key", Value: "65543"})
		}},
		{name: "SparseBitsetAdd", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATESB", Key: "sparse:key"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDSB", Key: "sparse:key", Value: "18446744073709551615"})
		}},
		{name: "SparseBitsetHas", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATESB", Key: "sparse:key"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDSB", Key: "sparse:key", Value: "18446744073709551615"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "HASSB", Key: "sparse:key", Value: "18446744073709551615"})
		}},
		{name: "RadixPut", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATERT", Key: "radix:key"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUTRT", Key: "radix:key", Subkey: "session:active", Value: "value"})
		}},
		{name: "RadixPrefix", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATERT", Key: "radix:key"})
			for i := 0; i < 16; i++ {
				idx := fmt.Sprint(i)
				benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PUTRT", Key: "radix:key", Subkey: "session:" + idx, Value: "value-" + idx})
			}
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "PREFIXRT", Key: "radix:key", Subkey: "session:"})
		}},
		{name: "CountMinSketchIncrement", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATECMS", Key: "cms:key", Value: "1024", Subkey: "4"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "INCRCMS", Key: "cms:key", Value: "value", Subkey: "1"})
		}},
		{name: "CountMinSketchEstimate", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATECMS", Key: "cms:key", Value: "1024", Subkey: "4"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "INCRCMS", Key: "cms:key", Value: "value", Subkey: "1"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ESTCMS", Key: "cms:key", Value: "value"})
		}},
		{name: "HyperLogLogAdd", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATEHLL", Key: "hll:key", Value: "10"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDHLL", Key: "hll:key", Value: "value"})
		}},
		{name: "HyperLogLogCount", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "CREATEHLL", Key: "hll:key", Value: "10"})
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "ADDHLL", Key: "hll:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "COUNTHLL", Key: "hll:key"})
		}},
		{name: "ReplicationDump", setup: func(b *testing.B, execute benchmarkCommandExecutor) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "SETSTR", Key: "replication:key", Value: "value"})
		}, run: func(b *testing.B, execute benchmarkCommandExecutor, i int) {
			benchmarkExecuteTransportCommand(b, execute, CacheCommandRequest{Command: "DUMP", Key: "replication:key"})
		}},
	}

	transports := []struct {
		name string
		new  func(*testing.B) (benchmarkCommandExecutor, func())
	}{
		{name: "InProcess", new: newInProcessBenchmarkExecutor},
		{name: "HTTPJSON", new: func(b *testing.B) (benchmarkCommandExecutor, func()) {
			return newHTTPBenchmarkExecutor(b, CommandWireFormatJSON)
		}},
		{name: "HTTPProtobuf", new: func(b *testing.B) (benchmarkCommandExecutor, func()) {
			return newHTTPBenchmarkExecutor(b, CommandWireFormatProtobuf)
		}},
		{name: "GRPC", new: newGRPCBenchmarkExecutor},
		{name: "GRPCStream", new: newGRPCStreamBenchmarkExecutor},
	}

	for _, transport := range transports {
		for _, benchmark := range benchmarks {
			b.Run(transport.name+"/"+benchmark.name, func(b *testing.B) {
				execute, stop := transport.new(b)
				defer stop()
				if benchmark.setup != nil {
					benchmark.setup(b, execute)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchmark.run(b, execute, i)
				}
			})
		}
	}
}

func benchmarkExecuteTransportCommand(b *testing.B, execute benchmarkCommandExecutor, request CacheCommandRequest) CacheCommandResponse {
	b.Helper()
	response := execute(request)
	if !response.OK {
		b.Fatalf("ExecuteCommand(%s, %s) = %#v, want ok", request.Command, request.Key, response)
	}
	benchmarkCommandResponseSink = response
	return response
}

func newInProcessBenchmarkExecutor(b *testing.B) (benchmarkCommandExecutor, func()) {
	b.Helper()
	ht := CreateHatTrie()
	return ht.ExecuteCommand, ht.Destroy
}

func newHTTPBenchmarkExecutor(b *testing.B, format CommandWireFormat) (benchmarkCommandExecutor, func()) {
	b.Helper()
	ht := CreateHatTrie()
	handler := NewMonitoringHandler(ht, MonitoringOptions{NodeName: "bench", WebDir: ""}).Handler()
	server := httptest.NewServer(handler)
	client := server.Client()
	execute := func(request CacheCommandRequest) CacheCommandResponse {
		body, contentType, contentEncoding, err := CommandRequestBody(request, format, 0, 0)
		if err != nil {
			b.Fatalf("CommandRequestBody(%s) error = %v", request.Command, err)
		}
		httpRequest, err := http.NewRequest(http.MethodPost, server.URL+"/api/commands", body)
		if err != nil {
			b.Fatalf("NewRequest(%s) error = %v", request.Command, err)
		}
		httpRequest.Header.Set("Content-Type", contentType)
		httpRequest.Header.Set("Accept", contentType)
		if contentEncoding != "" {
			httpRequest.Header.Set("Content-Encoding", contentEncoding)
		}
		response, err := client.Do(httpRequest)
		if err != nil {
			b.Fatalf("HTTP command %s error = %v", request.Command, err)
		}
		defer drainAndClose(response.Body)
		if response.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			b.Fatalf("HTTP command %s status = %s body=%q, want 200", request.Command, response.Status, string(data))
		}
		decoded, err := DecodeCommandResponseWire(response.Body, response.Header.Get("Content-Type"), maxMonitoringJSONRequestBytes)
		if err != nil {
			b.Fatalf("DecodeCommandResponseWire(%s) error = %v", request.Command, err)
		}
		return decoded
	}
	stop := func() {
		server.Close()
		ht.Destroy()
	}
	return execute, stop
}

func newGRPCBenchmarkExecutor(b *testing.B) (benchmarkCommandExecutor, func()) {
	return newGRPCBenchmarkExecutorMode(b, false)
}

func newGRPCStreamBenchmarkExecutor(b *testing.B) (benchmarkCommandExecutor, func()) {
	return newGRPCBenchmarkExecutorMode(b, true)
}

func newGRPCBenchmarkExecutorMode(b *testing.B, streaming bool) (benchmarkCommandExecutor, func()) {
	b.Helper()
	client, stopClient := newGRPCBenchmarkClient(b)
	var stream hatriecachev1.CacheService_CommandStreamClient
	var err error
	if streaming {
		stream, err = client.CommandStream(context.Background())
		if err != nil {
			b.Fatalf("CommandStream() error = %v", err)
		}
	}
	execute := func(request CacheCommandRequest) CacheCommandResponse {
		protoRequest, callErr := cacheCommandRequestToProto(request)
		if callErr != nil {
			b.Fatalf("cacheCommandRequestToProto(%s) error = %v", request.Command, callErr)
		}
		var response *hatriecachev1.CommandResponse
		if stream != nil {
			callErr = stream.Send(protoRequest)
			if callErr == nil {
				response, callErr = stream.Recv()
			}
		} else {
			response, callErr = client.Command(context.Background(), protoRequest)
		}
		if callErr != nil {
			b.Fatalf("gRPC command %s error = %v", request.Command, callErr)
		}
		return cacheCommandResponseFromProto(response)
	}
	stop := func() {
		if stream != nil {
			_ = stream.CloseSend()
		}
		stopClient()
	}
	return execute, stop
}

func newGRPCBenchmarkClient(b *testing.B, extraDialOptions ...grpc.DialOption) (hatriecachev1.CacheServiceClient, func()) {
	b.Helper()
	ht := CreateHatTrie()
	listener := bufconn.Listen(testGRPCBufferSize)
	server := grpc.NewServer()
	RegisterCacheGRPCServer(server, NewCacheGRPCServer(ht, CacheGRPCOptions{NodeName: "bench"}))
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			b.Errorf("grpc Serve() error = %v", err)
		}
	}()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	dialOptions = append(dialOptions, extraDialOptions...)
	conn, err := grpc.DialContext(context.Background(), "bufnet", dialOptions...)
	if err != nil {
		b.Fatalf("DialContext() error = %v", err)
	}
	client := hatriecachev1.NewCacheServiceClient(conn)
	stop := func() {
		if err := conn.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
		server.Stop()
		if err := listener.Close(); err != nil {
			b.Fatalf("listener Close() error = %v", err)
		}
		ht.Destroy()
	}
	return client, stop
}
