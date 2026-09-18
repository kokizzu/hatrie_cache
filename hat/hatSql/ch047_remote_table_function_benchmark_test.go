package hatSql_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var ch047RemoteTableFunctionBenchmarkSink []hatSql.SQLRow

func BenchmarkCH047RemoteTableFunctionSelectiveRange(b *testing.B) {
	full, prefix := ch047RemoteTableFunctionBenchmarkCSV()
	for _, test := range []struct {
		name      string
		body      []byte
		arguments []interface{}
		wireBytes int
	}{
		{name: "full_object", body: full, arguments: []interface{}{"https://example.test/data.csv", "csv"}, wireBytes: len(full)},
		{name: "bounded_range", body: full, arguments: []interface{}{"https://example.test/data.csv", "csv", int64(0), int64(len(prefix))}, wireBytes: len(prefix)},
	} {
		b.Run(test.name, func(b *testing.B) {
			resolver, err := hatSql.NewRemoteTableFunctionResolver(ch047RemoteTableFunctionBenchmarkBase{}, hatSql.RemoteTableFunctionResolverOptions{
				HTTPClient:       &http.Client{Transport: ch047RemoteTableFunctionBenchmarkTransport{body: test.body}},
				MaxResponseBytes: 1 << 20,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportMetric(float64(test.wireBytes), "wire-B")
			for range b.N {
				rows, err := resolver.ResolveSQLTableFunction("url", test.arguments)
				if err != nil {
					b.Fatal(err)
				}
				ch047RemoteTableFunctionBenchmarkSink = rows
			}
		})
	}
}

func ch047RemoteTableFunctionBenchmarkCSV() ([]byte, []byte) {
	var builder strings.Builder
	builder.WriteString("id,value\n")
	prefixLength := 0
	for index := 0; index < 2048; index++ {
		fmt.Fprintf(&builder, "%d,value-%04d\n", index, index)
		if index == 255 {
			prefixLength = builder.Len()
		}
	}
	full := []byte(builder.String())
	return full, full[:prefixLength]
}

type ch047RemoteTableFunctionBenchmarkTransport struct {
	body []byte
}

func (transport ch047RemoteTableFunctionBenchmarkTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body := transport.body
	status := http.StatusOK
	if header := request.Header.Get("Range"); header != "" {
		parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid benchmark range %q", header)
		}
		start, err := strconv.Atoi(parts[0])
		if err != nil || start < 0 {
			return nil, fmt.Errorf("invalid benchmark range start %q", parts[0])
		}
		end, err := strconv.Atoi(parts[1])
		if err != nil || end < start || end >= len(body) {
			return nil, fmt.Errorf("invalid benchmark range end %q", parts[1])
		}
		body = body[start : end+1]
		status = http.StatusPartialContent
	}
	return &http.Response{
		StatusCode:    status,
		Status:        strconv.Itoa(status),
		Header:        make(http.Header),
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}, nil
}

type ch047RemoteTableFunctionBenchmarkBase struct{}

func (ch047RemoteTableFunctionBenchmarkBase) ResolveSQLSource(kind, key string) ([]hatSql.SQLRow, error) {
	return nil, fmt.Errorf("unexpected SQL source %s(%q)", kind, key)
}

func (ch047RemoteTableFunctionBenchmarkBase) ResolveSQLTableFunction(name string, arguments []interface{}) ([]hatSql.SQLRow, error) {
	return nil, fmt.Errorf("unexpected delegated function %q", name)
}
