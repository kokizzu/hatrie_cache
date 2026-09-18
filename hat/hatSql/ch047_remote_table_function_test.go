package hatSql_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestRemoteTableFunctionReadsCSVRangeAndDelegates(t *testing.T) {
	payload := "id,name\n1,alpha\n2,beta\n"
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotRange = request.Header.Get("Range")
		http.ServeContent(writer, request, "data.csv", time.Unix(0, 0), bytes.NewReader([]byte(payload)))
	}))
	defer server.Close()

	resolver, err := hatSql.NewRemoteTableFunctionResolver(testRemoteTableFunctionResolver{}, hatSql.RemoteTableFunctionResolverOptions{
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewRemoteTableFunctionResolver() error = %v", err)
	}
	query := fmt.Sprintf("FROM TABLE(url('%s', 'csv', 0, %d)) AS item SELECT item.id, item.name ORDER BY item.id", server.URL+"/data.csv", len(payload))
	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if gotRange != "bytes=0-"+strconv.Itoa(len(payload)-1) {
		t.Fatalf("Range header = %q, want full bounded range", gotRange)
	}
	want := []hatSql.SQLRow{{"id": "1", "name": "alpha"}, {"id": "2", "name": "beta"}}
	if fmt.Sprint(result.Rows) != fmt.Sprint(want) {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}

	delegated, err := resolver.ResolveSQLTableFunction("series", []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("delegated table function error = %v", err)
	}
	if len(delegated) != 1 || delegated[0]["value"] != int64(1) {
		t.Fatalf("delegated rows = %#v", delegated)
	}
}

func TestRemoteTableFunctionReadsS3StyleNDJSON(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.Path
		writer.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = writer.Write([]byte("{\"id\":1}\n{\"id\":2}\n"))
	}))
	defer server.Close()

	resolver, err := hatSql.NewRemoteTableFunctionResolver(testRemoteTableFunctionResolver{}, hatSql.RemoteTableFunctionResolverOptions{
		S3Endpoint:       server.URL,
		MaxResponseBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewRemoteTableFunctionResolver() error = %v", err)
	}
	result, err := hatSql.ExecuteSQLQuery("FROM TABLE(s3('bucket', 'object.ndjson', 'ndjson')) AS item SELECT item.id ORDER BY item.id", resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if gotPath != "/bucket/object.ndjson" {
		t.Fatalf("S3 path = %q, want /bucket/object.ndjson", gotPath)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["id"] != float64(2) {
		t.Fatalf("S3 rows = %#v", result.Rows)
	}
}

func TestRemoteTableFunctionRejectsUnsafeURL(t *testing.T) {
	resolver, err := hatSql.NewRemoteTableFunctionResolver(testRemoteTableFunctionResolver{}, hatSql.RemoteTableFunctionResolverOptions{})
	if err != nil {
		t.Fatalf("NewRemoteTableFunctionResolver() error = %v", err)
	}
	_, err = resolver.ResolveSQLTableFunction("url", []interface{}{"file:///etc/passwd", "json"})
	if err == nil || !strings.Contains(err.Error(), "HTTP or HTTPS") {
		t.Fatalf("unsafe URL error = %v, want HTTP or HTTPS validation", err)
	}
}

func TestRemoteTableFunctionRejectsS3PathTraversal(t *testing.T) {
	resolver, err := hatSql.NewRemoteTableFunctionResolver(testRemoteTableFunctionResolver{}, hatSql.RemoteTableFunctionResolverOptions{})
	if err != nil {
		t.Fatalf("NewRemoteTableFunctionResolver() error = %v", err)
	}
	_, err = resolver.ResolveSQLTableFunction("s3", []interface{}{"bucket", "../secret.json", "json"})
	if err == nil || !strings.Contains(err.Error(), "S3 key") {
		t.Fatalf("S3 traversal error = %v, want key validation", err)
	}
}

type testRemoteTableFunctionResolver struct{}

func (testRemoteTableFunctionResolver) ResolveSQLSource(kind, key string) ([]hatSql.SQLRow, error) {
	return nil, fmt.Errorf("unexpected SQL source %s(%q)", kind, key)
}

func (testRemoteTableFunctionResolver) ResolveSQLTableFunction(name string, arguments []interface{}) ([]hatSql.SQLRow, error) {
	if name != "series" || len(arguments) != 1 {
		return nil, fmt.Errorf("unexpected delegated table function %q %#v", name, arguments)
	}
	return []hatSql.SQLRow{{"value": arguments[0]}}, nil
}
