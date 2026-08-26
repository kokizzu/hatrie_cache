package hatCache

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type sqlClientPerson struct {
	Name string `json:"name"`
}

func TestSQLConnQueryRowsDecodesAndStopsOnCallbackError(t *testing.T) {
	t.Parallel()

	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		if r.URL.Path != "/api/sql" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s, want POST /api/sql", r.Method, r.URL.Path)
		}
		var request SQLQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || !request.Stream {
			t.Errorf("stream request = %#v/%v, want stream=true", request, err)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"type\":\"columns\",\"columns\":[\"name\"]}\n{\"type\":\"row\",\"row\":{\"name\":\"Ivi\"}}\n{\"type\":\"row\",\"row\":{\"name\":\"Lia\"}}\n"))
	}))
	defer server.Close()

	conn := NewSQLConn(server.URL, "secret")
	var names []string
	stop := errors.New("stop")
	n, err := QueryRows(context.Background(), conn, "FROM KEYS SELECT key", func(person sqlClientPerson) error {
		names = append(names, person.Name)
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("QueryRows() error = %v, want callback stop", err)
	}
	if n != 1 || !reflect.DeepEqual(names, []string{"Ivi"}) {
		t.Fatalf("QueryRows() n/names = %d/%#v, want 1/[Ivi]", n, names)
	}
	if authorization != "Bearer secret" {
		t.Fatalf("Authorization = %q, want bearer token", authorization)
	}
}

func TestSQLConnQueryRowsConsumesOneNDJSONResponse(t *testing.T) {
	t.Parallel()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request SQLQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		requests++
		if !request.Stream || request.PageSize != 0 || request.Cursor != "" {
			t.Errorf("stream/page/cursor = %t/%d/%q, want true/0/empty", request.Stream, request.PageSize, request.Cursor)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"type\":\"columns\",\"columns\":[\"name\"]}\n{\"type\":\"row\",\"row\":{\"name\":\"Ivi\"}}\n{\"type\":\"row\",\"row\":{\"name\":\"Lia\"}}\n{\"type\":\"done\",\"rows\":2}\n"))
	}))
	defer server.Close()
	var names []string
	n, err := QueryRows(context.Background(), NewSQLConn(server.URL, ""), "FROM KEYS SELECT key", func(person sqlClientPerson) error {
		names = append(names, person.Name)
		return nil
	})
	if err != nil || n != 2 || !reflect.DeepEqual(names, []string{"Ivi", "Lia"}) || requests != 1 {
		t.Fatalf("QueryRows() n/names/requests/error = %d/%#v/%d/%v", n, names, requests, err)
	}
}
