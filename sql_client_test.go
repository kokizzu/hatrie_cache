package hatriecache

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
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":["name"],"rows":[{"name":"Ivi"},{"name":"Lia"}]}`))
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

func TestSQLConnQueryRowsFollowsBoundedCursorPages(t *testing.T) {
	t.Parallel()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request SQLQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		requests++
		if request.PageSize <= 0 || request.PageSize > 1000 {
			t.Errorf("page_size = %d, want bounded positive size", request.PageSize)
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Cursor {
		case "":
			_, _ = w.Write([]byte(`{"columns":["name"],"rows":[{"name":"Ivi"}],"has_more":true,"next_cursor":"second"}`))
		case "second":
			_, _ = w.Write([]byte(`{"columns":["name"],"rows":[{"name":"Lia"}]}`))
		default:
			t.Errorf("cursor = %q", request.Cursor)
		}
	}))
	defer server.Close()
	var names []string
	n, err := QueryRows(context.Background(), NewSQLConn(server.URL, ""), "FROM KEYS SELECT key", func(person sqlClientPerson) error {
		names = append(names, person.Name)
		return nil
	})
	if err != nil || n != 2 || !reflect.DeepEqual(names, []string{"Ivi", "Lia"}) || requests != 2 {
		t.Fatalf("QueryRows() n/names/requests/error = %d/%#v/%d/%v", n, names, requests, err)
	}
}
