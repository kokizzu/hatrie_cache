package hatriecache

import (
	"context"
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
