package hatCache

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestExecuteSQLQuerySupportsTypedIPValues(t *testing.T) {
	v4a, err := ParseSQLIPv4("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	v4b, err := ParseSQLIPv4("192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	v6a, err := ParseSQLIPv6("2001:db8::1")
	if err != nil {
		t.Fatal(err)
	}
	v6b, err := ParseSQLIPv6("2001:db8::2")
	if err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQuery(`FROM VALUES
	(IPV4 '192.0.2.2', IPV6 '2001:db8::2'),
	(IPV4 '192.0.2.1', IPV6 '2001:db8::1')
	AS values(v4, v6)
	SELECT v4, v6, CAST(v4 AS TEXT) AS v4_text, CAST(v6 AS TEXT) AS v6_text
	WHERE v4 >= IPV4 '192.0.2.1'
	ORDER BY v4`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{
		Columns: []string{"v4", "v6", "v4_text", "v6_text"},
		Rows: []SQLRow{
			{"v4": v4a, "v6": v6a, "v4_text": "192.0.2.1", "v6_text": "2001:db8::1"},
			{"v4": v4b, "v6": v6b, "v4_text": "192.0.2.2", "v6_text": "2001:db8::2"},
		},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("typed IP result = %#v, want %#v", result, want)
	}
	encoded, err := json.Marshal(result.Rows[0])
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if got, wantJSON := string(encoded), `{"v4":"192.0.2.1","v4_text":"192.0.2.1","v6":"2001:db8::1","v6_text":"2001:db8::1"}`; got != wantJSON {
		t.Fatalf("JSON result = %s, want %s", got, wantJSON)
	}

	for _, query := range []string{
		`FROM VALUES (IPV4 '2001:db8::1') AS values(value) SELECT value`,
		`FROM VALUES (IPV6 '192.0.2.1') AS values(value) SELECT value`,
	} {
		if _, err := ExecuteSQLQuery(query, nil); err == nil {
			t.Errorf("ExecuteSQLQuery(%q) error = nil", query)
		}
	}
}
