package hatSql

import "testing"

func TestNamedQueryTemplateAndResultAssertions(t *testing.T) {
	template, err := NewNamedQueryTemplate("active_user", "SELECT * FROM CACHE('users') WHERE id = :id AND state = :state")
	if err != nil {
		t.Fatal(err)
	}
	source, parameters, err := template.Bind(map[string]interface{}{"id": int64(7), "state": "active"})
	if err != nil || source != "SELECT * FROM CACHE('users') WHERE id = $1 AND state = $2" || len(parameters) != 2 || parameters[0] != int64(7) || parameters[1] != "active" {
		t.Fatalf("Bind() = %q, %#v, %v", source, parameters, err)
	}
	result := QueryResult{Rows: []Row{{"id": 7}}}
	if _, err := RequireExactlyOne(result); err != nil {
		t.Fatal(err)
	}
	if err := RequireNonEmpty(QueryResult{}); err == nil {
		t.Fatal("RequireNonEmpty accepted empty result")
	}
}
