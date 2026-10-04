package hatSql

import "testing"

type tu24ConditionalResolver struct {
	calls int
}

func (resolver *tu24ConditionalResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *tu24ConditionalResolver) ResolveSQLConditionalIndexedSource(name, key string, fields []string, values []interface{}) ([]Row, bool, error) {
	resolver.calls++
	if name != "CACHE" || key != "jobs" || len(fields) != 2 || len(values) != 2 {
		return nil, false, nil
	}
	return []Row{{"id": int64(1), "status": "active"}}, true, nil
}

func TestTU24ConditionalIndexForwardingThroughCatalogAndSession(t *testing.T) {
	base := &tu24ConditionalResolver{}
	catalog := CatalogResolver{Source: base}
	rows, available, err := catalog.ResolveSQLConditionalIndexedSource("CACHE", "jobs", []string{"id", "status"}, []interface{}{int64(1), "active"})
	if err != nil || !available || len(rows) != 1 || base.calls != 1 {
		t.Fatalf("catalog forwarding = rows=%#v available=%t calls=%d err=%v", rows, available, base.calls, err)
	}

	session := NewSQLSession(base)
	rows, available, err = session.ResolveSQLConditionalIndexedSource("CACHE", "jobs", []string{"id", "status"}, []interface{}{int64(1), "active"})
	if err != nil || !available || len(rows) != 1 || base.calls != 2 {
		t.Fatalf("session forwarding = rows=%#v available=%t calls=%d err=%v", rows, available, base.calls, err)
	}
}
