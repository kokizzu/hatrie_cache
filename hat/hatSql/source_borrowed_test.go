package hatSql

import "testing"

type sqlBorrowedSourceProbe struct {
	rows          []Row
	borrowedCalls int
}

func (probe *sqlBorrowedSourceProbe) ResolveSQLSource(name, key string) ([]Row, error) {
	return probe.rows, nil
}

func (probe *sqlBorrowedSourceProbe) BorrowSQLSource(name, key string) ([]Row, bool, error) {
	probe.borrowedCalls++
	return probe.rows, true, nil
}

func TestSQLSourceResolverUsesBorrowedImmutableSnapshotWhenAvailable(t *testing.T) {
	t.Parallel()
	probe := &sqlBorrowedSourceProbe{rows: []Row{{"id": int64(1), "team": "core"}}}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.team = 'core' SELECT event.id", probe)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) || probe.borrowedCalls != 1 {
		t.Fatalf("ExecuteSQLQuery() = %#v, %v, borrowed calls = %d", result, err, probe.borrowedCalls)
	}
}
