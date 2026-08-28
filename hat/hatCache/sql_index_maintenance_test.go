package hatCache

import "testing"

func TestSQLJSONIndexMaintenanceSchedulesRebuild(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	status, available, err := trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Current {
		t.Fatalf("initial SQLJSONIndexMaintenanceStats() = %#v, %v, %v", status, available, err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() error = %v", err)
	}
	status, available, err = trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || !status.Pending || status.Scheduled != 1 {
		t.Fatalf("scheduled SQLJSONIndexMaintenanceStats() = %#v, %v, %v", status, available, err)
	}
	processed, err := trie.RunScheduledSQLJSONIndexRebuilds(1)
	if err != nil || processed != 1 {
		t.Fatalf("RunScheduledSQLJSONIndexRebuilds() = %d, %v", processed, err)
	}
	status, available, err = trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Pending || !status.Current || status.Rebuilds != 1 {
		t.Fatalf("rebuilt SQLJSONIndexMaintenanceStats() = %#v, %v, %v", status, available, err)
	}
	trie.UpsertString("jobs", `[{"id":2,"state":"running"}]`)
	status, available, err = trie.SQLJSONIndexMaintenanceStats("jobs", "state")
	if err != nil || !available || status.Current {
		t.Fatalf("changed SQLJSONIndexMaintenanceStats() = %#v, %v, %v", status, available, err)
	}
	if err := trie.ScheduleSQLJSONIndexRebuild("jobs", "state"); err != nil {
		t.Fatalf("ScheduleSQLJSONIndexRebuild() after update error = %v", err)
	}
	processed, err = trie.RunScheduledSQLJSONIndexRebuilds(1)
	if err != nil || processed != 1 {
		t.Fatalf("RunScheduledSQLJSONIndexRebuilds() after update = %d, %v", processed, err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('jobs') AS job WHERE job.state = 'running' SELECT job.id", trie)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(2) {
		t.Fatalf("indexed query after rebuild = %#v, %v", result, err)
	}
}
