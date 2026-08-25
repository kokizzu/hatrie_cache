package hatriecache

import "testing"

func TestExecuteTracedCommandReplaysAgainstFreshTrie(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	recorder := NewWorkloadTraceRecorder(nil, 0)
	if response, err := source.ExecuteTracedCommand(recorder, CacheCommandRequest{Command: "SET", Key: "account:1", Value: "active"}); err != nil || !response.OK {
		t.Fatalf("SET response/error = %#v, %v", response, err)
	}
	if response, err := source.ExecuteTracedCommand(recorder, CacheCommandRequest{Command: "SETINT", Key: "counter:1", Value: "4"}); err != nil || !response.OK {
		t.Fatalf("SETINT response/error = %#v, %v", response, err)
	}
	if response, err := source.ExecuteTracedCommand(recorder, CacheCommandRequest{Command: "INC", Key: "counter:1", Value: "2"}); err != nil || !response.OK {
		t.Fatalf("INC response/error = %#v, %v", response, err)
	}

	replayed := CreateHatTrie()
	defer replayed.Destroy()
	report, err := ReplayWorkloadTrace(replayed, recorder.Traces())
	if err != nil || report.Applied != 3 || report.Mismatches != 0 {
		t.Fatalf("ReplayWorkloadTrace() = %#v, %v", report, err)
	}
	if value := replayed.GetString("account:1"); value != "active" {
		t.Fatalf("replayed account = %q", value)
	}
	if value := replayed.GetCounter("counter:1"); value != 6 {
		t.Fatalf("replayed counter = %d", value)
	}
}
