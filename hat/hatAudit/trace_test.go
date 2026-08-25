package hatAudit_test

import (
	"testing"

	"hatrie_cache/hat/hatAudit"
	"hatrie_cache/hat/hatCommand"
)

func TestQueryAndReplayTraceFilterAndValidateResponses(t *testing.T) {
	logger := hatAudit.NewAuditLogger(nil)
	if err := logger.Log(hatAudit.AuditEvent{Action: "backup", Command: "SET", Key: "backup:1", OK: true}); err != nil {
		t.Fatalf("Log backup error = %v", err)
	}
	if err := logger.Log(hatAudit.AuditEvent{Action: "command", Command: "SET", Key: "cache:1", OK: true}); err != nil {
		t.Fatalf("Log command error = %v", err)
	}
	events, err := logger.Query(hatAudit.Query{Action: "command", KeyPrefix: "cache:"})
	if err != nil || len(events) != 1 || events[0].Key != "cache:1" {
		t.Fatalf("Query() = %#v, %v", events, err)
	}

	recorder := hatAudit.NewTraceRecorder(nil, 2)
	response := hatCommand.Response{OK: true, Message: "stored"}
	if err := recorder.Record(hatCommand.Request{Command: "SET", Key: "cache:1", BinaryValue: []byte{1, 2}}, response); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	report, err := hatAudit.Replay(recorder.Traces(), func(request hatCommand.Request) hatCommand.Response {
		if string(request.BinaryValue) != "\x01\x02" {
			t.Fatalf("replay binary value = %v", request.BinaryValue)
		}
		return response
	})
	if err != nil || report.Applied != 1 || report.Mismatches != 0 {
		t.Fatalf("Replay() = %#v, %v", report, err)
	}
}
