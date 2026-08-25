package hatJournal_test

import (
	"testing"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatJournal"
)

func TestRecordUsesPortableCommandContract(t *testing.T) {
	record := hatJournal.Record{Sequence: 9, Request: hatCommand.Request{Command: "SET", Key: "session:9", Value: "active"}}
	if record.Sequence != 9 || record.Request.Command != "SET" || record.Request.Key != "session:9" {
		t.Fatalf("Record = %#v", record)
	}
}
