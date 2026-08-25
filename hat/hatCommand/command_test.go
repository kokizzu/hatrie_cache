package hatCommand_test

import (
	"reflect"
	"testing"

	json "github.com/goccy/go-json"

	"hatrie_cache/hat/hatCommand"
)

func TestRequestAndResponseJSONContractsRoundTrip(t *testing.T) {
	priority := int64(7)
	request := hatCommand.Request{
		Command:  "PUTMAP",
		Key:      "account:42",
		Pairs:    map[string]any{"tier": "gold"},
		Priority: &priority,
		Batch: []hatCommand.Request{{
			Command: "SET",
			Key:     "account:43",
			Value:   "active",
		}},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded hatCommand.Request
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("request round trip = %#v, want %#v", decoded, request)
	}

	response := hatCommand.Response{OK: true, Message: "batch applied", Responses: []hatCommand.Response{{OK: true, Value: "active"}}}
	encoded, err = json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response error = %v", err)
	}
	var decodedResponse hatCommand.Response
	if err := json.Unmarshal(encoded, &decodedResponse); err != nil {
		t.Fatalf("Unmarshal response error = %v", err)
	}
	if !reflect.DeepEqual(decodedResponse, response) {
		t.Fatalf("response round trip = %#v, want %#v", decodedResponse, response)
	}
}
