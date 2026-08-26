package hatCache

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestValidateScalarBatchColumnsAcceptsSharedKey(t *testing.T) {
	request := &hatriecachev1.ScalarBatchRequest{
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS,
		},
		Keys: []string{"shared"},
	}
	if err := validateScalarBatchColumns(request); err != nil {
		t.Fatalf("validateScalarBatchColumns() error = %v", err)
	}
}

func TestCacheGRPCServerScalarBatchStreamExecutesSharedKey(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId: 81,
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
		},
		Keys:          []string{"shared"},
		StringValues:  [][]byte{[]byte("ivi")},
		IntegerValues: []int64{40, 2},
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBatchId() != 81 || !response.GetOk() || response.GetError() != "" {
		t.Fatalf("shared-key scalar response = %#v", response)
	}
	wantStatuses := []hatriecachev1.ScalarResultStatus{
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND,
	}
	if len(response.GetStatuses()) != len(wantStatuses) {
		t.Fatalf("shared-key scalar statuses = %v, want %v", response.GetStatuses(), wantStatuses)
	}
	for index, want := range wantStatuses {
		if response.GetStatuses()[index] != want {
			t.Fatalf("shared-key scalar status %d = %v, want %v", index, response.GetStatuses()[index], want)
		}
	}
	if string(response.GetValues()) != "ivi42" || len(response.GetValueEnds()) != 2 || response.GetValueEnds()[0] != 3 || response.GetValueEnds()[1] != 5 {
		t.Fatalf("shared-key scalar byte values = %q/%v, want ivi42/[3 5]", response.GetValues(), response.GetValueEnds())
	}
	if len(response.GetIntegerValues()) != 2 || response.GetIntegerValues()[0] != 42 || response.GetIntegerValues()[1] != 1 {
		t.Fatalf("shared-key scalar integer values = %v, want [42 1]", response.GetIntegerValues())
	}
}

func TestCacheGRPCServerScalarBatchSharedKeyPreservesJournalAndDirtyTracking(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{Journal: journal, DirtyTracker: dirty})
	defer stop()
	stream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.ScalarBatchRequest{
		BatchId: 82,
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
		},
		Keys:          []string{"count"},
		IntegerValues: []int64{40, 2},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || len(response.GetIntegerValues()) != 1 || response.GetIntegerValues()[0] != 42 {
		t.Fatalf("journaled shared-key scalar batch = %#v/%v, want increment result 42", response, err)
	}
	if journal.Sequence() != 2 || dirty.Pending() != 1 {
		t.Fatalf("shared-key scalar durability state = sequence %d, dirty %d; want 2/1", journal.Sequence(), dirty.Pending())
	}
	recovered := newTestTrie(t)
	sequence, err := journal.Replay(recovered, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 2 || recovered.GetCounter("count") != 42 {
		t.Fatalf("shared-key scalar journal replay = %d/count=%d, want 2/42", sequence, recovered.GetCounter("count"))
	}
}

func TestExecuteScalarBatchDirectSharedKeyMatchesExpandedNativeBatch(t *testing.T) {
	compactTrie := newTestTrie(t)
	expandedTrie := newTestTrie(t)
	compact := scalarBatchMixedSharedKeyRequest(64, true)
	expanded := scalarBatchMixedSharedKeyRequest(64, false)
	compactResponse := compactTrie.executeScalarBatchDirect(context.Background(), compact)
	expandedResponse := expandedTrie.executeScalarBatchDirect(context.Background(), expanded)
	if !reflect.DeepEqual(compactResponse, expandedResponse) {
		t.Fatalf("compact response = %#v, want expanded %#v", compactResponse, expandedResponse)
	}
}

func scalarBatchMixedSharedKeyRequest(commands int, compact bool) *hatriecachev1.ScalarBatchRequest {
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    83,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		request.Keys[index] = "shared:native"
		switch index % 6 {
		case 0:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING
			request.StringValues = append(request.StringValues, []byte("value"))
		case 1:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		case 2:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER
			request.IntegerValues = append(request.IntegerValues, 7)
		case 3:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT
			request.IntegerValues = append(request.IntegerValues, 2)
		case 4:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS
		case 5:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE
		}
	}
	if compact && commands > 1 {
		request.Keys = request.Keys[:1]
	}
	return request
}
