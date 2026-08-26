package hatCache

import (
	"context"
	"path/filepath"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestValidateStructuredBatchColumnsAcceptsSharedKey(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city", "country"},
	}
	if _, err := validateStructuredBatchColumns(request); err != nil {
		t.Fatalf("validateStructuredBatchColumns() error = %v", err)
	}
}

func TestValidateStructuredBatchColumnsAcceptsSharedSubkey(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("SG")},
	}
	if _, err := validateStructuredBatchColumns(request); err != nil {
		t.Fatalf("validateStructuredBatchColumns() error = %v", err)
	}
}

func TestValidateStructuredBatchColumnsAcceptsSharedValue(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
		},
		Keys:    []string{"profile", "queue", "members"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("shared")},
	}
	if _, err := validateStructuredBatchColumns(request); err != nil {
		t.Fatalf("validateStructuredBatchColumns() error = %v", err)
	}
}

func TestValidateStructuredBatchColumnsPreallocatesOnlyGuaranteedSharedBooleans(t *testing.T) {
	operations := []hatriecachev1.StructuredCommand{
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET,
	}
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: operations,
		Keys:       []string{"members"},
		Values:     [][]byte{[]byte("shared")},
	}
	columns, err := validateStructuredBatchColumns(request)
	if err != nil || columns.guaranteedIntegerResults != len(operations) {
		t.Fatalf("guaranteed shared boolean columns = %#v/%v, want %d", columns, err, len(operations))
	}

	request.Values[0] = nil
	columns, err = validateStructuredBatchColumns(request)
	if err != nil || columns.guaranteedIntegerResults != 0 {
		t.Fatalf("empty shared boolean columns = %#v/%v, want no preallocation", columns, err)
	}

	request.Values[0] = []byte("shared")
	request.Operations[1] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET
	columns, err = validateStructuredBatchColumns(request)
	if err != nil || columns.guaranteedIntegerResults != 0 {
		t.Fatalf("mixed integer columns = %#v/%v, want no preallocation", columns, err)
	}
}

func TestValidateStructuredBatchColumnsRejectsSharedKeyWithoutOperations(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{Keys: []string{"profile"}}
	if _, err := validateStructuredBatchColumns(request); err == nil {
		t.Fatal("validateStructuredBatchColumns() error = nil, want operations error")
	}
}

func TestValidateStructuredBatchColumnsRejectsSubkeyWithoutMapOperations(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET,
		},
		Keys:    []string{"first", "second"},
		Subkeys: []string{"unexpected"},
	}
	if _, err := validateStructuredBatchColumns(request); err == nil {
		t.Fatal("validateStructuredBatchColumns() error = nil, want subkey-count error")
	}
}

func TestValidateStructuredBatchColumnsRejectsValueWithoutValueOperations(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE,
		},
		Keys:   []string{"first", "second"},
		Values: [][]byte{[]byte("unexpected")},
	}
	if _, err := validateStructuredBatchColumns(request); err == nil {
		t.Fatal("validateStructuredBatchColumns() error = nil, want value-count error")
	}
}

func TestCacheGRPCServerStructuredBatchStreamExecutesSharedKey(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId: 101,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city", "country", "city", "country", "country"},
		Values:  [][]byte{[]byte("SG"), []byte("Singapore")},
	}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBatchId() != 101 || !response.GetOk() || response.GetError() != "" {
		t.Fatalf("shared-key structured response = %#v", response)
	}
	wantStatuses := []hatriecachev1.ScalarResultStatus{
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND,
	}
	if len(response.GetStatuses()) != len(wantStatuses) {
		t.Fatalf("shared-key structured statuses = %v, want %v", response.GetStatuses(), wantStatuses)
	}
	for index, want := range wantStatuses {
		if response.GetStatuses()[index] != want {
			t.Fatalf("shared-key structured status %d = %v, want %v", index, response.GetStatuses()[index], want)
		}
	}
	values := structuredBatchResponseValues(response)
	if len(values) != 2 || values[0] != "SG" || values[1] != "Singapore" {
		t.Fatalf("shared-key structured values = %q, want [SG Singapore]", values)
	}
}

func TestCacheGRPCServerStructuredBatchStreamExecutesSharedKeyAndSubkey(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 104,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("SG")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("shared-column structured response = %#v/%v", response, err)
	}
	wantStatuses := []hatriecachev1.ScalarResultStatus{
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK,
		hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND,
	}
	if len(response.GetStatuses()) != len(wantStatuses) {
		t.Fatalf("shared-column statuses = %v, want %v", response.GetStatuses(), wantStatuses)
	}
	for index, want := range wantStatuses {
		if response.GetStatuses()[index] != want {
			t.Fatalf("shared-column status %d = %v, want %v", index, response.GetStatuses()[index], want)
		}
	}
	values := structuredBatchResponseValues(response)
	if len(values) != 2 || values[0] != "SG" || values[1] != "SG" {
		t.Fatalf("shared-column values = %q, want [SG SG]", values)
	}
}

func TestCacheGRPCServerStructuredBatchStreamExecutesSharedSubkeyAcrossMixedKeys(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 106,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		},
		Keys:    []string{"first", "queue", "second", "first", "second"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("SG"), []byte("queued"), []byte("US")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("mixed-key shared-subkey response = %#v/%v", response, err)
	}
	for index, status := range response.GetStatuses() {
		if status != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
			t.Fatalf("mixed-key shared-subkey status %d = %v, want OK", index, status)
		}
	}
	values := structuredBatchResponseValues(response)
	if len(values) != 2 || values[0] != "SG" || values[1] != "US" {
		t.Fatalf("mixed-key shared-subkey values = %q, want [SG US]", values)
	}
}

func TestCacheGRPCServerStructuredBatchStreamExecutesSharedValueAcrossCommandFamilies(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 107,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY,
		},
		Keys:       []string{"profile", "queue", "members", "members", "priority"},
		Subkeys:    []string{"city"},
		Values:     [][]byte{[]byte("shared")},
		Priorities: []int64{7},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || len(response.GetStatuses()) != 5 {
		t.Fatalf("shared-value structured response = %#v/%v", response, err)
	}
	for index, status := range response.GetStatuses() {
		if status != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
			t.Fatalf("shared-value status %d = %v, want OK", index, status)
		}
	}
	mapValue, mapOK, mapErr := ht.PeekMapChecked("profile", "city")
	if mapErr != nil || !mapOK || mapValue != "shared" {
		t.Fatalf("shared-value map = %#v/%v/%v", mapValue, mapOK, mapErr)
	}
	if result := ht.ExecuteCommand(CacheCommandRequest{Command: "HEADSLICE", Key: "queue"}); !result.OK || result.Value != "shared" {
		t.Fatalf("shared-value slice = %#v", result)
	}
	if result := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSET", Key: "members", Value: "shared"}); !result.OK || result.Value != "1" {
		t.Fatalf("shared-value set = %#v", result)
	}
	if result := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKPQ", Key: "priority"}); !result.OK || result.Value != `{"priority":7,"value":"shared"}` {
		t.Fatalf("shared-value priority queue = %#v", result)
	}
}

func TestCacheGRPCServerStructuredBatchSharedKeySupportsLocalPartitions(t *testing.T) {
	ht := newTestTrie(t)
	if err := ht.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 103,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("SG")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || len(response.GetStatuses()) != 2 || response.GetStatuses()[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.GetStatuses()[1] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
		t.Fatalf("partitioned shared-key structured response = %#v/%v", response, err)
	}
	values := structuredBatchResponseValues(response)
	if len(values) != 1 || values[0] != "SG" {
		t.Fatalf("partitioned shared-key structured values = %q, want [SG]", values)
	}
}

func TestCacheGRPCServerStructuredBatchSharedSubkeyPreservesJournalAndDirtyTracking(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{Journal: journal, DirtyTracker: dirty})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 105,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city"},
		Values:  [][]byte{[]byte("SG"), []byte("Singapore")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("journaled shared-column structured batch = %#v/%v", response, err)
	}
	if journal.Sequence() != 2 || dirty.Pending() != 1 {
		t.Fatalf("shared-column durability state = sequence %d, dirty %d; want 2/1", journal.Sequence(), dirty.Pending())
	}
	recovered := newTestTrie(t)
	sequence, err := journal.Replay(recovered, 0)
	if err != nil {
		t.Fatal(err)
	}
	city, ok, peekErr := recovered.PeekMapChecked("profile", "city")
	if sequence != 2 || peekErr != nil || !ok || city != "Singapore" {
		t.Fatalf("shared-column replay = sequence %d, city %#v/%v/%v", sequence, city, ok, peekErr)
	}
}

func TestCacheGRPCServerStructuredBatchSharedValuePreservesJournalAndDirtyTracking(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{Journal: journal, DirtyTracker: dirty})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 108,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city", "country"},
		Values:  [][]byte{[]byte("shared")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("journaled shared-value structured batch = %#v/%v", response, err)
	}
	if journal.Sequence() != 2 || dirty.Pending() != 1 {
		t.Fatalf("shared-value durability state = sequence %d, dirty %d; want 2/1", journal.Sequence(), dirty.Pending())
	}
	recovered := newTestTrie(t)
	sequence, err := journal.Replay(recovered, 0)
	if err != nil {
		t.Fatal(err)
	}
	city, cityOK, cityErr := recovered.PeekMapChecked("profile", "city")
	country, countryOK, countryErr := recovered.PeekMapChecked("profile", "country")
	if sequence != 2 || cityErr != nil || countryErr != nil || !cityOK || !countryOK || city != "shared" || country != "shared" {
		t.Fatalf("shared-value replay = sequence %d, city %#v/%v/%v, country %#v/%v/%v", sequence, city, cityOK, cityErr, country, countryOK, countryErr)
	}
}

func TestCacheGRPCServerStructuredBatchSharedValueSupportsLocalPartitions(t *testing.T) {
	ht := newTestTrie(t)
	if err := ht.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 109,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET,
		},
		Keys:   []string{"members"},
		Values: [][]byte{[]byte("shared")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || len(response.GetStatuses()) != 2 || response.GetStatuses()[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.GetStatuses()[1] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
		t.Fatalf("partitioned shared-value structured response = %#v/%v", response, err)
	}
}

func TestCacheGRPCServerStructuredBatchSharedKeyPreservesJournalAndDirtyTracking(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{Journal: journal, DirtyTracker: dirty})
	defer stop()
	stream, err := client.StructuredBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.StructuredBatchRequest{
		BatchId: 102,
		Operations: []hatriecachev1.StructuredCommand{
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
		},
		Keys:    []string{"profile"},
		Subkeys: []string{"city", "country"},
		Values:  [][]byte{[]byte("SG"), []byte("Singapore")},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("journaled shared-key structured batch = %#v/%v", response, err)
	}
	if journal.Sequence() != 2 || dirty.Pending() != 1 {
		t.Fatalf("shared-key structured durability state = sequence %d, dirty %d; want 2/1", journal.Sequence(), dirty.Pending())
	}
	recovered := newTestTrie(t)
	sequence, err := journal.Replay(recovered, 0)
	if err != nil {
		t.Fatal(err)
	}
	city, cityOK, cityErr := recovered.PeekMapChecked("profile", "city")
	country, countryOK, countryErr := recovered.PeekMapChecked("profile", "country")
	if sequence != 2 || cityErr != nil || countryErr != nil || !cityOK || !countryOK || city != "SG" || country != "Singapore" {
		t.Fatalf("shared-key structured replay = sequence %d, city %#v/%v/%v, country %#v/%v/%v", sequence, city, cityOK, cityErr, country, countryOK, countryErr)
	}
}
