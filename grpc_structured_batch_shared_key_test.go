package hatriecache

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
	if err := validateStructuredBatchColumns(request); err != nil {
		t.Fatalf("validateStructuredBatchColumns() error = %v", err)
	}
}

func TestValidateStructuredBatchColumnsRejectsSharedKeyWithoutOperations(t *testing.T) {
	request := &hatriecachev1.StructuredBatchRequest{Keys: []string{"profile"}}
	if err := validateStructuredBatchColumns(request); err == nil {
		t.Fatal("validateStructuredBatchColumns() error = nil, want operations error")
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
		Subkeys: []string{"city", "city"},
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
