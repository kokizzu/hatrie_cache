package hatSql

import (
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

const mz014TestSchema = `{"type":"record","name":"event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`

func TestMZ014AvroSchemaRegistryCachesAndBounds(t *testing.T) {
	var fetches int32
	registry, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{
		MaxSchemas:     1,
		MaxSchemaBytes: 1024,
		Fetch: func(schemaID uint32) ([]byte, error) {
			atomic.AddInt32(&fetches, 1)
			return []byte(mz014TestSchema + string(rune(schemaID))), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Borrow(7)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := registry.Borrow(7); err != nil || string(second) != string(first) {
		t.Fatalf("cached schema = %q, %v", second, err)
	}
	copyOfSchema, err := registry.Schema(7)
	if err != nil {
		t.Fatal(err)
	}
	copyOfSchema[0] = 'X'
	unchanged, err := registry.Borrow(7)
	if err != nil || unchanged[0] != '{' {
		t.Fatalf("schema copy mutated cache: %q, %v", unchanged, err)
	}
	if _, err := registry.Borrow(8); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Borrow(7); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&fetches); got != 3 {
		t.Fatalf("fetches = %d, want 3 after one eviction", got)
	}
	stats := registry.Stats()
	if stats.Entries != 1 || stats.Evictions != 2 || stats.Hits < 2 || stats.Fetches != 3 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestMZ014AvroSchemaRegistryCoalescesConcurrentFetches(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var fetches int32
	registry, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{
		Fetch: func(uint32) ([]byte, error) {
			atomic.AddInt32(&fetches, 1)
			close(started)
			<-release
			return []byte(mz014TestSchema), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	results := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			defer group.Done()
			_, callErr := registry.Borrow(7)
			results <- callErr
		}()
	}
	<-started
	close(release)
	group.Wait()
	close(results)
	for callErr := range results {
		if callErr != nil {
			t.Fatal(callErr)
		}
	}
	if got := atomic.LoadInt32(&fetches); got != 1 {
		t.Fatalf("fetches = %d, want one coalesced fetch", got)
	}
}

func TestMZ014ConfluentAvroDecoderAndKafkaSource(t *testing.T) {
	var fetches int32
	registry, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{
		Fetch: func(schemaID uint32) ([]byte, error) {
			atomic.AddInt32(&fetches, 1)
			if schemaID != 7 {
				return nil, ErrAvroSchemaNotFound
			}
			return []byte(mz014TestSchema), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewAvroKafkaTableDecoder(registry, func(schemaID uint32, schema, payload []byte, message KafkaTableMessage) (KafkaTableChange, error) {
		if schemaID != 7 || string(schema) != mz014TestSchema || string(payload) != "value" || message.Key != "event-1" {
			return KafkaTableChange{}, errors.New("decoder received unexpected envelope")
		}
		return KafkaTableChange{Operation: KafkaTableUpsert, Row: Row{"id": message.Key, "value": string(payload)}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 5+len("value"))
	payload[0] = 0
	binary.BigEndian.PutUint32(payload[1:5], 7)
	copy(payload[5:], "value")
	change, err := decoder(KafkaTableMessage{Key: "event-1", Value: payload})
	if err != nil {
		t.Fatal(err)
	}
	if change.Operation != KafkaTableUpsert || change.Row["value"] != "value" {
		t.Fatalf("change = %#v", change)
	}
	deleteChange, err := decoder(KafkaTableMessage{Key: "event-1", Value: nil})
	if err != nil {
		t.Fatal(err)
	}
	if deleteChange.Operation != KafkaTableDelete || atomic.LoadInt32(&fetches) != 1 {
		t.Fatalf("delete change = %#v, fetches = %d", deleteChange, fetches)
	}

	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "avro-source",
		Table:   "events",
		Topic:   "events",
		Decoder: decoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{TransactionID: "tx-1", Messages: []KafkaTableMessage{{
		Topic: "events", Partition: "0", Offset: 0, Key: "event-1", Value: payload,
	}}}); err != nil {
		t.Fatal(err)
	}
	rows, err := source.ResolveSQLSource("KAFKA", "events")
	if err != nil || len(rows) != 1 || rows[0]["value"] != "value" {
		t.Fatalf("rows = %#v, %v", rows, err)
	}
}

func TestMZ014ConfluentAvroPayloadValidation(t *testing.T) {
	if _, _, err := ParseConfluentAvroPayload(nil); !errors.Is(err, ErrConfluentAvroPayloadInvalid) {
		t.Fatalf("empty payload error = %v", err)
	}
	if _, _, err := ParseConfluentAvroPayload([]byte{1, 0, 0, 0, 7}); !errors.Is(err, ErrConfluentAvroPayloadInvalid) {
		t.Fatalf("wrong magic error = %v", err)
	}
	payload := []byte{0, 0, 0, 0, 7, 1, 2}
	schemaID, datum, err := ParseConfluentAvroPayload(payload)
	if err != nil || schemaID != 7 || !reflect.DeepEqual(datum, []byte{1, 2}) {
		t.Fatalf("parsed payload = %d/%v/%v", schemaID, datum, err)
	}
}

func TestMZ014AvroSchemaCompatibility(t *testing.T) {
	oldSchema := `{"type":"record","name":"event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`
	newSchema := `{"type":"record","name":"event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}`
	if err := ValidateAvroSchemaCompatibility([]byte(oldSchema), []byte(newSchema), AvroCompatibilityFull); err != nil {
		t.Fatalf("defaulted field compatibility = %v", err)
	}
	withoutDefault := `{"type":"record","name":"event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"active","type":"boolean"}]}`
	if err := ValidateAvroSchemaCompatibility([]byte(oldSchema), []byte(withoutDefault), AvroCompatibilityBackward); !errors.Is(err, ErrAvroSchemaIncompatible) {
		t.Fatalf("missing default error = %v", err)
	}
	if err := ValidateAvroSchemaCompatibility([]byte(oldSchema), []byte(`{"type":"record","name":"event","fields":[{"name":"id","type":"double"},{"name":"name","type":"string"}]}`), AvroCompatibilityBackward); err != nil {
		t.Fatalf("numeric promotion error = %v", err)
	}
	if err := ValidateAvroSchemaCompatibility([]byte(oldSchema), []byte(`{"type":"record","name":"event","fields":[{"name":"id","type":"long"},{"name":"name","type":"long"}]}`), AvroCompatibilityBackward); !errors.Is(err, ErrAvroSchemaIncompatible) {
		t.Fatalf("incompatible type error = %v", err)
	}
	if err := ValidateAvroSchemaCompatibility([]byte(oldSchema), []byte(`{"type":"array","items":"string"}`), AvroCompatibilityBackward); !errors.Is(err, ErrAvroSchemaCompatibilityInvalid) {
		t.Fatalf("unsupported schema error = %v", err)
	}
}

func TestMZ014AvroSchemaRegistryRejectsUnsafeConfiguration(t *testing.T) {
	if _, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{}); !errors.Is(err, ErrAvroSchemaRegistryFetcherRequired) {
		t.Fatalf("missing fetcher error = %v", err)
	}
	registry, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{
		MaxSchemaBytes: 4,
		Fetch:          func(uint32) ([]byte, error) { return []byte("too large"), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Borrow(7); !errors.Is(err, ErrAvroSchemaTooLarge) {
		t.Fatalf("large schema error = %v", err)
	}
}
