package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseStorageFormat(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  StorageFormat
	}{
		{value: "", want: StorageFormatBinary},
		{value: " binary ", want: StorageFormatBinary},
		{value: "bin", want: StorageFormatBinary},
		{value: "json", want: StorageFormatJSON},
	} {
		got, err := ParseStorageFormat(tt.value)
		if err != nil {
			t.Fatalf("ParseStorageFormat(%q) error = %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("ParseStorageFormat(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}
	if _, err := ParseStorageFormat("msgpack"); err == nil {
		t.Fatal("ParseStorageFormat(msgpack) error = nil, want unsupported format error")
	}
}

func TestLevelDBStoreDefaultFormatWritesBinaryAndLoadsLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("binary", "value")
	source.UpsertBytes("blob", []byte{0, 1, 2, 3})

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(default) error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	data, ok, err := store.entryData("binary")
	if err != nil || !ok {
		t.Fatalf("entryData(binary) = %v/%v, want saved record", err, ok)
	}
	if !levelDBEntryDataIsBinary(data) {
		t.Fatalf("default LevelDB record header = % x, want binary", data[:shortHeaderLen(data)])
	}

	if err := store.db.Put(levelDBKey("legacy"), []byte(`{"key":"legacy","type":"string","string":"json"}`), nil); err != nil {
		t.Fatalf("Put(legacy JSON) error = %v", err)
	}

	loaded := newTestTrie(t)
	count, err := store.Load(loaded)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("Load() count = %d, want 3", count)
	}
	if got := loaded.GetString("binary"); got != "value" {
		t.Fatalf("loaded binary = %q, want value", got)
	}
	if got := loaded.GetString("legacy"); got != "json" {
		t.Fatalf("loaded legacy = %q, want json", got)
	}
	if got := loaded.GetBytes("blob"); !bytes.Equal(got, []byte{0, 1, 2, 3}) {
		t.Fatalf("loaded blob = %v, want [0 1 2 3]", got)
	}
}

func TestLevelDBStoreJSONFormatWritesPreviousLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, StorageFormatJSON)
	if err != nil {
		t.Fatalf("OpenLevelDBStoreWithFormat(json) error = %v", err)
	}
	defer store.Close()
	source := newTestTrie(t)
	source.UpsertString("json", "value")

	if err := store.Save(source); err != nil {
		t.Fatalf("Save(json format) error = %v", err)
	}
	data, ok, err := store.entryData("json")
	if err != nil || !ok {
		t.Fatalf("entryData(json) = %v/%v, want saved record", err, ok)
	}
	if levelDBEntryDataIsBinary(data) || len(data) == 0 || data[0] != '{' {
		t.Fatalf("json LevelDB record = %q, want previous JSON layout", data)
	}
	entry, err := decodeLevelDBEntry(data)
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(json) error = %v", err)
	}
	if entry.Key != "json" || entry.Type != "string" || entry.String != "value" {
		t.Fatalf("decoded JSON entry = %#v, want string value", entry)
	}
}

func TestLevelDBBinaryRecordsPreallocateStringAndComplexValues(t *testing.T) {
	expiresAt := time.Unix(1234, 0)
	stats := &KeyStats{
		Reads:             9,
		Hits:              7,
		Misses:            2,
		Writes:            3,
		LastHit:           time.Unix(1235, 0),
		LastMiss:          time.Unix(1236, 0),
		LastWrite:         time.Unix(1237, 0),
		HitRate:           0.75,
		CumulativeHitRate: 0.625,
	}
	stringEntry := snapshotEntry{
		Key:       "large:string",
		Type:      "string",
		String:    strings.Repeat("active-user-", 4096),
		ExpiresAt: &expiresAt,
		Stats:     stats,
	}
	mapEntry := snapshotEntry{
		Key:  "profile",
		Type: "map",
		Map: Map{
			"name": "ivi",
			"tags": Slice{"alpha", json.Number("7"), Map{"nested": "value"}},
		},
		Stats: stats,
	}
	for _, entry := range []snapshotEntry{stringEntry, mapEntry} {
		data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
		if err != nil {
			t.Fatalf("marshalLevelDBEntry(%s) error = %v", entry.Type, err)
		}
		if cap(data) != len(data) {
			t.Fatalf("binary %s record cap = %d, want exact len %d", entry.Type, cap(data), len(data))
		}
		decoded, err := decodeLevelDBEntry(data)
		if err != nil {
			t.Fatalf("decodeLevelDBEntry(%s) error = %v", entry.Type, err)
		}
		if decoded.Key != entry.Key || decoded.Type != entry.Type {
			t.Fatalf("decoded %s metadata = %#v, want key/type preserved", entry.Type, decoded)
		}
		if entry.Type == "string" && decoded.String != entry.String {
			t.Fatalf("decoded string length = %d, want %d", len(decoded.String), len(entry.String))
		}
		if entry.Type == "map" && !reflect.DeepEqual(decoded.Map, entry.Map) {
			t.Fatalf("decoded map = %#v, want %#v", decoded.Map, entry.Map)
		}
		if !keyStatsPtrEqual(decoded.Stats, entry.Stats) {
			t.Fatalf("decoded %s stats = %#v, want %#v", entry.Type, decoded.Stats, entry.Stats)
		}
	}
}

func TestLevelDBBinaryMapSliceSetUseBinaryValuePayloads(t *testing.T) {
	stringItems := make(Slice, 0, 12)
	for idx := 0; idx < 12; idx++ {
		stringItems = append(stringItems, fmt.Sprintf("line-%02d\nquoted\"value", idx))
	}
	tests := []struct {
		name  string
		entry snapshotEntry
	}{
		{
			name: "map",
			entry: snapshotEntry{
				Key:  "profile",
				Type: "map",
				Map: Map{
					"name":   "ivi",
					"age":    json.Number("32"),
					"active": true,
					"tags":   Slice{"alpha", "beta", "gamma"},
					"nested": Map{"role": "admin"},
				},
			},
		},
		{
			name: "slice",
			entry: snapshotEntry{
				Key:   "events",
				Type:  "slice",
				Slice: stringItems,
			},
		},
		{
			name: "set",
			entry: snapshotEntry{
				Key:  "tags",
				Type: "set",
				Set:  Set{"alpha", json.Number("7"), false, Map{"nested": "value"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := marshalLevelDBEntry(test.entry, StorageFormatBinary)
			if err != nil {
				t.Fatalf("marshalLevelDBEntry(%s) error = %v", test.name, err)
			}
			_, payload := levelDBBinaryValuePayloadForTest(t, data)
			if !snapshotValueDataIsBinary(payload) {
				t.Fatalf("%s payload header = % x, want binary snapshot value", test.name, payload[:shortHeaderLen(payload)])
			}

			decoded, err := decodeLevelDBEntry(data)
			if err != nil {
				t.Fatalf("decodeLevelDBEntry(%s) error = %v", test.name, err)
			}
			if decoded.Key != test.entry.Key || decoded.Type != test.entry.Type {
				t.Fatalf("decoded %s metadata = %#v, want key/type preserved", test.name, decoded)
			}
			switch test.entry.Type {
			case "map":
				if !reflect.DeepEqual(decoded.Map, test.entry.Map) {
					t.Fatalf("decoded map = %#v, want %#v", decoded.Map, test.entry.Map)
				}
			case "slice":
				if !reflect.DeepEqual(decoded.Slice, test.entry.Slice) {
					t.Fatalf("decoded slice = %#v, want %#v", decoded.Slice, test.entry.Slice)
				}
			case "set":
				if !reflect.DeepEqual(decoded.Set, test.entry.Set) {
					t.Fatalf("decoded set = %#v, want %#v", decoded.Set, test.entry.Set)
				}
			}
		})
	}
}

func TestLevelDBBinaryCollectionPayloadCanBeSmallerThanJSON(t *testing.T) {
	items := make(Slice, 0, 64)
	for idx := 0; idx < 64; idx++ {
		items = append(items, fmt.Sprintf("escaped\nstring\twith\"quotes\"-%02d", idx))
	}
	entry := snapshotEntry{Key: "items", Type: "slice", Slice: items}

	data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(binary slice) error = %v", err)
	}
	_, binaryPayload := levelDBBinaryValuePayloadForTest(t, data)
	jsonPayload, err := marshalSnapshotEntryValueJSON(entry)
	if err != nil {
		t.Fatalf("marshalSnapshotEntryValueJSON(slice) error = %v", err)
	}
	if len(binaryPayload) >= len(jsonPayload) {
		t.Fatalf("binary slice payload size = %d, want smaller than JSON payload %d", len(binaryPayload), len(jsonPayload))
	}
}

func TestLevelDBBinaryCollectionEncodesBytesAsBase64String(t *testing.T) {
	entry := snapshotEntry{
		Key:  "payload",
		Type: "map",
		Map:  Map{"bytes": []byte("value")},
	}
	data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(binary map with bytes) error = %v", err)
	}
	_, payload := levelDBBinaryValuePayloadForTest(t, data)
	if !snapshotValueDataIsBinary(payload) {
		t.Fatalf("map with bytes payload header = % x, want binary snapshot value", payload[:shortHeaderLen(payload)])
	}
	decoded, err := decodeLevelDBEntry(data)
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(binary map with bytes) error = %v", err)
	}
	if got := decoded.Map["bytes"]; got != base64.StdEncoding.EncodeToString([]byte("value")) {
		t.Fatalf("decoded bytes = %#v, want base64 JSON string", got)
	}
}

func TestLevelDBBinaryCollectionFallsBackToJSONForUnsupportedBinaryValue(t *testing.T) {
	entry := snapshotEntry{
		Key:  "payload",
		Type: "map",
		Map:  Map{"queue": PriorityQueue{{Priority: 1, Value: "job"}}},
	}
	data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(binary map with priority queue) error = %v", err)
	}
	_, payload := levelDBBinaryValuePayloadForTest(t, data)
	if snapshotValueDataIsBinary(payload) {
		t.Fatalf("map with priority queue payload header = % x, want JSON fallback", payload[:shortHeaderLen(payload)])
	}
	decoded, err := decodeLevelDBEntry(data)
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(JSON fallback map) error = %v", err)
	}
	queue, ok := decoded.Map["queue"].([]interface{})
	if !ok || len(queue) != 1 {
		t.Fatalf("decoded fallback queue = %#v, want one JSON array item", decoded.Map["queue"])
	}
	item, ok := queue[0].(map[string]interface{})
	if !ok || item["priority"] != json.Number("1") || item["value"] != "job" {
		t.Fatalf("decoded fallback queue item = %#v, want JSON object with priority/value", queue[0])
	}
}

func TestLevelDBBinaryCollectionStillReadsLegacyJSONPayload(t *testing.T) {
	entry := snapshotEntry{
		Key:  "legacy",
		Type: "map",
		Map:  Map{"name": "ivi", "age": json.Number("32")},
	}
	payload, err := marshalSnapshotEntryValueJSON(entry)
	if err != nil {
		t.Fatalf("marshalSnapshotEntryValueJSON(map) error = %v", err)
	}
	size, err := binaryLengthPrefixedSize(int64(len(payload)))
	if err != nil {
		t.Fatalf("binaryLengthPrefixedSize() error = %v", err)
	}
	capacity, err := levelDBBinaryRecordCapacityForValue(entry.Key, entry.Type, size, nil, nil)
	if err != nil {
		t.Fatalf("levelDBBinaryRecordCapacityForValue() error = %v", err)
	}
	writer := newLevelDBBinaryWriterWithCapacity(capacity)
	writer.writeString(entry.Key)
	writer.writeString(entry.Type)
	writer.writeBytes(payload)
	writer.writeTimePtr(nil)
	writer.writeKeyStatsPtr(nil)

	decoded, err := decodeLevelDBEntry(writer.bytes())
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(legacy inner JSON map) error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Map, entry.Map) {
		t.Fatalf("decoded legacy inner JSON map = %#v, want %#v", decoded.Map, entry.Map)
	}
}

func levelDBBinaryValuePayloadForTest(t *testing.T, data []byte) (string, []byte) {
	t.Helper()
	if !levelDBEntryDataIsBinary(data) {
		t.Fatalf("record header = % x, want binary LevelDB record", data[:shortHeaderLen(data)])
	}
	reader := newBinaryFieldReader(data[len(levelDBBinaryMagic):])
	if _, err := reader.readString(); err != nil {
		t.Fatalf("read key error = %v", err)
	}
	entryType, err := reader.readString()
	if err != nil {
		t.Fatalf("read type error = %v", err)
	}
	payload, err := reader.readBytes()
	if err != nil {
		t.Fatalf("read value payload error = %v", err)
	}
	return entryType, payload
}

func TestLevelDBBinaryBytesRecordAvoidsBase64StorageExpansion(t *testing.T) {
	raw := testPayload(256)
	entry := snapshotEntry{
		Key:   "blob",
		Type:  "bytes",
		Bytes: base64.StdEncoding.EncodeToString(raw),
	}
	jsonData, err := marshalLevelDBEntry(entry, StorageFormatJSON)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(json) error = %v", err)
	}
	binaryData, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		t.Fatalf("marshalLevelDBEntry(binary) error = %v", err)
	}
	if cap(binaryData) != len(binaryData) {
		t.Fatalf("binary bytes record cap = %d, want exact len %d", cap(binaryData), len(binaryData))
	}
	if !levelDBEntryDataIsBinary(binaryData) {
		t.Fatalf("binary record header = % x, want binary", binaryData[:shortHeaderLen(binaryData)])
	}
	if len(binaryData) >= len(jsonData) {
		t.Fatalf("binary record size = %d, want smaller than JSON size %d", len(binaryData), len(jsonData))
	}
	decoded, err := decodeLevelDBEntry(binaryData)
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(binary) error = %v", err)
	}
	if decoded.Bytes != "" {
		t.Fatalf("decoded binary bytes field length = %d, want lazy raw bytes", len(decoded.Bytes))
	}
	value, err := snapshotEntryBytesValue(decoded)
	if err != nil {
		t.Fatalf("snapshotEntryBytesValue(decoded bytes) error = %v", err)
	}
	if decoded.Key != "blob" || decoded.Type != "bytes" || !bytes.Equal(value, raw) {
		t.Fatalf("decoded binary entry = %#v bytes %v, want original", decoded, value)
	}
}

func TestLevelDBBinaryRawBytesMaterializeAtJSONBoundary(t *testing.T) {
	raw := testPayload(257)
	entry := snapshotEntry{
		Key:      "blob",
		Type:     "bytes",
		rawBytes: raw,
	}

	data, err := marshalSnapshotEntryJSON(entry)
	if err != nil {
		t.Fatalf("marshalSnapshotEntryJSON(raw bytes) error = %v", err)
	}
	if !bytes.Contains(data, []byte(base64.StdEncoding.EncodeToString(raw))) {
		t.Fatalf("raw bytes JSON = %s, want base64 bytes field", data)
	}

	operation, err := prepareSnapshotBytesOperation(entry)
	if err != nil {
		t.Fatalf("prepareSnapshotBytesOperation(raw bytes) error = %v", err)
	}
	if operation.entry.Bytes != "" {
		t.Fatalf("operation entry Bytes length = %d, want raw-only binary bytes", len(operation.entry.Bytes))
	}
	if !bytes.Equal(operation.bytes, raw) {
		t.Fatalf("operation bytes changed")
	}
	if shouldStreamSnapshotBytes(operation) {
		t.Fatal("raw binary bytes should not use base64 streaming path")
	}
}

func TestMarshalLevelDBBytesEntryBinaryFromReaderPreallocatesRecord(t *testing.T) {
	raw := testPayload(DiskBytesThreshold + 257)
	expiresAt := time.Unix(1234, 5678)
	stats := &KeyStats{
		Reads:             7,
		Hits:              5,
		Misses:            2,
		Writes:            3,
		LastHit:           time.Unix(1235, 0),
		LastMiss:          time.Unix(1236, 0),
		LastWrite:         time.Unix(1237, 0),
		HitRate:           0.75,
		CumulativeHitRate: 0.625,
	}

	data, err := marshalLevelDBBytesEntryBinaryFromReader("blob", int64(len(raw)), bytes.NewReader(raw), &expiresAt, stats)
	if err != nil {
		t.Fatalf("marshalLevelDBBytesEntryBinaryFromReader() error = %v", err)
	}
	if cap(data) != len(data) {
		t.Fatalf("binary record cap = %d, want exact len %d", cap(data), len(data))
	}

	decoded, err := decodeLevelDBEntry(data)
	if err != nil {
		t.Fatalf("decodeLevelDBEntry(reader binary) error = %v", err)
	}
	if decoded.Bytes != "" {
		t.Fatalf("decoded reader binary bytes field length = %d, want lazy raw bytes", len(decoded.Bytes))
	}
	value, err := snapshotEntryBytesValue(decoded)
	if err != nil {
		t.Fatalf("snapshotEntryBytesValue(reader binary bytes) error = %v", err)
	}
	if decoded.Key != "blob" || decoded.Type != "bytes" || !bytes.Equal(value, raw) {
		t.Fatalf("decoded reader binary entry = %#v bytes %v, want original", decoded, value)
	}
	if decoded.ExpiresAt == nil || !decoded.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("decoded expires_at = %v, want %v", decoded.ExpiresAt, expiresAt)
	}
	if !keyStatsPtrEqual(decoded.Stats, stats) {
		t.Fatalf("decoded stats = %#v, want %#v", decoded.Stats, stats)
	}
}

func TestLevelDBStoreEntryMaterializesBinaryBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	raw := testPayload(256)
	source := newTestTrie(t)
	source.UpsertBytes("blob", raw)
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	entry, ok, err := store.Entry("blob")
	if err != nil || !ok {
		t.Fatalf("Entry(blob) = %#v/%v/%v, want stored entry", entry, ok, err)
	}
	if entry.Bytes == "" || entry.rawBytes != nil {
		t.Fatalf("Entry(blob) bytes = %d raw=%d, want materialized base64 only", len(entry.Bytes), len(entry.rawBytes))
	}
	decoded, err := base64.StdEncoding.DecodeString(entry.Bytes)
	if err != nil {
		t.Fatalf("DecodeString(Entry bytes) error = %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("Entry(blob) bytes changed")
	}
}

func TestMarshalLevelDBBytesEntryBinaryFromReaderRequiresExactSize(t *testing.T) {
	raw := []byte("payload")
	if _, err := marshalLevelDBBytesEntryBinaryFromReader("blob", int64(len(raw)+1), bytes.NewReader(raw), nil, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("marshalLevelDBBytesEntryBinaryFromReader(short) error = %v, want unexpected EOF", err)
	}
	if _, err := marshalLevelDBBytesEntryBinaryFromReader("blob", int64(len(raw)-1), bytes.NewReader(raw), nil, nil); err == nil {
		t.Fatal("marshalLevelDBBytesEntryBinaryFromReader(long) error = nil, want size mismatch")
	}
	if _, err := levelDBBinaryRecordCapacity("blob", "bytes", int64(int(^uint(0)>>1)), nil, nil); !errors.Is(err, errLevelDBBinaryRecordTooLarge) {
		t.Fatalf("levelDBBinaryRecordCapacity(too large) error = %v, want too large", err)
	}
}

func shortHeaderLen(data []byte) int {
	if len(data) < len(levelDBBinaryMagic) {
		return len(data)
	}
	return len(levelDBBinaryMagic)
}

func TestLevelDBStoreRoundTripRestoresValuesAndTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4000, 0)
	source.now = func() time.Time { return now }

	source.UpsertCounter("counter", -7)
	source.UpsertString("string", "value")
	source.UpsertBytes("bytes", []byte{0, 1, 2, 3})
	source.UpsertMap("map", Map{"name": "ivi", "age": json.Number("32")})
	source.UpsertSlice("slice", Slice{"a", json.Number("2")})
	source.UpsertSet("set", Set{"a", json.Number("2"), "a"})
	source.UpsertPriorityQueue("priority", PriorityQueue{{Priority: 5, Value: json.Number("2")}, {Priority: 1, Value: "urgent"}})
	if err := source.UpsertBloomFilter("bloom", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	source.AddBloomFilter("bloom", "alpha", "beta")
	if err := source.UpsertCuckooFilter("cuckoo", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	source.AddCuckooFilter("cuckoo", "alpha", "beta")
	if err := source.UpsertXorFilter("xor", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	if _, err := source.AddXorFilter("xor", "alpha", "beta"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := source.BuildXorFilter("xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %v/%v, want ok", err, ok)
	}
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "user:100/profile", Map{"status": "active"})
	source.PutRadixTree("radix", "user:101/profile", json.Number("42"))
	source.UpsertRoaringBitmap("bitmap")
	source.AddRoaringBitmap("bitmap", 1, 1<<16+7)
	source.UpsertSparseBitset("bitset")
	source.AddSparseBitset("bitset", 1, 1<<32+7, ^uint64(0))
	source.UpsertString("ttl", "alive")
	if !source.Expire("ttl", time.Minute) {
		t.Fatal("Expire(ttl) = false, want true")
	}

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(10 * time.Second) }
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 14 {
		t.Fatalf("loaded count = %d, want 14", count)
	}

	if got := loaded.GetCounter("counter"); got != -7 {
		t.Fatalf("counter = %d, want -7", got)
	}
	if got := loaded.GetString("string"); got != "value" {
		t.Fatalf("string = %q, want value", got)
	}
	if got := loaded.GetBytes("bytes"); !bytes.Equal(got, []byte{0, 1, 2, 3}) {
		t.Fatalf("bytes = %v, want [0 1 2 3]", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"name": "ivi", "age": json.Number("32")}) {
		t.Fatalf("map = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"a", json.Number("2")}) {
		t.Fatalf("slice = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"a", json.Number("2")}) {
		t.Fatalf("set = %#v, want preserved json.Number", got)
	}
	if got := loaded.GetPriorityQueue("priority"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 1, Value: "urgent"}, {Priority: 5, Value: json.Number("2")}}) {
		t.Fatalf("priority queue = %#v, want restored priority order", got)
	}
	if !loaded.HasBloomFilter("bloom", "alpha") || !loaded.HasBloomFilter("bloom", "beta") {
		t.Fatal("loaded Bloom filter does not contain inserted values")
	}
	if !loaded.HasCuckooFilter("cuckoo", "alpha") || !loaded.HasCuckooFilter("cuckoo", "beta") {
		t.Fatal("loaded Cuckoo filter does not contain inserted values")
	}
	if hit, queryable := loaded.HasXorFilter("xor", "alpha"); !queryable || !hit {
		t.Fatalf("loaded XOR filter alpha = %v/%v, want hit", hit, queryable)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:100/profile"); !ok || !reflect.DeepEqual(value, Map{"status": "active"}) {
		t.Fatalf("loaded radix user:100/profile = %#v/%v, want restored nested value", value, ok)
	}
	if value, ok := loaded.GetRadixTree("radix", "user:101/profile"); !ok || value != json.Number("42") {
		t.Fatalf("loaded radix user:101/profile = %#v/%v, want restored json.Number", value, ok)
	}
	if got := loaded.GetRoaringBitmap("bitmap"); !reflect.DeepEqual(got, []uint32{1, 1<<16 + 7}) {
		t.Fatalf("loaded Roaring bitmap = %#v, want restored integer set", got)
	}
	if got := loaded.GetSparseBitset("bitset"); !reflect.DeepEqual(got, []uint64{1, 1<<32 + 7, ^uint64(0)}) {
		t.Fatalf("loaded sparse bitset = %#v, want restored uint64 set", got)
	}
	if got := loaded.TTL("ttl"); got <= 0 || got > time.Minute {
		t.Fatalf("ttl = %s, want remaining positive TTL", got)
	}
}

func TestLevelDBStoreSaveStreamsOnDiskBytesWithMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4010, 0)
	expiresAt := now.Add(time.Hour)
	source.now = func() time.Time { return now }
	payload := testPayload(DiskBytesThreshold + 1)

	source.UpsertBytes("large", payload)
	if got := source.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("GetBytes(large) changed before save")
	}
	if !source.ExpireAt("large", expiresAt) {
		t.Fatal("ExpireAt(large) = false, want true")
	}
	if hval := source.Get("large"); !hval.OnDisk() {
		t.Fatalf("large before save = %+v, want on-disk bytes", hval)
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	entry, ok, err := store.Entry("large")
	if err != nil || !ok {
		t.Fatalf("Entry(large) = %#v/%v/%v, want saved entry", entry, ok, err)
	}
	if entry.Type != "bytes" || entry.ExpiresAt == nil || !entry.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("saved entry metadata = %#v, want bytes with TTL", entry)
	}
	if entry.Stats == nil || entry.Stats.Reads == 0 || entry.Stats.Writes == 0 {
		t.Fatalf("saved entry stats = %#v, want persisted read/write stats", entry.Stats)
	}
	decoded, err := base64.StdEncoding.DecodeString(entry.Bytes)
	if err != nil {
		t.Fatalf("DecodeString(saved bytes) error = %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("saved bytes changed")
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if count, err := store.Load(loaded); err != nil || count != 1 {
		t.Fatalf("Load() = %d/%v, want one loaded value", count, err)
	}
	if hval := loaded.Get("large"); !hval.OnDisk() || !hval.HasTtl() {
		t.Fatalf("loaded large = %+v, want on-disk bytes with TTL", hval)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("loaded large bytes changed")
	}
}

func TestLevelDBStoreSaveScansLargeHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	large := strings.Repeat("x", 70*1024)
	for idx := 0; idx < 64; idx++ {
		source.UpsertString(fmt.Sprintf("key:%03d", idx), large)
	}

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 64 {
		t.Fatalf("LoadLevelDB() count = %d, want 64", count)
	}
	if got := loaded.GetString("key:063"); got != large {
		t.Fatalf("loaded large value length = %d, want %d", len(got), len(large))
	}
}

func TestLevelDBLoadUsesSingleExpirationClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	base := time.Unix(4050, 0)

	source := newTestTrie(t)
	source.now = func() time.Time { return base }
	source.UpsertString("soon", "value")
	if !source.Expire("soon", time.Second) {
		t.Fatal("Expire(soon) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	clockReads := 0
	loaded.now = func() time.Time {
		clockReads++
		if clockReads == 1 {
			return base
		}
		return base.Add(2 * time.Second)
	}
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("LoadLevelDB() count = %d, want 1", count)
	}
	if clockReads != 1 {
		t.Fatalf("LoadLevelDB clock reads = %d, want one captured load time", clockReads)
	}
	loaded.now = func() time.Time { return base }
	if got := loaded.GetString("soon"); got != "value" {
		t.Fatalf("soon after LevelDB load = %q, want value", got)
	}
}

func TestLoadLevelDBRemovesKeysMissingFromStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("keep", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.UpsertString("stale", "old")
	loaded.UpsertBytes("stale-large", testPayload(DiskBytesThreshold+1))
	staleValue := loaded.Get("stale-large")
	stalePath := loaded.disks.paths[staleValue.Index]
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("LoadLevelDB() count = %d, want 1", count)
	}
	if got := loaded.GetString("keep"); got != "value" {
		t.Fatalf("keep after LevelDB load = %q, want value", got)
	}
	if got := loaded.GetString("stale"); got != "" {
		t.Fatalf("stale after LevelDB load = %q, want empty", got)
	}
	if got := loaded.GetBytes("stale-large"); got != nil {
		t.Fatalf("stale-large after LevelDB load = %q, want nil", got)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale disk file Stat() error = %v, want not exist", err)
	}
}

func TestLoadLevelDBRemovesKeysExpiredInStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	base := time.Unix(4525, 0)
	source := newTestTrie(t)
	source.now = func() time.Time { return base }
	source.UpsertString("expired", "from-store")
	source.UpsertString("keep", "value")
	if !source.Expire("expired", time.Second) {
		t.Fatal("Expire(expired) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return base.Add(2 * time.Second) }
	loaded.UpsertString("expired", "old")
	loaded.UpsertString("missing", "old")
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("LoadLevelDB() count = %d, want 1", count)
	}
	if got := loaded.GetString("keep"); got != "value" {
		t.Fatalf("keep after LevelDB load = %q, want value", got)
	}
	if loaded.Exists("expired") {
		t.Fatal("expired store key survived LevelDB load")
	}
	if loaded.Exists("missing") {
		t.Fatal("missing store key survived LevelDB load")
	}
}

func TestLoadLevelDBCleansCreatedKeysAfterApplyFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	payload := testPayload(DiskBytesThreshold + 1)
	source := newTestTrie(t)
	source.UpsertBytes("a-created", payload)
	source.UpsertBytes("b-blocked", payload)
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.UpsertString("existing", "keep")
	firstPath := loaded.disks.pathFor(0)
	blockedPath := loaded.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if _, err := loaded.LoadLevelDB(path); err == nil {
		t.Fatal("LoadLevelDB() error = nil, want blocked disk write error")
	}
	if loaded.Exists("a-created") {
		t.Fatal("failed LevelDB load left created key a-created")
	}
	if loaded.Exists("b-blocked") {
		t.Fatal("failed LevelDB load left blocked key b-blocked")
	}
	if got := loaded.GetString("existing"); got != "keep" {
		t.Fatalf("existing after failed LevelDB load = %q, want keep", got)
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("created disk file Stat() error = %v, want not exist", err)
	}
	if got := len(loaded.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed LevelDB load = %d, want 0", got)
	}
}

func TestLoadLevelDBRollsBackExistingKeysAfterApplyFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	payload := testPayload(DiskBytesThreshold + 1)
	source := newTestTrie(t)
	source.UpsertBytes("a-existing", payload)
	source.UpsertBytes("b-blocked", payload)
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.UpsertString("a-existing", "keep")
	firstPath := loaded.disks.pathFor(0)
	blockedPath := loaded.disks.pathFor(1)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatalf("Mkdir(blocked path) error = %v", err)
	}

	if _, err := loaded.LoadLevelDB(path); err == nil {
		t.Fatal("LoadLevelDB() error = nil, want blocked disk write error")
	}
	if got := loaded.GetString("a-existing"); got != "keep" {
		t.Fatalf("a-existing after failed LevelDB load = %q, want keep", got)
	}
	if loaded.Exists("b-blocked") {
		t.Fatal("failed LevelDB load left blocked key b-blocked")
	}
	if _, err := os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("rolled-back disk file Stat() error = %v, want not exist", err)
	}
	if got := len(loaded.disks.paths); got != 0 {
		t.Fatalf("disk paths after failed LevelDB rollback = %d, want 0", got)
	}
}

func TestLevelDBStoreRoundTripPreservesBlankKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("", "empty")
	source.UpsertString(" ", "space")
	source.UpsertString("\t", "tab")

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 3 {
		t.Fatalf("loaded count = %d, want 3", count)
	}

	for key, want := range map[string]string{
		"":   "empty",
		" ":  "space",
		"\t": "tab",
	} {
		if got := loaded.GetString(key); got != want {
			t.Fatalf("GetString(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestLevelDBStoreRoundTripRestoresKeyStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4500, 0)
	source.now = func() time.Time { return now }

	source.UpsertBytes("hot", []byte("value"))
	now = now.Add(time.Second)
	if got := source.GetBytes("hot"); !bytes.Equal(got, []byte("value")) {
		t.Fatalf("GetBytes(hot) = %q, want value", got)
	}
	now = now.Add(time.Second)
	if got := source.GetMap("hot"); got != nil {
		t.Fatalf("GetMap(hot) = %#v, want nil", got)
	}
	want, ok := source.StatsForKey("hot")
	if !ok {
		t.Fatal("StatsForKey(hot) = false, want true")
	}

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(time.Hour) }
	if _, err := loaded.LoadLevelDB(path); err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	got, ok := loaded.StatsForKey("hot")
	if !ok {
		t.Fatal("loaded StatsForKey(hot) = false, want true")
	}
	if got != want {
		t.Fatalf("loaded key stats = %#v, want %#v", got, want)
	}
}

func TestSnapshotOperationValueSizeSupportsPriorityQueue(t *testing.T) {
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type: "priority_queue",
			PriorityQueue: []priorityQueueItem{
				{Priority: 1, Sequence: 0, Value: "urgent"},
				{Priority: 5, Sequence: 1, Value: json.Number("2")},
			},
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(priority_queue) error = %v", err)
	}
	if size == 0 {
		t.Fatal("snapshotOperationValueSize(priority_queue) = 0, want encoded size")
	}
}

func TestSnapshotOperationValueSizeSupportsStreamingBytes(t *testing.T) {
	payload := testPayload(DiskBytesThreshold + 1)
	operation, err := prepareSnapshotBytesOperation(snapshotEntry{
		Key:   "large",
		Type:  "bytes",
		Bytes: base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		t.Fatalf("prepareSnapshotBytesOperation() error = %v", err)
	}
	if operation.bytes != nil {
		t.Fatalf("operation decoded %d bytes, want streaming bytes", len(operation.bytes))
	}
	size, err := snapshotOperationValueSize(operation)
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(streaming bytes) error = %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("snapshotOperationValueSize(streaming bytes) = %d, want %d", size, len(payload))
	}
}

func TestValidatedBase64DecodedSize(t *testing.T) {
	for _, tt := range []struct {
		name    string
		encoded string
		want    int64
	}{
		{name: "empty", encoded: "", want: 0},
		{name: "single padding", encoded: base64.StdEncoding.EncodeToString([]byte("ab")), want: 2},
		{name: "double padding", encoded: base64.StdEncoding.EncodeToString([]byte("a")), want: 1},
		{name: "no padding", encoded: base64.StdEncoding.EncodeToString([]byte("abc")), want: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validatedBase64DecodedSize(tt.encoded)
			if err != nil {
				t.Fatalf("validatedBase64DecodedSize(%s) error = %v", tt.name, err)
			}
			if got != tt.want {
				t.Fatalf("validatedBase64DecodedSize(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}

	for _, encoded := range []string{"!!!!", "abc"} {
		if _, err := validatedBase64DecodedSize(encoded); err == nil {
			t.Fatalf("validatedBase64DecodedSize(%q) error = nil, want invalid base64 error", encoded)
		}
	}
}

func TestSnapshotOperationValueSizeSupportsBloomFilter(t *testing.T) {
	filter, err := newBloomFilterData(100, 0.01)
	if err != nil {
		t.Fatalf("newBloomFilterData() error = %v", err)
	}
	snapshot := filter.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:        "bloom_filter",
			BloomFilter: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(bloom_filter) error = %v", err)
	}
	if size != filter.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(bloom_filter) = %d, want %d", size, filter.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsCuckooFilter(t *testing.T) {
	filter, err := newCuckooFilterData(100, 0.01)
	if err != nil {
		t.Fatalf("newCuckooFilterData() error = %v", err)
	}
	snapshot := filter.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:         "cuckoo_filter",
			CuckooFilter: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(cuckoo_filter) error = %v", err)
	}
	if size != filter.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(cuckoo_filter) = %d, want %d", size, filter.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsRoaringBitmap(t *testing.T) {
	bitmap := newRoaringBitmapData()
	for idx := 0; idx <= roaringBitmapArrayMaxSize; idx++ {
		bitmap.Add(uint32(idx))
	}
	snapshot := bitmap.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:          "roaring_bitmap",
			RoaringBitmap: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(roaring_bitmap) error = %v", err)
	}
	if size != bitmap.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(roaring_bitmap) = %d, want %d", size, bitmap.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsSparseBitset(t *testing.T) {
	bitset := newSparseBitsetData()
	for idx := 0; idx <= sparseBitsetArrayMaxSize; idx++ {
		bitset.Add(uint64(idx))
	}
	snapshot := bitset.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:         "sparse_bitset",
			SparseBitset: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(sparse_bitset) error = %v", err)
	}
	if size != bitset.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(sparse_bitset) = %d, want %d", size, bitset.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsCountMinSketch(t *testing.T) {
	sketch, err := newCountMinSketchData(128, 4)
	if err != nil {
		t.Fatalf("newCountMinSketchData() error = %v", err)
	}
	snapshot := sketch.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:           "count_min_sketch",
			CountMinSketch: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(count_min_sketch) error = %v", err)
	}
	if size != sketch.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(count_min_sketch) = %d, want %d", size, sketch.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsHyperLogLog(t *testing.T) {
	hll, err := newHyperLogLogData(10)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	snapshot := hll.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:        "hyperloglog",
			HyperLogLog: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(hyperloglog) error = %v", err)
	}
	if size != hll.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(hyperloglog) = %d, want %d", size, hll.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsTopK(t *testing.T) {
	top, err := newTopKData(3)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	top.Add("alpha", 5)
	snapshot := top.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type: "top_k",
			TopK: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(top_k) error = %v", err)
	}
	if size != top.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(top_k) = %d, want %d", size, top.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsReservoirSample(t *testing.T) {
	sample, err := newReservoirSampleData(3)
	if err != nil {
		t.Fatalf("newReservoirSampleData() error = %v", err)
	}
	sample.AddOne("alpha", "beta", "gamma", "delta")
	snapshot := sample.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:            "reservoir_sample",
			ReservoirSample: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(reservoir_sample) error = %v", err)
	}
	if size != sample.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(reservoir_sample) = %d, want %d", size, sample.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsXorFilter(t *testing.T) {
	filter, err := newXorFilterData(8)
	if err != nil {
		t.Fatalf("newXorFilterData() error = %v", err)
	}
	if _, err := filter.AddOne("alpha", "beta"); err != nil {
		t.Fatalf("AddOne() error = %v", err)
	}
	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	snapshot := filter.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:      "xor_filter",
			XorFilter: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(xor_filter) error = %v", err)
	}
	if size != filter.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(xor_filter) = %d, want %d", size, filter.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsRadixTree(t *testing.T) {
	tree := newRadixTreeData()
	tree.Put("user:100/profile", Map{"status": "active"})
	tree.Put("user:101/profile", json.Number("42"))
	snapshot := tree.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:      "radix_tree",
			RadixTree: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(radix_tree) error = %v", err)
	}
	want, err := newRadixTreeSizeFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("newRadixTreeSizeFromSnapshot() error = %v", err)
	}
	if size != want {
		t.Fatalf("snapshotOperationValueSize(radix_tree) = %d, want %d", size, want)
	}
}

func TestSnapshotOperationValueSizeSupportsQuantileSketch(t *testing.T) {
	sketch, err := newQuantileSketchData(0.01)
	if err != nil {
		t.Fatalf("newQuantileSketchData() error = %v", err)
	}
	sketch.Add(10, 20, 30)
	snapshot := sketch.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:           "quantile_sketch",
			QuantileSketch: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(quantile_sketch) error = %v", err)
	}
	if size != sketch.EncodedSize() {
		t.Fatalf("snapshotOperationValueSize(quantile_sketch) = %d, want %d", size, sketch.EncodedSize())
	}
}

func TestSnapshotOperationValueSizeSupportsFenwickTree(t *testing.T) {
	tree, err := newFenwickTreeData(8)
	if err != nil {
		t.Fatalf("newFenwickTreeData() error = %v", err)
	}
	tree.Add(2, 5)
	tree.Add(6, 7)
	snapshot := tree.Snapshot()
	size, err := snapshotOperationValueSize(snapshotOperation{
		entry: snapshotEntry{
			Type:        "fenwick_tree",
			FenwickTree: &snapshot,
		},
	})
	if err != nil {
		t.Fatalf("snapshotOperationValueSize(fenwick_tree) error = %v", err)
	}
	if size != int64(len(snapshot.Tree)*8) {
		t.Fatalf("snapshotOperationValueSize(fenwick_tree) = %d, want %d", size, len(snapshot.Tree)*8)
	}
}

func TestLevelDBShouldHotLoadRejectsNegativeLimits(t *testing.T) {
	now := time.Unix(4600, 0)
	operation := snapshotOperation{
		entry: snapshotEntry{
			Type:   "string",
			String: "hot",
			Stats: &KeyStats{
				Hits:    1,
				LastHit: now,
			},
		},
	}
	policy := LevelDBLoadPolicy{
		HotValuesOnly: true,
		MaxValueBytes: 1024,
		MaxLastHitAge: time.Hour,
		MinHits:       1,
	}

	if !levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(valid policy) = false, want true")
	}

	policy.MaxValueBytes = -1
	if levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(negative max bytes) = true, want false")
	}

	policy.MaxValueBytes = 1024
	policy.MaxLastHitAge = -time.Second
	if levelDBShouldHotLoad(operation, now, policy) {
		t.Fatal("levelDBShouldHotLoad(negative max age) = true, want false")
	}
}

func TestLevelDBStoreHotLoadKeepsColdReferencesAndHydratesOnAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4600, 0)
	source.now = func() time.Time { return now }

	source.UpsertString("cold", "cold-value")
	source.UpsertString("hot", "hot-value")
	source.UpsertString("large-hot", string(bytes.Repeat([]byte("x"), 2048)))
	policy := DefaultLevelDBHotLoadPolicy()
	for i := uint64(0); i < policy.MinHits; i++ {
		if got := source.GetString("hot"); got != "hot-value" {
			t.Fatalf("GetString(hot) = %q, want hot-value", got)
		}
		if got := source.GetString("large-hot"); got == "" {
			t.Fatal("GetString(large-hot) = empty, want value")
		}
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(30 * time.Minute) }
	result, err := store.LoadWithPolicy(loaded, policy)
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 3 || result.ValuesLoaded != 1 {
		t.Fatalf("hot-load result = %#v, want 3 keys and 1 value", result)
	}
	valuesByKey := map[string]HatValue{}
	for _, entry := range loaded.Entries(true) {
		valuesByKey[entry.Key] = entry.Value
	}
	if !valuesByKey["hot"].IsStringAtRaws() {
		t.Fatalf("hot value = %+v, want in-memory string", valuesByKey["hot"])
	}
	if !valuesByKey["cold"].IsLevelDBReference() {
		t.Fatalf("cold value = %+v, want leveldb reference", valuesByKey["cold"])
	}
	if !valuesByKey["large-hot"].IsLevelDBReference() {
		t.Fatalf("large-hot value = %+v, want leveldb reference", valuesByKey["large-hot"])
	}
	if !loaded.Exists("cold") {
		t.Fatal("Exists(cold) = false, want true")
	}
	for _, entry := range loaded.Entries(true) {
		if entry.Key == "cold" && !entry.Value.IsLevelDBReference() {
			t.Fatalf("Exists(cold) hydrated value to %+v, want leveldb reference", entry.Value)
		}
	}

	if got := loaded.GetString("cold"); got != "cold-value" {
		t.Fatalf("hydrated cold value = %q, want cold-value", got)
	}
	if hval := loaded.Get("cold"); !hval.IsStringAtRaws() {
		t.Fatalf("cold after hydration = %+v, want in-memory string", hval)
	}
	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(after hot-load) error = %v", err)
	}

	roundTrip := newTestTrie(t)
	if count, err := store.Load(roundTrip); err != nil || count != 3 {
		t.Fatalf("Load(roundTrip) = %d/%v, want 3 nil", count, err)
	}
	if got := roundTrip.GetString("large-hot"); got == "" {
		t.Fatal("large-hot was not preserved after saving hot-loaded trie")
	}
}

func TestLevelDBStoreHotLoadKeepsLargeBytesColdWithoutMaterializing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4625, 0)
	source.now = func() time.Time { return now }
	payload := testPayload(70 * 1024)
	source.UpsertBytes("large", payload)
	policy := DefaultLevelDBHotLoadPolicy()
	for i := uint64(0); i < policy.MinHits; i++ {
		if got := source.GetBytes("large"); !bytes.Equal(got, payload) {
			t.Fatalf("GetBytes(large) changed before save")
		}
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(time.Minute) }
	result, err := store.LoadWithPolicy(loaded, policy)
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want cold large bytes reference only", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || entries[0].Key != "large" || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want large leveldb reference", entries)
	}
	if got := len(loaded.disks.paths); got != 0 {
		t.Fatalf("disk paths before hydration = %d, want 0", got)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("hydrated large bytes changed")
	}
	if hval := loaded.Get("large"); !hval.IsBytesAtRaws() || !hval.OnDisk() {
		t.Fatalf("large value after hydration = %+v, want on-disk bytes", hval)
	}
}

func TestLevelDBStoreHotLoadRejectsCorruptColdBytesWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	good := snapshotEntry{Key: "a-good", Type: "string", String: "value"}
	goodData, err := json.Marshal(good)
	if err != nil {
		t.Fatalf("Marshal(good entry) error = %v", err)
	}
	bad := snapshotEntry{Key: "z-bad", Type: "bytes", Bytes: "not-base64!!!"}
	badData, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("Marshal(bad entry) error = %v", err)
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		t.Fatalf("lockDB() error = %v", err)
	}
	if err := db.Put(levelDBKey(good.Key), goodData, nil); err != nil {
		unlock()
		t.Fatalf("db.Put(good) error = %v", err)
	}
	if err := db.Put(levelDBKey(bad.Key), badData, nil); err != nil {
		unlock()
		t.Fatalf("db.Put(bad) error = %v", err)
	}
	unlock()

	loaded := newTestTrie(t)
	loaded.UpsertString("existing", "keep")
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err == nil {
		t.Fatal("LoadWithPolicy(corrupt cold bytes) error = nil, want base64 error")
	}
	if got := loaded.GetString("existing"); got != "keep" {
		t.Fatalf("existing after rejected load = %q, want keep", got)
	}
	if loaded.Exists("a-good") {
		t.Fatal("rejected load created earlier valid key")
	}
	if loaded.Exists("z-bad") {
		t.Fatal("corrupt cold bytes created bad key")
	}
}

func TestLevelDBStoreCloseIsIdempotentAndRejectsOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("key", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	loaded := newTestTrie(t)
	if err := store.Save(loaded); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Save(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := store.Load(loaded); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Load(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("LoadWithPolicy(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := store.Entry("key"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("Entry(closed) error = %v, want ErrLevelDBStoreClosed", err)
	}
}

func TestHydrateLevelDBReferencesAllowsClosingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "cold-value")
	source.UpsertMap("map", Map{"field": "value"})
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 2 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 2 keys and 0 values", result)
	}
	if got, err := loaded.HydrateLevelDBReferences(); err != nil || got != 2 {
		t.Fatalf("HydrateLevelDBReferences() = %d/%v, want 2/nil", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := loaded.GetString("cold"); got != "cold-value" {
		t.Fatalf("GetString(cold) after close = %q, want cold-value", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"field": "value"}) {
		t.Fatalf("GetMap(map) after close = %#v, want hydrated map", got)
	}
	path = filepath.Join(t.TempDir(), "snapshot.json")
	if err := loaded.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot(after close) error = %v", err)
	}
}

func TestLevelDBColdReferencesHydrateBeforeIncrementalMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 5)
	source.UpsertString("suffix", "old")
	source.UpsertString("prefix", "old")
	source.UpsertMap("map", Map{"old": "value"})
	source.PushSlice("slice", "old")
	source.PushPriorityQueue("queue", 5, "old")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 6 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 6 cold keys", result)
	}

	loaded.IncrementCounter("counter", 2)
	loaded.AppendString("suffix", "-new")
	loaded.PrependString("prefix", "new-")
	loaded.PutMap("map", "new", "value")
	loaded.PushSlice("slice", "new")
	loaded.PushPriorityQueue("queue", 1, "new")

	if got := loaded.GetCounter("counter"); got != 7 {
		t.Fatalf("IncrementCounter(cold ref) = %d, want 7", got)
	}
	if got := loaded.GetString("suffix"); got != "old-new" {
		t.Fatalf("AppendString(cold ref) = %q, want old-new", got)
	}
	if got := loaded.GetString("prefix"); got != "new-old" {
		t.Fatalf("PrependString(cold ref) = %q, want new-old", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"new": "value", "old": "value"}) {
		t.Fatalf("PutMap(cold ref) = %#v, want old and new fields", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"old", "new"}) {
		t.Fatalf("PushSlice(cold ref) = %#v, want old then new", got)
	}
	if got, ok := loaded.PopPriorityQueue("queue"); !ok || got.Value != "new" {
		t.Fatalf("first PopPriorityQueue(cold ref) = %#v/%v, want new", got, ok)
	}
	if got, ok := loaded.PopPriorityQueue("queue"); !ok || got.Value != "old" {
		t.Fatalf("second PopPriorityQueue(cold ref) = %#v/%v, want old", got, ok)
	}
}

func TestLevelDBColdReferencesHydrateBeforeCheckedMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 5)
	source.UpsertString("append", "old")
	source.UpsertString("prepend", "old")
	source.UpsertMap("map", Map{"old": "value"})
	source.PushSlice("slice", "old")
	source.UpsertSet("set", Set{"old"})
	source.AddBloomFilter("bloom", "old")
	source.AddCuckooFilter("cuckoo", "old")
	source.UpsertRoaringBitmap("rb")
	source.AddRoaringBitmap("rb", 1)
	source.UpsertSparseBitset("sb")
	source.AddSparseBitset("sb", 1)
	if err := source.UpsertFenwickTree("fw", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := source.AddFenwickTree("fw", 1, 5); !ok {
		t.Fatal("AddFenwickTree(seed) = false, want true")
	}
	if err := source.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	source.AddQuantileSketch("quantile", 10)
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "old", "value")
	source.PushPriorityQueue("queue", 5, "old")
	source.IncrementCountMinSketch("cms", "old", 2)
	source.AddHyperLogLog("hll", "old")
	source.AddTopK("top", "old", 2)
	source.AddReservoirSample("sample", "old")
	if _, err := source.AddXorFilter("xor", "old"); err != nil {
		t.Fatalf("AddXorFilter(old) error = %v", err)
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 19 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 19 cold keys", result)
	}

	if got, err := loaded.IncrementCounterChecked("counter", 2); err != nil || got != 7 {
		t.Fatalf("IncrementCounterChecked(cold ref) = %d/%v, want 7/nil", got, err)
	}
	if got, err := loaded.AppendStringChecked("append", "-new"); err != nil || got != "old-new" {
		t.Fatalf("AppendStringChecked(cold ref) = %q/%v, want old-new/nil", got, err)
	}
	if got, err := loaded.PrependStringChecked("prepend", "new-"); err != nil || got != "new-old" {
		t.Fatalf("PrependStringChecked(cold ref) = %q/%v, want new-old/nil", got, err)
	}
	if err := loaded.PutMapChecked("map", "new", "value"); err != nil {
		t.Fatalf("PutMapChecked(cold ref) error = %v, want nil", err)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"new": "value", "old": "value"}) {
		t.Fatalf("GetMap(after cold put) = %#v, want old and new", got)
	}
	if err := loaded.PushSliceChecked("slice", "new"); err != nil {
		t.Fatalf("PushSliceChecked(cold ref) error = %v, want nil", err)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"old", "new"}) {
		t.Fatalf("GetSlice(after cold push) = %#v, want old and new", got)
	}

	if info, ok, err := loaded.BloomFilterInfoChecked("bloom"); err != nil || !ok || info.Insertions != 1 {
		t.Fatalf("BloomFilterInfoChecked(cold ref) = %#v/%v/%v, want one insertion", info, ok, err)
	}
	if info, ok, err := loaded.CuckooFilterInfoChecked("cuckoo"); err != nil || !ok || info.Count != 1 {
		t.Fatalf("CuckooFilterInfoChecked(cold ref) = %#v/%v/%v, want one insertion", info, ok, err)
	}
	if info, ok, err := loaded.XorFilterInfoChecked("xor"); err != nil || !ok || info.Staged != 1 {
		t.Fatalf("XorFilterInfoChecked(cold ref) = %#v/%v/%v, want one staged item", info, ok, err)
	}

	if added, err := loaded.AddSetChecked("set", "new"); err != nil || added != 1 {
		t.Fatalf("AddSetChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"new", "old"}) {
		t.Fatalf("GetSet(after cold add) = %#v, want old and new", got)
	}

	if added, err := loaded.AddBloomFilterChecked("bloom", "new"); err != nil || added != 1 {
		t.Fatalf("AddBloomFilterChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	bloomInfo, ok := loaded.BloomFilterInfo("bloom")
	if !ok || bloomInfo.Insertions != 2 {
		t.Fatalf("BloomFilterInfo(after cold add) = %#v/%v, want two insertions", bloomInfo, ok)
	}
	if !loaded.HasBloomFilter("bloom", "old") || !loaded.HasBloomFilter("bloom", "new") {
		t.Fatal("Bloom filter cold add did not retain old and new values")
	}

	if added, err := loaded.AddCuckooFilterChecked("cuckoo", "new"); err != nil || added != 1 {
		t.Fatalf("AddCuckooFilterChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	cuckooInfo, ok := loaded.CuckooFilterInfo("cuckoo")
	if !ok || cuckooInfo.Count != 2 {
		t.Fatalf("CuckooFilterInfo(after cold add) = %#v/%v, want two items", cuckooInfo, ok)
	}
	if !loaded.HasCuckooFilter("cuckoo", "old") || !loaded.HasCuckooFilter("cuckoo", "new") {
		t.Fatal("Cuckoo filter cold add did not retain old and new values")
	}

	if added, err := loaded.AddRoaringBitmapChecked("rb", 2); err != nil || added != 1 {
		t.Fatalf("AddRoaringBitmapChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	if got := loaded.GetRoaringBitmap("rb"); !reflect.DeepEqual(got, []uint32{1, 2}) {
		t.Fatalf("GetRoaringBitmap(after cold add) = %#v, want old and new values", got)
	}

	if added, err := loaded.AddSparseBitsetChecked("sb", 2); err != nil || added != 1 {
		t.Fatalf("AddSparseBitsetChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	if got := loaded.GetSparseBitset("sb"); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("GetSparseBitset(after cold add) = %#v, want old and new values", got)
	}

	if update, ok, err := loaded.AddFenwickTreeChecked("fw", 2, 3); err != nil || !ok || update.Total != 8 {
		t.Fatalf("AddFenwickTreeChecked(cold ref) = %#v/%v/%v, want total 8", update, ok, err)
	}
	if got, ok := loaded.RangeSumFenwickTree("fw", 1, 2); !ok || got != 8 {
		t.Fatalf("RangeSumFenwickTree(after cold add) = %d/%v, want 8/true", got, ok)
	}

	if estimate, err := loaded.AddQuantileSketchChecked("quantile", 20); err != nil || estimate.Count != 2 {
		t.Fatalf("AddQuantileSketchChecked(cold ref) = %#v/%v, want count 2", estimate, err)
	}
	if estimate, ok := loaded.EstimateQuantileSketch("quantile", 0.5); !ok || estimate.Count != 2 || estimate.Value < 10 || estimate.Value > 20 {
		t.Fatalf("EstimateQuantileSketch(after cold add) = %#v/%v, want retained values", estimate, ok)
	}

	if added, err := loaded.PutRadixTreeChecked("radix", "new", "value"); err != nil || !added {
		t.Fatalf("PutRadixTreeChecked(cold ref) = %v/%v, want true/nil", added, err)
	}
	if oldValue, ok := loaded.GetRadixTree("radix", "old"); !ok || oldValue != "value" {
		t.Fatalf("GetRadixTree(old after cold put) = %#v/%v, want retained value", oldValue, ok)
	}
	if newValue, ok := loaded.GetRadixTree("radix", "new"); !ok || newValue != "value" {
		t.Fatalf("GetRadixTree(new after cold put) = %#v/%v, want new value", newValue, ok)
	}

	if added, err := loaded.PushPriorityQueueChecked("queue", 1, "new"); err != nil || added != 1 {
		t.Fatalf("PushPriorityQueueChecked(cold ref) = %d/%v, want 1/nil", added, err)
	}
	if got, ok := loaded.PopPriorityQueue("queue"); !ok || got.Value != "new" {
		t.Fatalf("first PopPriorityQueue(after cold push) = %#v/%v, want new", got, ok)
	}
	if got, ok := loaded.PopPriorityQueue("queue"); !ok || got.Value != "old" {
		t.Fatalf("second PopPriorityQueue(after cold push) = %#v/%v, want old", got, ok)
	}

	if estimate, err := loaded.IncrementCountMinSketchChecked("cms", "new", 3); err != nil || estimate < 3 {
		t.Fatalf("IncrementCountMinSketchChecked(cold ref) = %d/%v, want estimate at least 3", estimate, err)
	}
	oldCMS, ok := loaded.EstimateCountMinSketch("cms", "old")
	newCMS, newOK := loaded.EstimateCountMinSketch("cms", "new")
	if !ok || !newOK || oldCMS < 2 || newCMS < 3 {
		t.Fatalf("Count-min estimates after cold increment old=%d/%v new=%d/%v, want retained counts", oldCMS, ok, newCMS, newOK)
	}

	if estimate, err := loaded.AddHyperLogLogChecked("hll", "new"); err != nil || estimate < 2 {
		t.Fatalf("AddHyperLogLogChecked(cold ref) = %d/%v, want estimate at least 2", estimate, err)
	}
	hllInfo, ok := loaded.HyperLogLogInfo("hll")
	if !ok || hllInfo.Observations != 2 {
		t.Fatalf("HyperLogLogInfo(after cold add) = %#v/%v, want two observations", hllInfo, ok)
	}

	if estimate, err := loaded.AddTopKChecked("top", "new", 3); err != nil || !estimate.Tracked || estimate.Count != 3 {
		t.Fatalf("AddTopKChecked(cold ref) = %#v/%v, want new count 3", estimate, err)
	}
	oldTop := loaded.EstimateTopK("top", "old")
	if !oldTop.Tracked || oldTop.Count != 2 {
		t.Fatalf("EstimateTopK(old after cold add) = %#v, want retained count 2", oldTop)
	}
	topInfo, ok := loaded.TopKInfo("top")
	if !ok || topInfo.Total != 5 || topInfo.Tracked != 2 {
		t.Fatalf("TopKInfo(after cold add) = %#v/%v, want two tracked values total 5", topInfo, ok)
	}

	if update, err := loaded.AddReservoirSampleChecked("sample", "new"); err != nil || update.Seen != 2 || update.Tracked != 2 {
		t.Fatalf("AddReservoirSampleChecked(cold ref) = %#v/%v, want seen/tracked 2", update, err)
	}
	sampleValues := loaded.GetReservoirSample("sample")
	if len(sampleValues) != 2 {
		t.Fatalf("GetReservoirSample(after cold add) = %#v, want two items", sampleValues)
	}
	seenSample := map[interface{}]bool{}
	for _, item := range sampleValues {
		seenSample[item.Value] = true
	}
	if !seenSample["old"] || !seenSample["new"] {
		t.Fatalf("reservoir sample after cold add = %#v, want old and new", sampleValues)
	}

	if added, err := loaded.AddXorFilter("xor", "new"); err != nil || added != 1 {
		t.Fatalf("AddXorFilter(cold ref) = %d/%v, want 1/nil", added, err)
	}
	xorInfo, ok, err := loaded.BuildXorFilter("xor")
	if err != nil || !ok || xorInfo.Items != 2 {
		t.Fatalf("BuildXorFilter(after cold add) = %#v/%v/%v, want two items", xorInfo, ok, err)
	}
	oldXor, oldQueryable := loaded.HasXorFilter("xor", "old")
	newXor, newQueryable := loaded.HasXorFilter("xor", "new")
	if !oldQueryable || !newQueryable || !oldXor || !newXor {
		t.Fatalf("xor filter after cold add old=%v/%v new=%v/%v, want both queryable hits", oldXor, oldQueryable, newXor, newQueryable)
	}
}

func TestLevelDBColdReferencesOverwriteWithoutHydration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 5)
	source.UpsertString("string", "old")
	source.UpsertMap("map", Map{"old": "value"})
	source.PushSlice("slice", "old")
	source.UpsertSet("set", Set{"old"})
	source.PushPriorityQueue("queue", 5, "old")
	source.AddBloomFilter("bloom", "old")
	source.AddCuckooFilter("cuckoo", "old")
	if _, err := source.AddXorFilter("xor", "old"); err != nil {
		t.Fatalf("AddXorFilter(old) error = %v", err)
	}
	source.UpsertRoaringBitmap("rb")
	source.AddRoaringBitmap("rb", 1)
	source.UpsertSparseBitset("sb")
	source.AddSparseBitset("sb", 1)
	if err := source.UpsertFenwickTree("fw", 4); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := source.AddFenwickTree("fw", 1, 5); !ok {
		t.Fatal("AddFenwickTree(seed) = false, want true")
	}
	if err := source.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	source.AddQuantileSketch("quantile", 10)
	source.IncrementCountMinSketch("cms", "old", 2)
	source.AddHyperLogLog("hll", "old")
	source.AddTopK("top", "old", 2)
	source.AddReservoirSample("sample", "old")
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "old", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 18 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 18 cold keys", result)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	loaded.UpsertCounter("counter", 9)
	loaded.UpsertString("string", "new")
	loaded.UpsertMap("map", Map{"new": "value"})
	loaded.UpsertSlice("slice", Slice{"new"})
	if err := loaded.UpsertSetChecked("set", Set{"new"}); err != nil {
		t.Fatalf("UpsertSetChecked() error = %v", err)
	}
	loaded.UpsertPriorityQueue("queue", PriorityQueue{{Priority: 1, Value: "new"}})
	if err := loaded.UpsertBloomFilter("bloom", 100, DefaultBloomFilterFalsePositiveRate); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	if err := loaded.UpsertCuckooFilter("cuckoo", 100, DefaultCuckooFilterFalsePositiveRate); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	if err := loaded.UpsertXorFilter("xor", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	loaded.UpsertRoaringBitmap("rb")
	loaded.UpsertSparseBitset("sb")
	if err := loaded.UpsertFenwickTree("fw", 4); err != nil {
		t.Fatalf("UpsertFenwickTree(replace) error = %v", err)
	}
	if err := loaded.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch(replace) error = %v", err)
	}
	if err := loaded.UpsertCountMinSketch("cms", 32, 3); err != nil {
		t.Fatalf("UpsertCountMinSketch(replace) error = %v", err)
	}
	if err := loaded.UpsertHyperLogLog("hll", 8); err != nil {
		t.Fatalf("UpsertHyperLogLog(replace) error = %v", err)
	}
	if err := loaded.UpsertTopK("top", 2); err != nil {
		t.Fatalf("UpsertTopK(replace) error = %v", err)
	}
	if err := loaded.UpsertReservoirSample("sample", 2); err != nil {
		t.Fatalf("UpsertReservoirSample(replace) error = %v", err)
	}
	loaded.UpsertRadixTree("radix")

	if got := loaded.GetCounter("counter"); got != 9 {
		t.Fatalf("GetCounter(counter) = %d, want 9", got)
	}
	if got := loaded.GetString("string"); got != "new" {
		t.Fatalf("GetString(string) = %q, want new", got)
	}
	if got := loaded.GetMap("map"); !reflect.DeepEqual(got, Map{"new": "value"}) {
		t.Fatalf("GetMap(map) = %#v, want replacement map", got)
	}
	if got := loaded.GetSlice("slice"); !reflect.DeepEqual(got, Slice{"new"}) {
		t.Fatalf("GetSlice(slice) = %#v, want replacement slice", got)
	}
	if got := loaded.GetSet("set"); !reflect.DeepEqual(got, Set{"new"}) {
		t.Fatalf("GetSet(set) = %#v, want replacement set", got)
	}
	if got := loaded.GetPriorityQueue("queue"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 1, Value: "new"}}) {
		t.Fatalf("GetPriorityQueue(queue) = %#v, want replacement queue", got)
	}
	if info, ok := loaded.BloomFilterInfo("bloom"); !ok || info.Insertions != 0 {
		t.Fatalf("BloomFilterInfo(bloom) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.CuckooFilterInfo("cuckoo"); !ok || info.Count != 0 {
		t.Fatalf("CuckooFilterInfo(cuckoo) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.XorFilterInfo("xor"); !ok || info.Staged != 0 || info.Built {
		t.Fatalf("XorFilterInfo(xor) = %#v/%v, want empty unbuilt replacement", info, ok)
	}
	if count, ok := loaded.CountRoaringBitmap("rb"); !ok || count != 0 {
		t.Fatalf("CountRoaringBitmap(rb) = %d/%v, want empty replacement", count, ok)
	}
	if count, ok := loaded.CountSparseBitset("sb"); !ok || count != 0 {
		t.Fatalf("CountSparseBitset(sb) = %d/%v, want empty replacement", count, ok)
	}
	if info, ok := loaded.FenwickTreeInfo("fw"); !ok || info.Size != 4 || info.Total != 0 {
		t.Fatalf("FenwickTreeInfo(fw) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.QuantileSketchInfo("quantile"); !ok || info.Count != 0 {
		t.Fatalf("QuantileSketchInfo(quantile) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.CountMinSketchInfo("cms"); !ok || info.TotalCount != 0 {
		t.Fatalf("CountMinSketchInfo(cms) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.HyperLogLogInfo("hll"); !ok || info.Observations != 0 {
		t.Fatalf("HyperLogLogInfo(hll) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.TopKInfo("top"); !ok || info.Total != 0 || info.Capacity != 2 {
		t.Fatalf("TopKInfo(top) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.ReservoirSampleInfo("sample"); !ok || info.Seen != 0 || info.Capacity != 2 {
		t.Fatalf("ReservoirSampleInfo(sample) = %#v/%v, want empty replacement", info, ok)
	}
	if info, ok := loaded.RadixTreeInfo("radix"); !ok || info.Items != 0 {
		t.Fatalf("RadixTreeInfo(radix) = %#v/%v, want empty replacement", info, ok)
	}

	entries := loaded.Entries(true)
	if len(entries) != 18 {
		t.Fatalf("Entries(after replacement) len = %d, want 18", len(entries))
	}
	for _, entry := range entries {
		if entry.Value.IsLevelDBReference() {
			t.Fatalf("entry after replacement = %#v, want materialized value", entry)
		}
	}
}

func TestLevelDBClosedColdReferencesBlockLegacyIncrementalMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 5)
	source.UpsertString("append", "old")
	source.UpsertString("prepend", "old")
	source.UpsertMap("map", Map{"old": "value"})
	source.PushSlice("slice", "old")
	source.UpsertSet("set", Set{"old"})
	source.PushPriorityQueue("queue", 5, "old")
	source.UpsertRoaringBitmap("rb")
	source.AddRoaringBitmap("rb", 1)
	source.UpsertSparseBitset("sb")
	source.AddSparseBitset("sb", 1)
	if err := source.UpsertFenwickTree("fw", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := source.AddFenwickTree("fw", 1, 5); !ok {
		t.Fatal("AddFenwickTree(seed) = false, want true")
	}
	if err := source.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	source.AddQuantileSketch("quantile", 10)
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "old", "value")
	source.UpsertCounter("cmd-counter", 5)
	source.UpsertMap("cmd-map", Map{"old": "value"})
	source.IncrementCountMinSketch("cmd-cms", "old", 2)
	source.AddHyperLogLog("cmd-hll", "old")
	source.AddTopK("cmd-top", "old", 2)
	source.AddReservoirSample("cmd-sample", "old")
	source.AddBloomFilter("cmd-bloom", "old")
	source.AddCuckooFilter("cmd-cuckoo", "old")
	if _, err := source.AddXorFilter("cmd-xor", "old"); err != nil {
		t.Fatalf("AddXorFilter(cmd-xor) error = %v", err)
	}
	if _, ok, err := source.BuildXorFilter("cmd-xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter(cmd-xor) ok=%v err=%v, want ok nil", ok, err)
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	loaded.IncrementCounter("counter", 2)
	loaded.AppendString("append", "-new")
	loaded.PrependString("prepend", "new-")
	loaded.PutMap("map", "new", "value")
	loaded.PushSlice("slice", "new")
	if added := loaded.PushPriorityQueue("queue", 1, "new"); added != 0 {
		t.Fatalf("PushPriorityQueue(closed cold ref) = %d, want 0", added)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PUSHPQ", Key: "queue", Value: "new", Subkey: "1"}); got.OK {
		t.Fatalf("PUSHPQ closed cold ref response = %#v, want error", got)
	}
	if added := loaded.AddRoaringBitmap("rb", 2); added != 0 {
		t.Fatalf("AddRoaringBitmap(closed cold ref) = %d, want 0", added)
	}
	if added := loaded.AddSparseBitset("sb", 2); added != 0 {
		t.Fatalf("AddSparseBitset(closed cold ref) = %d, want 0", added)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "rb", Value: "2"}); got.OK {
		t.Fatalf("ADDRB closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "HASSB", Key: "sb", Value: "1"}); got.OK {
		t.Fatalf("HASSB closed cold ref response = %#v, want error", got)
	}
	if update, ok := loaded.AddFenwickTree("fw", 2, 3); ok || update.Total != 0 {
		t.Fatalf("AddFenwickTree(closed cold ref) = %#v/%v, want zero/false", update, ok)
	}
	if estimate := loaded.AddQuantileSketch("quantile", 20); estimate.Count != 0 {
		t.Fatalf("AddQuantileSketch(closed cold ref) = %#v, want zero estimate", estimate)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "fw", Value: "2", Subkey: "3"}); got.OK {
		t.Fatalf("ADDFW closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "ESTQ", Key: "quantile", Value: "0.5"}); got.OK {
		t.Fatalf("ESTQ closed cold ref response = %#v, want error", got)
	}
	if added := loaded.PutRadixTree("radix", "new", "value"); added {
		t.Fatal("PutRadixTree(closed cold ref) = true, want false")
	}
	if added := loaded.PutRadixTreeEntries("radix", Map{"new": "value"}); added != 0 {
		t.Fatalf("PutRadixTreeEntries(closed cold ref) = %d, want 0", added)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PUTRT", Key: "radix", Subkey: "new", Value: "value"}); got.OK {
		t.Fatalf("PUTRT closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GETRT", Key: "radix", Subkey: "old"}); got.OK {
		t.Fatalf("GETRT closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INC", Key: "cmd-counter", Value: "2"}); got.OK {
		t.Fatalf("INC closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "cmd-map", Subkey: "new", Value: "value"}); got.OK {
		t.Fatalf("PUTMAP closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PUSHSLICE", Key: "slice", Value: "new"}); got.OK {
		t.Fatalf("PUSHSLICE closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "map", Subkey: "old"}); got.OK {
		t.Fatalf("PEEKMAP closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "TAKEMAP", Key: "map", Subkey: "old"}); got.OK {
		t.Fatalf("TAKEMAP closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "HEADSLICE", Key: "slice"}); got.OK {
		t.Fatalf("HEADSLICE closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "TAILSLICE", Key: "slice"}); got.OK {
		t.Fatalf("TAILSLICE closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "POPSLICE", Key: "slice"}); got.OK {
		t.Fatalf("POPSLICE closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "SHIFTSLICE", Key: "slice"}); got.OK {
		t.Fatalf("SHIFTSLICE closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GETSET", Key: "set"}); got.OK {
		t.Fatalf("GETSET closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "PEEKPQ", Key: "queue"}); got.OK {
		t.Fatalf("PEEKPQ closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "POPPQ", Key: "queue"}); got.OK {
		t.Fatalf("POPPQ closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GETPQ", Key: "queue"}); got.OK {
		t.Fatalf("GETPQ closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOCMS", Key: "cmd-cms"}); got.OK {
		t.Fatalf("INFOCMS closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "COUNTHLL", Key: "cmd-hll"}); got.OK {
		t.Fatalf("COUNTHLL closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOHLL", Key: "cmd-hll"}); got.OK {
		t.Fatalf("INFOHLL closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GETTOPK", Key: "cmd-top"}); got.OK {
		t.Fatalf("GETTOPK closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOTOPK", Key: "cmd-top"}); got.OK {
		t.Fatalf("INFOTOPK closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GETRS", Key: "cmd-sample"}); got.OK {
		t.Fatalf("GETRS closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFORS", Key: "cmd-sample"}); got.OK {
		t.Fatalf("INFORS closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOBF", Key: "cmd-bloom"}); got.OK {
		t.Fatalf("INFOBF closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOCF", Key: "cmd-cuckoo"}); got.OK {
		t.Fatalf("INFOCF closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "cmd-xor", Value: "old"}); got.OK {
		t.Fatalf("HASXF closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "BUILDXF", Key: "cmd-xor"}); got.OK {
		t.Fatalf("BUILDXF closed cold ref response = %#v, want error", got)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "INFOXF", Key: "cmd-xor"}); got.OK {
		t.Fatalf("INFOXF closed cold ref response = %#v, want error", got)
	}

	keys := []string{
		"append",
		"cmd-bloom",
		"cmd-counter",
		"cmd-cms",
		"cmd-cuckoo",
		"cmd-hll",
		"cmd-map",
		"cmd-sample",
		"cmd-top",
		"cmd-xor",
		"counter",
		"fw",
		"map",
		"prepend",
		"quantile",
		"queue",
		"radix",
		"rb",
		"sb",
		"set",
		"slice",
	}
	entries := loaded.Entries(true)
	if len(entries) != len(keys) {
		t.Fatalf("entries after blocked mutations = %d, want %d", len(entries), len(keys))
	}
	for _, key := range keys {
		hval, err := loaded.GetChecked(key)
		if !hval.Empty() || !errors.Is(err, ErrLevelDBStoreClosed) {
			t.Fatalf("GetChecked(%s after blocked mutation) = %+v/%v, want empty/ErrLevelDBStoreClosed", key, hval, err)
		}
	}
	for _, entry := range entries {
		if !entry.Value.IsLevelDBReference() {
			t.Fatalf("entry after blocked mutation = %#v, want leveldb reference", entry)
		}
	}
}

func TestHydrateLevelDBReferencesReportsClosedStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got, err := loaded.HydrateLevelDBReferences(); got != 0 || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HydrateLevelDBReferences() = %d/%v, want 0/ErrLevelDBStoreClosed", got, err)
	}
	if err := loaded.SaveSnapshot(filepath.Join(t.TempDir(), "snapshot.json")); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("SaveSnapshot(with closed cold ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
}

func TestLevelDBColdReferenceReadErrorsDoNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 7)
	source.UpsertString("cold", "value")
	source.UpsertBytes("bytes", []byte("payload"))
	source.UpsertMap("map", Map{"field": "value"})
	source.UpsertSlice("slice", Slice{"value"})
	source.UpsertSet("set", Set{"value"})
	source.AddBloomFilter("bloom", "value")
	source.AddCuckooFilter("cuckoo", "value")
	source.PushPriorityQueue("queue", 1, "job")
	source.UpsertRoaringBitmap("rb")
	source.AddRoaringBitmap("rb", 1)
	source.UpsertSparseBitset("sb")
	source.AddSparseBitset("sb", 1)
	if err := source.UpsertFenwickTree("fw", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := source.AddFenwickTree("fw", 1, 5); !ok {
		t.Fatal("AddFenwickTree(seed) = false, want true")
	}
	if err := source.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	source.AddQuantileSketch("quantile", 10)
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "old", Map{"field": "value"})
	source.IncrementCountMinSketch("cms", "old", 2)
	source.AddHyperLogLog("hll", "old")
	source.AddTopK("top", "old", 2)
	source.AddReservoirSample("sample", "old")
	if _, err := source.AddXorFilter("xor", "old"); err != nil {
		t.Fatalf("AddXorFilter(xor) error = %v", err)
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if hval, err := loaded.GetChecked("cold"); !hval.Empty() || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetChecked(cold closed ref) = %+v/%v, want empty/ErrLevelDBStoreClosed", hval, err)
	}
	if got, ok, err := loaded.GetCounterChecked("counter"); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetCounterChecked(counter closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetStringChecked("cold"); got != "" || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetStringChecked(cold closed ref) = %q/%v/%v, want empty/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if hval := loaded.Get("cold"); !hval.Empty() {
		t.Fatalf("legacy Get(cold closed ref) = %+v, want empty", hval)
	}
	if got := loaded.GetString("cold"); got != "" {
		t.Fatalf("legacy GetString(cold closed ref) = %q, want empty", got)
	}
	if got, err := loaded.GetBytesChecked("bytes"); got != nil || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetBytesChecked(bytes closed ref) = %q/%v, want nil/ErrLevelDBStoreClosed", got, err)
	}
	if got, ok, err := loaded.GetMapChecked("map"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetMapChecked(map closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetMapJSON("map"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetMapJSON(map closed ref) = %q/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.PeekMapChecked("map", "field"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PeekMapChecked(map closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.TakeMapChecked("map", "field"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("TakeMapChecked(map closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetSliceChecked("slice"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetSliceChecked(slice closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.HeadSliceChecked("slice"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HeadSliceChecked(slice closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.TailSliceChecked("slice"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("TailSliceChecked(slice closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.PopSliceChecked("slice"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PopSliceChecked(slice closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.ShiftSliceChecked("slice"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("ShiftSliceChecked(slice closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetSetChecked("set"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetSetChecked(set closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.BloomFilterInfoChecked("bloom"); got.Insertions != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("BloomFilterInfoChecked(bloom closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.CuckooFilterInfoChecked("cuckoo"); got.Count != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CuckooFilterInfoChecked(cuckoo closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.PeekPriorityQueueChecked("queue"); got.Priority != 0 || got.Value != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PeekPriorityQueueChecked(queue closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.PopPriorityQueueChecked("queue"); got.Priority != 0 || got.Value != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PopPriorityQueueChecked(queue closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetPriorityQueueChecked("queue"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetPriorityQueueChecked(queue closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if hit, err := loaded.HasRoaringBitmapChecked("rb", 1); hit || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasRoaringBitmapChecked(rb closed ref) = %v/%v, want false/ErrLevelDBStoreClosed", hit, err)
	}
	if got, ok, err := loaded.CountRoaringBitmapChecked("rb"); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountRoaringBitmapChecked(rb closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetRoaringBitmapChecked("rb"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetRoaringBitmapChecked(rb closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.RoaringBitmapInfoChecked("rb"); got.Cardinality != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RoaringBitmapInfoChecked(rb closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if hit, err := loaded.HasSparseBitsetChecked("sb", 1); hit || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasSparseBitsetChecked(sb closed ref) = %v/%v, want false/ErrLevelDBStoreClosed", hit, err)
	}
	if got, ok, err := loaded.CountSparseBitsetChecked("sb"); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountSparseBitsetChecked(sb closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetSparseBitsetChecked("sb"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetSparseBitsetChecked(sb closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.SparseBitsetInfoChecked("sb"); got.Cardinality != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("SparseBitsetInfoChecked(sb closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetFenwickTreeChecked("fw", 1); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetFenwickTreeChecked(fw closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.PrefixSumFenwickTreeChecked("fw", 1); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PrefixSumFenwickTreeChecked(fw closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.RangeSumFenwickTreeChecked("fw", 0, 1); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RangeSumFenwickTreeChecked(fw closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.FenwickTreeInfoChecked("fw"); got.Size != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("FenwickTreeInfoChecked(fw closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.EstimateQuantileSketchChecked("quantile", 0.5); got.Count != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("EstimateQuantileSketchChecked(quantile closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.QuantileSketchInfoChecked("quantile"); got.Count != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("QuantileSketchInfoChecked(quantile closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetRadixTreeChecked("radix", "old"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetRadixTreeChecked(radix closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if hit, err := loaded.HasRadixTreeChecked("radix", "old"); hit || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasRadixTreeChecked(radix closed ref) = %v/%v, want false/ErrLevelDBStoreClosed", hit, err)
	}
	if got, ok, err := loaded.ScanRadixTreeChecked("radix", ""); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("ScanRadixTreeChecked(radix closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.RadixTreeInfoChecked("radix"); got.Items != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RadixTreeInfoChecked(radix closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.CountMinSketchInfoChecked("cms"); got.TotalCount != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountMinSketchInfoChecked(cms closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.CountHyperLogLogChecked("hll"); got != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountHyperLogLogChecked(hll closed ref) = %d/%v/%v, want 0/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.HyperLogLogInfoChecked("hll"); got.Observations != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HyperLogLogInfoChecked(hll closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetTopKChecked("top"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetTopKChecked(top closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.TopKInfoChecked("top"); got.Total != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("TopKInfoChecked(top closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.GetReservoirSampleChecked("sample"); got != nil || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetReservoirSampleChecked(sample closed ref) = %#v/%v/%v, want nil/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.ReservoirSampleInfoChecked("sample"); got.Seen != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("ReservoirSampleInfoChecked(sample closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got, ok, err := loaded.XorFilterInfoChecked("xor"); got.Items != 0 || ok || !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("XorFilterInfoChecked(xor closed ref) = %#v/%v/%v, want zero/false/ErrLevelDBStoreClosed", got, ok, err)
	}
	if got := loaded.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "cold"}); got.OK {
		t.Fatalf("GET cold closed ref response = %#v, want error", got)
	}

	entries := loaded.Entries(true)
	if len(entries) != 19 {
		t.Fatalf("Entries(after closed ref reads) len = %d, want 19", len(entries))
	}
	for _, entry := range entries {
		if !entry.Value.IsLevelDBReference() {
			t.Fatalf("entry after failed read = %#v, want leveldb reference", entry)
		}
	}
}

func TestLevelDBColdReferenceCheckedAPIsReturnHydrationErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertCounter("counter", 5)
	source.UpsertString("append", "old")
	source.UpsertString("prepend", "old")
	source.UpsertMap("map", Map{"alpha": "value"})
	source.PushSlice("slice", "alpha")
	source.UpsertSet("set", Set{"alpha"})
	source.AddBloomFilter("bloom", "alpha")
	source.AddCuckooFilter("cuckoo", "alpha")
	source.PushPriorityQueue("queue", 1, "alpha")
	source.UpsertRoaringBitmap("rb")
	source.AddRoaringBitmap("rb", 1)
	source.UpsertSparseBitset("sb")
	source.AddSparseBitset("sb", 1)
	if err := source.UpsertFenwickTree("fw", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	if _, ok := source.AddFenwickTree("fw", 1, 5); !ok {
		t.Fatal("AddFenwickTree(seed) = false, want true")
	}
	if err := source.UpsertQuantileSketch("quantile", DefaultQuantileSketchEpsilon); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	source.AddQuantileSketch("quantile", 10)
	source.UpsertRadixTree("radix")
	source.PutRadixTree("radix", "old", "value")
	source.IncrementCountMinSketch("cms", "alpha", 1)
	source.AddHyperLogLog("hll", "alpha")
	source.AddTopK("top", "alpha", 1)
	source.AddReservoirSample("sample", "alpha")
	if _, err := source.AddXorFilter("xor", "alpha"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := source.BuildXorFilter("xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = ok %v err %v, want ok nil", ok, err)
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := loaded.HasSetChecked("set", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasSetChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.RemoveSetChecked("set", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RemoveSetChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if err := loaded.PutMapChecked("map", "beta", "value"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PutMapChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if err := loaded.PutMapEntriesChecked("map", Map{"beta": "value"}); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PutMapEntriesChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if err := loaded.PushSliceChecked("slice", "beta"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PushSliceChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.IncrementCounterChecked("counter", 1); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("IncrementCounterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.AppendStringChecked("append", "-new"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AppendStringChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.PrependStringChecked("prepend", "new-"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PrependStringChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.HasBloomFilterChecked("bloom", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasBloomFilterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.BloomFilterInfoChecked("bloom"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("BloomFilterInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.HasCuckooFilterChecked("cuckoo", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasCuckooFilterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.DeleteCuckooFilterChecked("cuckoo", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("DeleteCuckooFilterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.CuckooFilterInfoChecked("cuckoo"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CuckooFilterInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.PushPriorityQueueChecked("queue", 1, "beta"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PushPriorityQueueChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.AddRoaringBitmapChecked("rb", 2); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AddRoaringBitmapChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.RemoveRoaringBitmapChecked("rb", 1); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RemoveRoaringBitmapChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.AddSparseBitsetChecked("sb", 2); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AddSparseBitsetChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.RemoveSparseBitsetChecked("sb", 1); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("RemoveSparseBitsetChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.AddFenwickTreeChecked("fw", 2, 3); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AddFenwickTreeChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.GetFenwickTreeChecked("fw", 1); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetFenwickTreeChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.AddQuantileSketchChecked("quantile", 20); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AddQuantileSketchChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.EstimateQuantileSketchChecked("quantile", 0.5); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("EstimateQuantileSketchChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.PutRadixTreeChecked("radix", "new", "value"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("PutRadixTreeChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.GetRadixTreeChecked("radix", "old"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetRadixTreeChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.DeleteRadixTreeChecked("radix", "old"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("DeleteRadixTreeChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.EstimateCountMinSketchChecked("cms", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("EstimateCountMinSketchChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.CountMinSketchInfoChecked("cms"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountMinSketchInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.CountHyperLogLogChecked("hll"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("CountHyperLogLogChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.HyperLogLogInfoChecked("hll"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HyperLogLogInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.EstimateTopKChecked("top", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("EstimateTopKChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.GetTopKChecked("top"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetTopKChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.TopKInfoChecked("top"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("TopKInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.GetReservoirSampleChecked("sample"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("GetReservoirSampleChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.ReservoirSampleInfoChecked("sample"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("ReservoirSampleInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.HasXorFilterChecked("xor", "alpha"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("HasXorFilterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, err := loaded.AddXorFilterChecked("xor", "beta"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("AddXorFilterChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.BuildXorFilter("xor"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("BuildXorFilter(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
	if _, _, err := loaded.XorFilterInfoChecked("xor"); !errors.Is(err, ErrLevelDBStoreClosed) {
		t.Fatalf("XorFilterInfoChecked(closed ref) error = %v, want ErrLevelDBStoreClosed", err)
	}
}

func TestLevelDBStoreHotLoadCanDeleteColdReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("cold", "value")
	source.UpsertString("keep", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 2 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 2 keys and 0 values", result)
	}
	if !loaded.Delete("cold") {
		t.Fatal("Delete(cold reference) = false, want true")
	}
	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(after delete) error = %v", err)
	}

	roundTrip := newTestTrie(t)
	if count, err := store.Load(roundTrip); err != nil || count != 1 {
		t.Fatalf("Load(roundTrip) = %d/%v, want 1 nil", count, err)
	}
	if got := roundTrip.GetString("cold"); got != "" {
		t.Fatalf("deleted cold value = %q, want empty", got)
	}
	if got := roundTrip.GetString("keep"); got != "value" {
		t.Fatalf("keep = %q, want value", got)
	}
}

func TestLevelDBStoreSavePreservesUnchangedColdReferenceRecordBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte("{\n  \"key\": \"cold\",\n  \"type\": \"string\",\n  \"string\": \"value\"\n}")
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 1 key and 0 values", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || entries[0].Key != "cold" || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want cold leveldb reference", entries)
	}

	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(unchanged cold ref) error = %v", err)
	}
	got, ok, err := store.entryData("cold")
	if err != nil || !ok {
		t.Fatalf("entryData(cold) = %v/%v, want record", ok, err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw cold record changed:\ngot  %q\nwant %q", got, raw)
	}
}

func TestLevelDBStoreSaveOmitsUnrelatedNullSnapshotFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, StorageFormatJSON)
	if err != nil {
		t.Fatalf("OpenLevelDBStoreWithFormat(json) error = %v", err)
	}
	defer store.Close()

	ht := newTestTrie(t)
	ht.UpsertString("key", "value")
	ht.mu.Lock()
	delete(ht.keyStats, "key")
	ht.mu.Unlock()
	if err := store.Save(ht); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, ok, err := store.entryData("key")
	if err != nil || !ok {
		t.Fatalf("entryData(key) = %v/%v, want record", ok, err)
	}
	if want := []byte(`{"key":"key","type":"string","string":"value"}`); !bytes.Equal(got, want) {
		t.Fatalf("entryData(key) = %q, want compact string record %q", got, want)
	}
	for _, field := range [][]byte{[]byte(`"map":null`), []byte(`"slice":null`), []byte(`"set":null`), []byte(`"priority_queue":null`)} {
		if bytes.Contains(got, field) {
			t.Fatalf("entryData(key) = %q, want no unrelated null field %q", got, field)
		}
	}
}

func TestLevelDBDiffBatchSkipsUnchangedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	trie := newTestTrie(t)
	trie.UpsertString("key", "value")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(initial) error = %v", err)
	}

	batch, err := levelDBDiffBatch(store, store.db, trie)
	if err != nil {
		t.Fatalf("levelDBDiffBatch(unchanged) error = %v", err)
	}
	if batch.Len() != 0 {
		t.Fatalf("levelDBDiffBatch(unchanged) len = %d, want 0", batch.Len())
	}
}

func TestLevelDBDiffBatchTracksAddsUpdatesAndDeletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	trie := newTestTrie(t)
	trie.UpsertString("added-later", "old")
	trie.UpsertString("changed", "old")
	trie.UpsertString("stale", "old")
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(initial) error = %v", err)
	}

	if !trie.Delete("added-later") {
		t.Fatal("Delete(added-later) = false, want true")
	}
	trie.UpsertString("added", "new")
	trie.UpsertString("changed", "new")
	if !trie.Delete("stale") {
		t.Fatal("Delete(stale) = false, want true")
	}

	batch, err := levelDBDiffBatch(store, store.db, trie)
	if err != nil {
		t.Fatalf("levelDBDiffBatch(changed) error = %v", err)
	}
	if batch.Len() != 4 {
		t.Fatalf("levelDBDiffBatch(changed) len = %d, want add/update/delete/delete", batch.Len())
	}
	if err := store.Save(trie); err != nil {
		t.Fatalf("Save(changed) error = %v", err)
	}

	loaded := newTestTrie(t)
	count, err := store.Load(loaded)
	if err != nil {
		t.Fatalf("Load(changed) error = %v", err)
	}
	if count != 2 {
		t.Fatalf("Load(changed) count = %d, want 2", count)
	}
	if got := loaded.GetString("added"); got != "new" {
		t.Fatalf("added = %q, want new", got)
	}
	if got := loaded.GetString("changed"); got != "new" {
		t.Fatalf("changed = %q, want new", got)
	}
	if got := loaded.GetString("stale"); got != "" {
		t.Fatalf("stale = %q, want empty", got)
	}
	if got := loaded.GetString("added-later"); got != "" {
		t.Fatalf("added-later = %q, want empty", got)
	}
}

func TestLevelDBDiffBatchReusesSameStoreColdReferenceWithoutNestedLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte(`{"key":"cold","type":"string","string":"value"}`)
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	loaded := newTestTrie(t)
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want cold reference", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want cold reference", entries)
	}

	assertDiffCompletesWithStoreLocked := func(name string) {
		t.Helper()
		store.mu.Lock()
		defer store.mu.Unlock()

		done := make(chan error, 1)
		go func() {
			_, err := levelDBDiffBatch(store, store.db, loaded)
			done <- err
		}()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("levelDBDiffBatch(%s) error = %v", name, err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("levelDBDiffBatch(%s) blocked on nested store lock", name)
		}
	}

	assertDiffCompletesWithStoreLocked("unchanged cold reference")
	if !loaded.Exists("cold") {
		t.Fatal("Exists(cold) = false, want true")
	}
	assertDiffCompletesWithStoreLocked("stats-changed cold reference")
}

func TestLevelDBStoreSaveRewritesColdReferenceWhenStatsChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte(`{"key":"cold","type":"string","string":"value"}`)
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	now := time.Unix(4800, 0)
	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if !loaded.Exists("cold") {
		t.Fatal("Exists(cold) = false, want true")
	}
	if err := store.Save(loaded); err != nil {
		t.Fatalf("Save(stats-changed cold ref) error = %v", err)
	}

	got, ok, err := store.entryData("cold")
	if err != nil || !ok {
		t.Fatalf("entryData(cold) = %v/%v, want record", ok, err)
	}
	if bytes.Equal(got, raw) {
		t.Fatal("stats-changed cold record was raw-copied without rewrite")
	}
	entry, err := decodeLevelDBEntryForKey("cold", got)
	if err != nil {
		t.Fatalf("decode rewritten cold record error = %v", err)
	}
	if entry.Stats == nil || entry.Stats.Reads != 1 || entry.Stats.Hits != 1 || entry.Stats.LastHit != now {
		t.Fatalf("rewritten stats = %#v, want one hit at %s", entry.Stats, now)
	}
	if entry.String != "value" {
		t.Fatalf("rewritten value = %q, want value", entry.String)
	}
}

func TestLevelDBStoreSaveValidatesChangedColdReferenceRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	raw := []byte(`{"key":"cold","type":"string","string":"value"}`)
	if err := store.db.Put(levelDBKey("cold"), raw, nil); err != nil {
		t.Fatalf("Put(raw) error = %v", err)
	}

	loaded := newTestTrie(t)
	if _, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy()); err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	changed := []byte(`{"key":"cold","type":"string","string":"value","unknown":true}`)
	if err := store.db.Put(levelDBKey("cold"), changed, nil); err != nil {
		t.Fatalf("Put(changed raw) error = %v", err)
	}

	if err := store.Save(loaded); err == nil {
		t.Fatal("Save(changed invalid cold ref) error = nil, want validation error")
	}
}

func TestLevelDBStoreHotLoadSchedulesColdReferenceExpiration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(4700, 0)
	source.now = func() time.Time { return now }
	source.UpsertString("cold", "value")
	if !source.Expire("cold", time.Minute) {
		t.Fatal("Expire(cold) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	store, err := OpenLevelDBStore(path)
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now }
	result, err := store.LoadWithPolicy(loaded, DefaultLevelDBHotLoadPolicy())
	if err != nil {
		t.Fatalf("LoadWithPolicy() error = %v", err)
	}
	if result.KeysLoaded != 1 || result.ValuesLoaded != 0 {
		t.Fatalf("hot-load result = %#v, want 1 key and 0 values", result)
	}
	entries := loaded.Entries(true)
	if len(entries) != 1 || entries[0].Key != "cold" || !entries[0].Value.IsLevelDBReference() {
		t.Fatalf("entries after hot-load = %#v, want cold leveldb reference", entries)
	}

	now = now.Add(2 * time.Minute)
	if got := loaded.VacuumExpired(); got != 1 {
		t.Fatalf("VacuumExpired() after hot-load = %d, want 1", got)
	}
	if got := loaded.GetString("cold"); got != "" {
		t.Fatalf("cold after vacuum = %q, want empty", got)
	}
}

func TestLevelDBStoreSkipsExpiredValuesOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	now := time.Unix(5000, 0)
	source.now = func() time.Time { return now }

	source.UpsertString("expired", "old")
	if !source.Expire("expired", time.Second) {
		t.Fatal("Expire(expired) = false, want true")
	}
	source.UpsertString("active", "new")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	loaded.now = func() time.Time { return now.Add(2 * time.Second) }
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("loaded count = %d, want 1", count)
	}
	if got := loaded.GetString("expired"); got != "" {
		t.Fatalf("expired = %q, want empty", got)
	}
	if got := loaded.GetString("active"); got != "new" {
		t.Fatalf("active = %q, want new", got)
	}
}

func TestLevelDBStoreRestoresLargeBytesToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	payload := testPayload(DiskBytesThreshold + 1)
	source.UpsertBytes("large", payload)

	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB() error = %v", err)
	}

	loaded := newTestTrie(t)
	if _, err := loaded.LoadLevelDB(path); err != nil {
		t.Fatalf("LoadLevelDB() error = %v", err)
	}
	hval := loaded.Get("large")
	if !hval.IsBytesAtRaws() || !hval.OnDisk() {
		t.Fatalf("loaded large value = %+v, want on-disk bytes", hval)
	}
	if got := loaded.GetBytes("large"); !bytes.Equal(got, payload) {
		t.Fatalf("large payload mismatch after LevelDB load")
	}
}

func TestLevelDBStoreSaveRemovesStaleKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("keep", "value")
	source.UpsertString("stale", "old")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(first) error = %v", err)
	}

	if !source.Delete("stale") {
		t.Fatal("Delete(stale) = false, want true")
	}
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(second) error = %v", err)
	}

	loaded := newTestTrie(t)
	if count, err := loaded.LoadLevelDB(path); err != nil || count != 1 {
		t.Fatalf("LoadLevelDB() = %d/%v, want 1 nil", count, err)
	}
	if got := loaded.GetString("keep"); got != "value" {
		t.Fatalf("keep = %q, want value", got)
	}
	if got := loaded.GetString("stale"); got != "" {
		t.Fatalf("stale = %q, want empty", got)
	}
}

func TestLevelDBStoreFailedSavePreservesExistingStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := newTestTrie(t)
	source.UpsertString("keep", "value")
	if err := source.SaveLevelDB(path); err != nil {
		t.Fatalf("SaveLevelDB(initial) error = %v", err)
	}

	bad := newTestTrie(t)
	bad.UpsertMap("bad", Map{"ch": make(chan int)})
	if err := bad.SaveLevelDB(path); err == nil {
		t.Fatal("SaveLevelDB(invalid) error = nil, want unsupported JSON value error")
	}

	loaded := newTestTrie(t)
	count, err := loaded.LoadLevelDB(path)
	if err != nil {
		t.Fatalf("LoadLevelDB(after failed save) error = %v", err)
	}
	if count != 1 {
		t.Fatalf("LoadLevelDB(after failed save) count = %d, want 1", count)
	}
	if got := loaded.GetString("keep"); got != "value" {
		t.Fatalf("keep after failed save = %q, want value", got)
	}
	if loaded.Exists("bad") {
		t.Fatal("failed LevelDB save wrote invalid bad key")
	}
}

func TestDecodeLevelDBEntryRejectsInvalidJSON(t *testing.T) {
	if _, err := decodeLevelDBEntry([]byte(`{"key":"x","type":"string"} trailing`)); err == nil {
		t.Fatal("decodeLevelDBEntry(trailing) error = nil, want error")
	}
	if _, err := decodeLevelDBEntry([]byte(`{"type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntry(missing key) error = nil, want error")
	}
	if _, err := decodeLevelDBEntry([]byte(`{"key":null,"type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntry(null key) error = nil, want error")
	}
	if _, err := decodeLevelDBEntryForKey("x", []byte(`{"key":"y","type":"string","string":"value"}`)); err == nil {
		t.Fatal("decodeLevelDBEntryForKey(mismatch) error = nil, want error")
	}
	if entry, err := decodeLevelDBEntryForKey("", []byte(`{"key":"","type":"string","string":"value"}`)); err != nil || entry.Key != "" {
		t.Fatalf("decodeLevelDBEntryForKey(empty key) = %#v/%v, want empty key/nil", entry, err)
	}
}
