package hatriecache

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONEncodedSizeMatchesMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name: "map",
			value: map[string]interface{}{
				"b": "<tag>",
				"a": json.Number("42"),
			},
		},
		{
			name: "slice",
			value: []interface{}{
				"alpha",
				json.Number("3.5"),
				map[string]interface{}{"ok": true},
			},
		},
		{
			name:  "snapshot items",
			value: []RadixTreeItem{{Key: "user:1", Value: map[string]interface{}{"status": "active"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			size, err := jsonEncodedSize(test.value)
			if err != nil {
				t.Fatalf("jsonEncodedSize() error = %v", err)
			}
			if size != int64(len(data)) {
				t.Fatalf("jsonEncodedSize() = %d, want %d", size, len(data))
			}
		})
	}
}

func TestJSONEncodedStringMatchesMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name: "map",
			value: map[string]interface{}{
				"b": "<tag>",
				"a": json.Number("42"),
			},
		},
		{
			name: "slice",
			value: []interface{}{
				"alpha",
				json.Number("3.5"),
				map[string]interface{}{"ok": true},
			},
		},
		{
			name:  "snapshot entry",
			value: snapshotEntry{Key: "tag", Type: "string", String: "<value>"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			out, err := jsonEncodedString(test.value)
			if err != nil {
				t.Fatalf("jsonEncodedString() error = %v", err)
			}
			if out != string(data) {
				t.Fatalf("jsonEncodedString() = %q, want %q", out, string(data))
			}
		})
	}
}

func TestJSONEncodedSizeReportsMarshalError(t *testing.T) {
	_, err := jsonEncodedSize(map[string]interface{}{"bad": func() {}})
	if err == nil {
		t.Fatal("jsonEncodedSize(unsupported value) error = nil, want error")
	}
}

func TestJSONEncodedSizeWithin(t *testing.T) {
	value := map[string]interface{}{
		"b": strings.Repeat("quoted\nvalue", 4),
		"a": json.Number("42"),
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	size, within, err := jsonEncodedSizeWithin(value, int64(len(data)))
	if err != nil {
		t.Fatalf("jsonEncodedSizeWithin(exact) error = %v", err)
	}
	if !within {
		t.Fatal("jsonEncodedSizeWithin(exact) within = false, want true")
	}
	if size != int64(len(data)) {
		t.Fatalf("jsonEncodedSizeWithin(exact) size = %d, want %d", size, len(data))
	}

	size, within, err = jsonEncodedSizeWithin(value, int64(len(data)-1))
	if err != nil {
		t.Fatalf("jsonEncodedSizeWithin(short) error = %v", err)
	}
	if within {
		t.Fatal("jsonEncodedSizeWithin(short) within = true, want false")
	}
	if size != 0 {
		t.Fatalf("jsonEncodedSizeWithin(short) size = %d, want 0", size)
	}

	size, within, err = jsonEncodedSizeWithin(value, -1)
	if err != nil {
		t.Fatalf("jsonEncodedSizeWithin(negative) error = %v", err)
	}
	if within {
		t.Fatal("jsonEncodedSizeWithin(negative) within = true, want false")
	}
	if size != 0 {
		t.Fatalf("jsonEncodedSizeWithin(negative) size = %d, want 0", size)
	}
}

func TestJSONEncodedSizeWithinReportsMarshalError(t *testing.T) {
	_, _, err := jsonEncodedSizeWithin(map[string]interface{}{"bad": func() {}}, 1024)
	if err == nil {
		t.Fatal("jsonEncodedSizeWithin(unsupported value) error = nil, want error")
	}
}

func TestJSONEncodedStringReportsMarshalError(t *testing.T) {
	_, err := jsonEncodedString(map[string]interface{}{"bad": func() {}})
	if err == nil {
		t.Fatal("jsonEncodedString(unsupported value) error = nil, want error")
	}
}

func TestJSONEncodedSizeBackedEncodedSizesMatchMarshal(t *testing.T) {
	top, err := newTopKData(3)
	if err != nil {
		t.Fatalf("newTopKData() error = %v", err)
	}
	top.Add("<alpha>", 5)
	top.Add("beta", 2)
	assertEncodedSizeMatchesMarshal(t, "top_k", top.EncodedSize(), top.Snapshot())

	sample, err := newReservoirSampleData(3)
	if err != nil {
		t.Fatalf("newReservoirSampleData() error = %v", err)
	}
	sample.AddOne("<alpha>", "beta", "gamma", "delta")
	assertEncodedSizeMatchesMarshal(t, "reservoir_sample", sample.EncodedSize(), sample.Snapshot())

	sketch, err := newQuantileSketchData(0.01)
	if err != nil {
		t.Fatalf("newQuantileSketchData() error = %v", err)
	}
	sketch.Add(10, 20, 30)
	assertEncodedSizeMatchesMarshal(t, "quantile_sketch", sketch.EncodedSize(), sketch.Snapshot())

	tree, err := newFenwickTreeData(8)
	if err != nil {
		t.Fatalf("newFenwickTreeData() error = %v", err)
	}
	tree.Add(2, 5)
	tree.Add(6, -1)
	assertEncodedSizeMatchesMarshal(t, "fenwick_tree", tree.EncodedSize(), tree.Snapshot())
}

func assertEncodedSizeMatchesMarshal(t *testing.T, name string, got int64, value interface{}) {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal(%s) error = %v", name, err)
	}
	if got != int64(len(data)) {
		t.Fatalf("%s EncodedSize() = %d, want %d", name, got, len(data))
	}
}
