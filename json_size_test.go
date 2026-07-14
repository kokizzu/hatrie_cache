package hatriecache

import (
	"encoding/json"
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

func TestJSONEncodedSizeReportsMarshalError(t *testing.T) {
	_, err := jsonEncodedSize(map[string]interface{}{"bad": func() {}})
	if err == nil {
		t.Fatal("jsonEncodedSize(unsupported value) error = nil, want error")
	}
}
