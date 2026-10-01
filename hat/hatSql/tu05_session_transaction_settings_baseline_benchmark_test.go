package hatSql

import (
	"testing"
	"time"
)

var tu05BeforeSettingsSink map[string]interface{}

// This is the pre-contract control: callers construct an ad-hoc map for every
// request because there is no shared session settings value.
func BenchmarkTU05BeforeAdHocSettingsMap(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		settings := map[string]interface{}{
			"isolation":  "read_committed",
			"read_only":  false,
			"timeout":    time.Duration(0),
			"durability": "durable",
		}
		tu05BeforeSettingsSink = settings
		if len(settings) != 4 {
			b.Fatal("unexpected settings field count")
		}
	}
}
