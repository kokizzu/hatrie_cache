package hatSnapshot_test

import (
	"testing"

	"hatrie_cache/hat/hatSnapshot"
)

func TestParseFormatUsesCompactDefaultAndAliases(t *testing.T) {
	format, err := hatSnapshot.ParseFormat("gzip-bin")
	if err != nil || format != hatSnapshot.FormatGzipBinary {
		t.Fatalf("ParseFormat(gzip-bin) = %q, %v", format, err)
	}
	format, err = hatSnapshot.ParseFormat("")
	if err != nil || format != hatSnapshot.DefaultFormat {
		t.Fatalf("ParseFormat(empty) = %q, %v", format, err)
	}
}
