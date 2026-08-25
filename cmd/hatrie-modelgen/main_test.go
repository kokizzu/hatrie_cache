package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunGeneratesModelsToStdout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(path, []byte(`{"sources":{"users":{"columns":[{"name":"id","type":"INTEGER","not_null":true}]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"-schema", path, "-package", "models", "-out", "-"}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if got := output.String(); !strings.Contains(got, "type Users struct") || !strings.Contains(got, "Id int64") {
		t.Fatalf("generated output = %q", got)
	}
}
