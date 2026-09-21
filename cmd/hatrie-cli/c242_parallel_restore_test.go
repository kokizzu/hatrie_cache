package main

import (
	"bytes"
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRestoreBundleRejectsInvalidPartConcurrency(t *testing.T) {
	err := run(context.Background(), []string{
		"restore-bundle",
		"-bundle", filepath.Join(t.TempDir(), "missing-backup"),
		"-max-part-concurrency", "-1",
	}, &bytes.Buffer{}, &bytes.Buffer{}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "restore file concurrency is invalid") {
		t.Fatalf("run(restore-bundle invalid concurrency) error = %v", err)
	}
}
