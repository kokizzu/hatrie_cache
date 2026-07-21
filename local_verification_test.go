package hatriecache

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLocalVerificationReplacesHostedWorkflow(t *testing.T) {
	if _, err := os.Stat(".github/workflows/ci.yml"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("GitHub Actions workflow still exists or cannot be checked: %v", err)
	}
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{
		"verify: verify-local",
		"verify-local: verify-local-contract verify-go verify-c verify-frontend verify-ops verify-benchmark-md-update",
		"./scripts/verify-local.sh",
		"bench-smoke:",
		"./scripts/benchmark-smoke.sh",
	} {
		if !strings.Contains(string(makefile), token) {
			t.Fatalf("local Makefile verification missing %q", token)
		}
	}
	for _, path := range []string{"scripts/verify-local.sh", "scripts/benchmark-smoke.sh"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s) error = %v", path, err)
		}
		if info.Mode()&0o111 == 0 {
			t.Fatalf("%s is not executable", path)
		}
	}
}
