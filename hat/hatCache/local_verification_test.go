package hatCache

import (
	"os"
	"strings"
	"testing"
)

func TestLocalVerificationAndHostedWorkflowsExist(t *testing.T) {
	for _, path := range []string{".github/workflows/ci.yml", ".github/workflows/release.yml"} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("Stat(%s) error = %v", path, err)
		}
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
