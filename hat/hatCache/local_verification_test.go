package hatCache

import (
	"os"
	"strings"
	"testing"
)

func TestLocalVerificationAndHostedWorkflowPolicy(t *testing.T) {
	if _, err := os.Stat(".github/workflows/release.yml"); err != nil {
		t.Fatalf("Stat(.github/workflows/release.yml) error = %v", err)
	}
	if _, err := os.Stat(".github/workflows/ci.yml"); !os.IsNotExist(err) {
		t.Fatalf("push/PR CI workflow must remain disabled, Stat(.github/workflows/ci.yml) error = %v", err)
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
		"verify-github-ci-disabled:",
		"./scripts/verify-github-ci-disabled.sh",
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
