package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestCIWorkflowRunsProjectVerification(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("ReadFile(CI workflow) error = %v", err)
	}
	workflow := string(data)
	for _, token := range []string{
		"actions/setup-go@v5",
		"make verify-go",
		"make verify-c",
		"make verify-ops",
		"actions/setup-node@v4",
		"make frontend-install",
		"make verify-frontend",
		"make docker-build DOCKER_IMAGE=hatrie-cache:ci",
	} {
		if !strings.Contains(workflow, token) {
			t.Fatalf("CI workflow missing %q", token)
		}
	}
}
