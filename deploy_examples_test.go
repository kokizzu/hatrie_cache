package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestDeployTopologyExamplesAreValid(t *testing.T) {
	fullReplica, err := LoadTopology("deploy/topology/full-replica.json")
	if err != nil {
		t.Fatalf("LoadTopology(full-replica) error = %v", err)
	}
	if fullReplica.Mode != TopologyModeFullReplica || len(fullReplica.Nodes) != 2 {
		t.Fatalf("full replica topology = %#v, want two full-replica nodes", fullReplica)
	}

	sharded, err := LoadTopology("deploy/topology/sharded.json")
	if err != nil {
		t.Fatalf("LoadTopology(sharded) error = %v", err)
	}
	if sharded.Mode != TopologyModeSharded || sharded.BucketCount != 1024 || len(sharded.Shards) != 2 || len(sharded.BucketRanges) != 2 {
		t.Fatalf("sharded topology = %#v, want two shards over 1024 buckets", sharded)
	}
	if sharded.BucketRanges[0].Start != 0 || sharded.BucketRanges[1].End != 1023 {
		t.Fatalf("bucket ranges = %#v, want full 0..1023 coverage", sharded.BucketRanges)
	}
}

func TestDeployServiceAndComposeExamplesExposeDurableRuntime(t *testing.T) {
	service := readDeployExample(t, "deploy/systemd/hatrie-cache.service")
	for _, token := range []string{
		"-monitoring-server",
		"-snapshot-path /var/lib/hatrie-cache/snapshot.hc",
		"-journal-path /var/lib/hatrie-cache/commands.journal",
		"-db-path /var/lib/hatrie-cache/cache.leveldb",
		"Restart=on-failure",
		"LimitNOFILE=1048576",
	} {
		if !strings.Contains(service, token) {
			t.Fatalf("systemd example missing %q", token)
		}
	}

	compose := readDeployExample(t, "deploy/docker-compose.yml")
	for _, token := range []string{
		"build:",
		"dockerfile: Dockerfile",
		"node-a:",
		"node-b:",
		"-monitoring-server",
		"-journal-pull-source",
		"http://node-a:8080",
		"./topology/full-replica.json:/etc/hatrie-cache/topology.json:ro",
		"node-a-data:",
		"node-b-data:",
	} {
		if !strings.Contains(compose, token) {
			t.Fatalf("compose example missing %q", token)
		}
	}
}

func TestProductionDockerfileAndBuildScript(t *testing.T) {
	dockerfile := readDeployExample(t, "Dockerfile")
	for _, token := range []string{
		"FROM node:",
		"FROM golang:",
		"FROM debian:",
		"pnpm run build",
		"CGO_ENABLED=1 go build",
		"USER hatrie-cache",
		"HEALTHCHECK",
		"HATRIE_HEALTHCHECK_ADDR",
		"MONITORING_AUTH_TOKEN",
		"ENTRYPOINT [\"/usr/local/bin/hatrie-cache\"]",
		"/var/lib/hatrie-cache",
		"/app/svelte-mpa/dist",
	} {
		if !strings.Contains(dockerfile, token) {
			t.Fatalf("Dockerfile missing %q", token)
		}
	}

	script := readDeployExample(t, "scripts/docker-build.sh")
	for _, token := range []string{
		"DOCKER_IMAGE",
		"DOCKER_PLATFORM",
		"docker build",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("docker build script missing %q", token)
		}
	}

	makefile := readDeployExample(t, "Makefile")
	for _, token := range []string{
		"docker-build:",
		"./scripts/docker-build.sh",
	} {
		if !strings.Contains(makefile, token) {
			t.Fatalf("Makefile missing docker build token %q", token)
		}
	}
}

func TestReadmeLinksDeployExamples(t *testing.T) {
	readme := readDeployExample(t, "README.md")
	for _, token := range []string{
		"make docker-build",
		"Dockerfile",
		"deploy/systemd/hatrie-cache.service",
		"deploy/topology/full-replica.json",
		"deploy/topology/sharded.json",
		"deploy/docker-compose.yml",
	} {
		if !strings.Contains(readme, token) {
			t.Fatalf("README missing deploy example link %q", token)
		}
	}
}

func readDeployExample(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(data)
}
