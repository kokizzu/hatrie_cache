//go:build mu46

package hatSql

import (
	"testing"

	json "github.com/goccy/go-json"
)

func TestDifferentialCheckpointPayloadSizes(t *testing.T) {
	checkpoint := differentialCheckpointBenchmarkFixture()
	hdf1, err := EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hdf1 payload bytes=%d", len(hdf1))
	t.Logf("json payload bytes=%d", len(legacy))
}
