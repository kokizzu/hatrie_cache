package hatStorage_test

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

type testEngine struct{}

func (testEngine) Backend() hatStorage.Backend { return hatStorage.BackendLevelDB }
func (testEngine) Path() string                { return "/data/cache" }
func (testEngine) Format() hatStorage.Format   { return hatStorage.FormatBinary }
func (testEngine) Properties() (hatStorage.Properties, error) {
	return hatStorage.Properties{}, nil
}
func (testEngine) Close() error { return nil }

func TestEngineExtendsInspectionAndLifecycleContract(t *testing.T) {
	var engine hatStorage.Engine = testEngine{}
	if engine.Backend() != hatStorage.BackendLevelDB || engine.Path() != "/data/cache" {
		t.Fatalf("Engine = %#v", engine)
	}
}
