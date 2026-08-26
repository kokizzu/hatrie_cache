package hatGrpc

import "testing"

func TestConfigureBestSpeedCompression(t *testing.T) {
	if err := ConfigureBestSpeedCompression(); err != nil {
		t.Fatalf("ConfigureBestSpeedCompression() error = %v", err)
	}
}
