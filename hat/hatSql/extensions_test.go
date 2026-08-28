package hatSql

import "testing"

func TestVersionedFunctionsCapabilitiesAndPlugins(t *testing.T) {
	registry := NewFunctionRegistry()
	if err := registry.Register(VersionedFunction{Package: "math", Name: "double", Version: "v1", Evaluate: func(values []interface{}) (interface{}, error) { return values[0].(int) * 2, nil }}); err != nil {
		t.Fatal(err)
	}
	function, ok := registry.Resolve("math", "double", "v1")
	if !ok {
		t.Fatal("function missing")
	}
	value, err := function.Evaluate([]interface{}{3})
	if err != nil || value != 6 {
		t.Fatalf("function = %v, %v", value, err)
	}
	capabilities := NewCapabilities(FeatureVersionedFunctions, FeaturePlugins)
	if !capabilities.Has(FeaturePlugins) || capabilities.Has(FeatureCDC) {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}
