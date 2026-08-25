package hatSql

import "testing"

type registryTestRuntime struct{ value interface{} }

func (runtime registryTestRuntime) Evaluate(calls []FunctionCall) ([]interface{}, error) {
	values := make([]interface{}, len(calls))
	for index := range values {
		values[index] = runtime.value
	}
	return values, nil
}

func TestRegistryPersistsBeforeInstallingReplacement(t *testing.T) {
	registry := NewRegistry(func(definition FunctionDefinition) (FunctionDefinition, FunctionRuntime, error) {
		return definition, registryTestRuntime{value: definition.Source}, nil
	})
	persisted := 0
	if err := registry.Register(FunctionDefinition{Name: "answer", Source: "42"}, func(definitions []FunctionDefinition) error {
		persisted = len(definitions)
		return nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	values, err := registry.EvaluateSQLFunction("ANSWER", []FunctionCall{{}})
	if err != nil || persisted != 1 || len(values) != 1 || values[0] != "42" {
		t.Fatalf("registry result = %#v/%d/%v, want 42/1/nil", values, persisted, err)
	}
}
