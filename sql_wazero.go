package hatriecache

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// sqlWASMFunction is a small numeric scalar ABI. A module must export the SQL
// function name. It is deliberately Wasm, not JavaScript source: JS needs a
// separately versioned compiler and vector ABI.
type sqlWASMFunction struct {
	definition SQLFunctionDefinition
	runtime    wazero.Runtime
	function   api.Function
	result     api.ValueType
	mu         sync.Mutex
}

func (function *sqlWASMFunction) Close() {
	function.mu.Lock()
	defer function.mu.Unlock()
	if function.runtime != nil {
		_ = function.runtime.Close(context.Background())
		function.runtime = nil
		function.function = nil
	}
}

func validateSQLWASMDefinition(definition SQLFunctionDefinition) error {
	if !isSQLIdentifierStart(definition.Name[0]) {
		return fmt.Errorf("invalid SQL function name %q", definition.Name)
	}
	for i, argument := range definition.Arguments {
		if argument == "" || !isSQLIdentifierStart(argument[0]) {
			return fmt.Errorf("invalid SQL function argument %q", argument)
		}
		switch definition.ArgumentTypes[i] {
		case "INTEGER", "NUMBER", "BOOLEAN":
		default:
			return fmt.Errorf("WASM SQL function argument %q must be INTEGER, NUMBER, or BOOLEAN; got %s", argument, definition.ArgumentTypes[i])
		}
	}
	if _, err := base64.StdEncoding.DecodeString(strings.TrimSpace(definition.Source)); err != nil {
		return fmt.Errorf("WASM SQL function source must be standard base64 WebAssembly: %w", err)
	}
	return nil
}

func newSQLWASMFunction(definition SQLFunctionDefinition) (_ sqlFunctionRuntime, err error) {
	wasm, err := base64.StdEncoding.DecodeString(strings.TrimSpace(definition.Source))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	compiled, err := runtime.CompileModule(ctx, wasm)
	if err != nil {
		runtime.Close(ctx)
		return nil, sqlWASMError(definition, "module error: "+err.Error())
	}
	module, err := runtime.InstantiateModule(ctx, compiled, wazero.NewModuleConfig())
	if err != nil {
		runtime.Close(ctx)
		return nil, sqlWASMError(definition, "module instantiation error: "+err.Error())
	}
	function := module.ExportedFunction(definition.Name)
	if function == nil {
		runtime.Close(ctx)
		return nil, sqlWASMError(definition, fmt.Sprintf("module must export function %q", definition.Name))
	}
	parameters, results := function.Definition().ParamTypes(), function.Definition().ResultTypes()
	if len(parameters) != len(definition.Arguments) || len(results) != 1 {
		runtime.Close(ctx)
		return nil, sqlWASMError(definition, fmt.Sprintf("export %q must have %d parameters and exactly one result; got %d parameters and %d results", definition.Name, len(definition.Arguments), len(parameters), len(results)))
	}
	for i, parameter := range parameters {
		if want := sqlWASMValueType(definition.ArgumentTypes[i]); parameter != want {
			runtime.Close(ctx)
			return nil, sqlWASMError(definition, fmt.Sprintf("export %q parameter %d must be Wasm value type %d for SQL %s", definition.Name, i+1, want, definition.ArgumentTypes[i]))
		}
	}
	return &sqlWASMFunction{definition: definition, runtime: runtime, function: function, result: results[0]}, nil
}

func sqlWASMError(definition SQLFunctionDefinition, message string) error {
	return &SQLFunctionError{Definition: definition, Message: "WASM " + message, Line: 1, Column: 1}
}

func sqlWASMValueType(typeName string) api.ValueType {
	switch typeName {
	case "INTEGER":
		return api.ValueTypeI64
	case "NUMBER":
		return api.ValueTypeF64
	case "BOOLEAN":
		return api.ValueTypeI32
	default:
		return api.ValueTypeI64
	}
}

func (function *sqlWASMFunction) Evaluate(calls []SQLFunctionCall) ([]interface{}, error) {
	function.mu.Lock()
	defer function.mu.Unlock()
	if function.function == nil {
		return nil, sqlWASMError(function.definition, "runtime is closed")
	}
	values := make([]interface{}, len(calls))
	for row, call := range calls {
		if len(call.Arguments) != len(function.definition.Arguments) {
			return nil, sqlWASMError(function.definition, fmt.Sprintf("expects %d arguments, got %d", len(function.definition.Arguments), len(call.Arguments)))
		}
		params := make([]uint64, len(call.Arguments))
		for i, value := range call.Arguments {
			if err := sqlFunctionTypeError(function.definition, i, value); err != nil {
				return nil, err
			}
			encoded, err := sqlWASMEncodeValue(function.definition.ArgumentTypes[i], value)
			if err != nil {
				return nil, sqlWASMError(function.definition, fmt.Sprintf("argument %q cannot be passed to WASM: %v", function.definition.Arguments[i], err))
			}
			params[i] = encoded
		}
		result, err := function.function.Call(context.Background(), params...)
		if err != nil {
			return nil, sqlWASMError(function.definition, fmt.Sprintf("runtime error on row %d: %v", row+1, err))
		}
		values[row] = sqlWASMDecodeValue(function.result, result[0])
	}
	return values, nil
}

func sqlWASMEncodeValue(typeName string, value interface{}) (uint64, error) {
	switch typeName {
	case "INTEGER":
		integer, ok := sqlInteger(value)
		if !ok {
			return 0, fmt.Errorf("expected INTEGER, got %s", sqlFunctionValueType(value))
		}
		return uint64(integer), nil
	case "NUMBER":
		number, ok := sqlNumber(value)
		if !ok {
			return 0, fmt.Errorf("expected NUMBER, got %s", sqlFunctionValueType(value))
		}
		return api.EncodeF64(number), nil
	case "BOOLEAN":
		boolean, ok := value.(bool)
		if !ok {
			return 0, fmt.Errorf("expected BOOLEAN, got %s", sqlFunctionValueType(value))
		}
		if boolean {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("unsupported SQL type %s", typeName)
}

func sqlWASMDecodeValue(valueType api.ValueType, encoded uint64) interface{} {
	switch valueType {
	case api.ValueTypeI32:
		return int64(int32(encoded))
	case api.ValueTypeI64:
		return int64(encoded)
	case api.ValueTypeF32:
		return float64(api.DecodeF32(encoded))
	case api.ValueTypeF64:
		return api.DecodeF64(encoded)
	default:
		return nil
	}
}
