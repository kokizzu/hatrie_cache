package hatSql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"hatrie_cache/hat/hatRuntime"
)

var (
	// ErrRuntimeFunctionArgument indicates an unsupported or incorrectly sized
	// SQL argument for a bounded runtime function.
	ErrRuntimeFunctionArgument = errors.New("hatSql: bounded runtime function argument is invalid")
	// ErrRuntimeFunctionInvalid indicates an invalid function registration.
	ErrRuntimeFunctionInvalid = errors.New("hatSql: bounded runtime function is invalid")
	// ErrRuntimeFunctionDuplicate indicates a duplicate normalized function name.
	ErrRuntimeFunctionDuplicate = errors.New("hatSql: bounded runtime function is already registered")
)

// RuntimeFunction binds one validated, immutable hatRuntime program to a SQL
// function name. Registration is explicit; this adapter never changes the
// default SQL function set.
type RuntimeFunction struct {
	FunctionDefinition
	Program hatRuntime.Program
}

// RuntimeFunctionResolver adapts capability-free bounded programs to the
// vectorized SQL FunctionResolver contract. The registry is immutable after
// construction and is safe for concurrent evaluation.
type RuntimeFunctionResolver struct {
	functions map[string]RuntimeFunction
}

// NewRuntimeFunctionResolver creates an immutable bounded-runtime function
// resolver. Names are case-insensitive. A program may omit argument names, but
// explicit names must match the program's argument count.
func NewRuntimeFunctionResolver(functions ...RuntimeFunction) (*RuntimeFunctionResolver, error) {
	resolver := &RuntimeFunctionResolver{functions: make(map[string]RuntimeFunction, len(functions))}
	for _, function := range functions {
		definition := function.FunctionDefinition
		name := normalizeRuntimeFunctionName(definition.Name)
		if name == "" || strings.IndexByte(name, 0) >= 0 || function.Program.InstructionCount() == 0 {
			return nil, fmt.Errorf("%w: name and compiled program are required", ErrRuntimeFunctionInvalid)
		}
		if len(definition.Arguments) > 0 && len(definition.Arguments) != function.Program.ArgumentCount() {
			return nil, fmt.Errorf("%w: function %q declares %d arguments but program accepts %d", ErrRuntimeFunctionInvalid, name, len(definition.Arguments), function.Program.ArgumentCount())
		}
		if len(definition.ArgumentTypes) > 0 && len(definition.ArgumentTypes) != function.Program.ArgumentCount() {
			return nil, fmt.Errorf("%w: function %q declares %d argument types but program accepts %d", ErrRuntimeFunctionInvalid, name, len(definition.ArgumentTypes), function.Program.ArgumentCount())
		}
		definition.Name = name
		if definition.Language == "" {
			definition.Language = "hatruntime"
		}
		if err := NormalizeFunctionCapabilities(&definition); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrRuntimeFunctionInvalid, err)
		}
		if _, exists := resolver.functions[name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrRuntimeFunctionDuplicate, name)
		}
		function.FunctionDefinition = definition
		resolver.functions[name] = function
	}
	return resolver, nil
}

// EvaluateSQLFunction executes one bounded program for every vectorized SQL
// call. It returns ErrSQLFunctionNotHandled for unknown names so callers can
// compose this resolver with other SQL function providers.
func (resolver *RuntimeFunctionResolver) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if resolver == nil {
		return nil, ErrSQLFunctionNotHandled
	}
	function, ok := resolver.functions[normalizeRuntimeFunctionName(name)]
	if !ok {
		return nil, ErrSQLFunctionNotHandled
	}
	argumentCount := function.Program.ArgumentCount()
	if argumentCount > 0 && len(calls) > int(^uint(0)>>1)/argumentCount {
		return nil, fmt.Errorf("%w: function %q call batch is too large", ErrRuntimeFunctionArgument, function.Name)
	}
	argumentValues := make([]hatRuntime.Value, len(calls)*argumentCount)
	arguments := make([][]hatRuntime.Value, len(calls))
	for callIndex, call := range calls {
		if len(call.Arguments) != argumentCount {
			return nil, fmt.Errorf("%w: function %q expects %d arguments, got %d", ErrRuntimeFunctionArgument, function.Name, argumentCount, len(call.Arguments))
		}
		converted := argumentValues[callIndex*argumentCount : (callIndex+1)*argumentCount]
		for argumentIndex, argument := range call.Arguments {
			value, err := runtimeValueFromSQL(argument)
			if err != nil {
				return nil, fmt.Errorf("%w: function %q argument %d: %v", ErrRuntimeFunctionArgument, function.Name, argumentIndex+1, err)
			}
			converted[argumentIndex] = value
		}
		arguments[callIndex] = converted
	}
	values, err := hatRuntime.ExecuteBatch(context.Background(), function.Program, arguments)
	if err != nil {
		if errors.Is(err, hatRuntime.ErrTypeMismatch) {
			return nil, fmt.Errorf("%w: function %q: %v", ErrRuntimeFunctionArgument, function.Name, err)
		}
		return nil, fmt.Errorf("SQL runtime function %q: %w", function.Name, err)
	}
	results := make([]interface{}, len(values))
	for index, value := range values {
		results[index] = sqlValueFromRuntime(value)
	}
	return results, nil
}

// FunctionCapabilities implements FunctionCapabilityResolver.
func (resolver *RuntimeFunctionResolver) FunctionCapabilities(name string) (deterministic, pure, ok bool) {
	if resolver == nil {
		return false, false, false
	}
	function, ok := resolver.functions[normalizeRuntimeFunctionName(name)]
	if !ok {
		return false, false, false
	}
	return function.Deterministic, function.Pure, true
}

func normalizeRuntimeFunctionName(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

func runtimeValueFromSQL(value interface{}) (hatRuntime.Value, error) {
	switch value := value.(type) {
	case nil:
		return hatRuntime.Null(), nil
	case bool:
		return hatRuntime.Bool(value), nil
	case string:
		return hatRuntime.Text(value), nil
	case []byte:
		return hatRuntime.Text(string(value)), nil
	case int:
		return hatRuntime.Int64(int64(value)), nil
	case int8:
		return hatRuntime.Int64(int64(value)), nil
	case int16:
		return hatRuntime.Int64(int64(value)), nil
	case int32:
		return hatRuntime.Int64(int64(value)), nil
	case int64:
		return hatRuntime.Int64(value), nil
	case uint:
		return runtimeUint64Value(uint64(value))
	case uint8:
		return runtimeUint64Value(uint64(value))
	case uint16:
		return runtimeUint64Value(uint64(value))
	case uint32:
		return runtimeUint64Value(uint64(value))
	case uint64:
		return runtimeUint64Value(value)
	case float32, float64:
		return hatRuntime.Value{}, fmt.Errorf("floating-point arguments are not supported")
	default:
		return hatRuntime.Value{}, fmt.Errorf("%T arguments are not supported", value)
	}
}

func runtimeUint64Value(value uint64) (hatRuntime.Value, error) {
	if value > math.MaxInt64 {
		return hatRuntime.Value{}, fmt.Errorf("unsigned integer %d overflows int64", value)
	}
	return hatRuntime.Int64(int64(value)), nil
}

func sqlValueFromRuntime(value hatRuntime.Value) interface{} {
	switch value.Kind() {
	case hatRuntime.ValueNull:
		return nil
	case hatRuntime.ValueBool:
		result, _ := value.Bool()
		return result
	case hatRuntime.ValueInt64:
		result, _ := value.Int64()
		return result
	case hatRuntime.ValueString:
		result, _ := value.Text()
		return result
	default:
		return nil
	}
}
