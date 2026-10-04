//go:build luajit

package hatCache

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aarzilli/golua/lua"
)

// sqlLuaFunction owns a deliberately unprivileged LuaJIT state. It does not
// open Lua's standard libraries: UDF source has no filesystem, network,
// process, module-loader, debug, or FFI access. One Lua call evaluates the
// whole batch, avoiding a Go<->Lua call for every SQL row.
type sqlLuaFunction struct {
	definition SQLFunctionDefinition
	state      *lua.State
	maxBatch   int
	budget     sqlLuaBudget
	mu         sync.Mutex
}

const sqlLuaHookInterval = 100_000

type sqlLuaBudget struct {
	maxInstructions  int
	memoryLimit      uint64
	hookInterval     int
	memoryCheckEvery int
	instructions     int
	hookCount        int
}

func (budget *sqlLuaBudget) hook(state *lua.State) {
	budget.instructions += budget.hookInterval
	budget.hookCount++
	if budget.maxInstructions > 0 && budget.instructions >= budget.maxInstructions {
		state.RaiseError(lua.ExecutionQuantumExceeded)
	}
	if budget.memoryLimit > 0 && budget.hookCount%budget.memoryCheckEvery == 0 && sqlLuaMemoryBytes(state) > budget.memoryLimit {
		state.RaiseError("Lua memory limit exceeded")
	}
}

func sqlLuaMemoryBytes(state *lua.State) uint64 {
	kilobytes := state.GC(lua.LUA_GCCOUNT, 0)
	if kilobytes < 0 {
		return 0
	}
	return uint64(kilobytes) * 1024
}

func sqlLuaAddEstimate(total *uint64, extra uint64) {
	if ^uint64(0)-*total < extra {
		*total = ^uint64(0)
		return
	}
	*total += extra
}

func (function *sqlLuaFunction) Close() {
	function.mu.Lock()
	defer function.mu.Unlock()
	if function.state != nil {
		function.state.Close()
		function.state = nil
	}
}

func newSQLLuaFunction(definition SQLFunctionDefinition, options SQLFunctionRegistryOptions) (_ sqlFunctionRuntime, err error) {
	hookInterval := sqlLuaHookInterval
	if options.LuaExecutionLimit > 0 && options.LuaExecutionLimit < hookInterval {
		hookInterval = options.LuaExecutionLimit
	}
	if options.LuaMemoryLimitBytes > 0 && options.LuaMemoryLimitBytes < 1<<20 && hookInterval > 10_000 {
		hookInterval = 10_000
	}
	memoryCheckEvery := 16
	if options.LuaMemoryLimitBytes > 0 && options.LuaMemoryLimitBytes < 1<<20 {
		memoryCheckEvery = 2
	}
	function := &sqlLuaFunction{
		definition: definition,
		maxBatch:   options.LuaMaxBatchCalls,
		budget:     sqlLuaBudget{maxInstructions: options.LuaExecutionLimit, memoryLimit: options.LuaMemoryLimitBytes, hookInterval: hookInterval, memoryCheckEvery: memoryCheckEvery},
		state:      lua.NewState(),
	}
	if function.state == nil {
		return nil, fmt.Errorf("SQL function %q could not create a LuaJIT state", definition.Name)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			function.state.Close()
			err = &SQLFunctionError{Definition: definition, Message: fmt.Sprintf("LuaJIT setup failed: %v", recovered), Line: 1, Column: 1}
		}
	}()
	if luaErr := function.state.DoString(sqlLuaBatchProgram(definition)); luaErr != nil {
		function.state.Close()
		return nil, &SQLFunctionError{Definition: definition, Message: "LuaJIT source error: " + luaErr.Error(), Line: 1, Column: 1}
	}
	function.state.SetHook(function.budget.hook, function.budget.hookInterval)
	return function, nil
}

func sqlLuaBatchProgram(definition SQLFunctionDefinition) string {
	arguments := strings.Join(definition.Arguments, ", ")
	callArguments := make([]string, len(definition.Arguments))
	for index := range callArguments {
		callArguments[index] = fmt.Sprintf("calls[i][%d]", index+1)
	}
	return "local function __hatrie_udf(" + arguments + ")\n" + definition.Source + "\nend\n" +
		"function __hatrie_eval(calls)\n local out = {}\n for i = 1, #calls do\n  out[i] = __hatrie_udf(" + strings.Join(callArguments, ", ") + ")\n end\n return out\nend\n"
}

func (function *sqlLuaFunction) Evaluate(calls []SQLFunctionCall) (_ []interface{}, err error) {
	function.mu.Lock()
	defer function.mu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("LuaJIT runtime error: %v", recovered), Line: 1, Column: 1}
		}
	}()
	if function.maxBatch > 0 && len(calls) > function.maxBatch {
		return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("LuaJIT batch contains %d calls, maximum is %d", len(calls), function.maxBatch), Line: 1, Column: 1}
	}
	function.budget.instructions = 0
	function.budget.hookCount = 0
	var estimatedInputBytes uint64
	for _, call := range calls {
		if len(call.Arguments) != len(function.definition.Arguments) {
			return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("expects %d arguments, got %d", len(function.definition.Arguments), len(call.Arguments)), Line: 1, Column: 1}
		}
		for index, value := range call.Arguments {
			switch typed := value.(type) {
			case string:
				sqlLuaAddEstimate(&estimatedInputBytes, uint64(len(typed)))
			case []byte:
				sqlLuaAddEstimate(&estimatedInputBytes, uint64(len(typed)))
			}
			if typeErr := sqlFunctionTypeError(function.definition, index, value); typeErr != nil {
				return nil, typeErr
			}
		}
	}
	if function.budget.memoryLimit > 0 && estimatedInputBytes > function.budget.memoryLimit {
		return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("LuaJIT input exceeds memory limit of %d bytes", function.budget.memoryLimit), Line: 1, Column: 1}
	}
	state := function.state
	if state == nil {
		return nil, &SQLFunctionError{Definition: function.definition, Message: "LuaJIT runtime is closed", Line: 1, Column: 1}
	}
	state.GetGlobal("__hatrie_eval")
	state.CreateTable(len(calls), 0)
	for row, call := range calls {
		state.CreateTable(len(call.Arguments), 0)
		for index, value := range call.Arguments {
			if pushErr := sqlLuaPushValue(state, value); pushErr != nil {
				state.Pop(2)
				return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("argument %q cannot be passed to LuaJIT: %v", function.definition.Arguments[index], pushErr), Line: 1, Column: 1}
			}
			state.RawSeti(-2, index+1)
		}
		state.RawSeti(-2, row+1)
	}
	if callErr := state.Call(1, 1); callErr != nil {
		return nil, &SQLFunctionError{Definition: function.definition, Message: "LuaJIT runtime error: " + callErr.Error(), Line: 1, Column: 1}
	}
	defer state.Pop(1)
	values := make([]interface{}, len(calls))
	for row := range calls {
		state.RawGeti(-1, row+1)
		value, convertErr := sqlLuaValue(state, -1)
		state.Pop(1)
		if convertErr != nil {
			return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("LuaJIT result for row %d: %v", row+1, convertErr), Line: 1, Column: 1}
		}
		values[row] = value
	}
	return values, nil
}

func sqlLuaPushValue(state *lua.State, value interface{}) error {
	switch typed := value.(type) {
	case nil:
		state.PushNil()
	case bool:
		state.PushBoolean(typed)
	case int:
		state.PushInteger(int64(typed))
	case int64:
		state.PushInteger(typed)
	case float32:
		state.PushNumber(float64(typed))
	case float64:
		state.PushNumber(typed)
	case string:
		state.PushString(typed)
	case []byte:
		state.PushBytes(typed)
	default:
		return fmt.Errorf("unsupported %s value", sqlFunctionValueType(value))
	}
	return nil
}

func sqlLuaValue(state *lua.State, index int) (interface{}, error) {
	switch state.Type(index) {
	case lua.LUA_TNIL:
		return nil, nil
	case lua.LUA_TBOOLEAN:
		return state.ToBoolean(index), nil
	case lua.LUA_TNUMBER:
		value := state.ToNumber(index)
		if float64(int64(value)) == value {
			return int64(value), nil
		}
		return value, nil
	case lua.LUA_TSTRING:
		return state.ToString(index), nil
	default:
		return nil, fmt.Errorf("unsupported Lua value type %d result", state.Type(index))
	}
}
