package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const sqlJSMaximumJSONBytes = 16 << 20

var (
	sqlJSSourcePosition    = regexp.MustCompile(`:(\d+):(\d+)`)
	sqlJSModuleSequence    uint64
	errSQLJSOutputTooLarge = errors.New("JavaScript UDF output exceeds 16 MiB batch limit")
)

// sqlJSFunction compiles JavaScript through Javy once, then runs the resulting
// WASI module in Wazero. No host filesystem, networking, process, or FFI is
// mounted for function execution.
type sqlJSFunction struct {
	definition SQLFunctionDefinition
	runtime    wazero.Runtime
	module     wazero.CompiledModule
	timeout    time.Duration
	mu         sync.Mutex
}

func validateSQLJSDefinition(definition SQLFunctionDefinition) error {
	if !isSQLIdentifierStart(definition.Name[0]) {
		return fmt.Errorf("invalid SQL function name %q", definition.Name)
	}
	seen := make(map[string]struct{}, len(definition.Arguments))
	for _, argument := range definition.Arguments {
		if argument == "" || !isSQLIdentifierStart(argument[0]) {
			return fmt.Errorf("invalid SQL function argument %q", argument)
		}
		if strings.HasPrefix(strings.ToLower(argument), "__hatrie_") {
			return fmt.Errorf("JavaScript SQL function argument %q uses reserved __hatrie_ prefix", argument)
		}
		key := strings.ToUpper(argument)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate SQL function argument %q", argument)
		}
		seen[key] = struct{}{}
	}
	if strings.TrimSpace(definition.Source) == "" {
		return fmt.Errorf("JavaScript SQL function source must not be empty")
	}
	if !regexp.MustCompile(`\breturn\b`).MatchString(definition.Source) {
		return &SQLFunctionError{Definition: definition, Message: "JavaScript SQL function source must contain a return statement", Line: 1, Column: 1}
	}
	return nil
}

func newSQLJSFunction(definition SQLFunctionDefinition, options SQLFunctionRegistryOptions) (_ sqlFunctionRuntime, err error) {
	compiler, err := sqlJSCompilerPath(options.JavyPath)
	if err != nil {
		return nil, sqlJSError(definition, err.Error(), 1, 1)
	}
	directory, err := os.MkdirTemp("", "hatrie-sql-js-")
	if err != nil {
		return nil, sqlJSError(definition, "cannot create isolated JavaScript compiler directory: "+err.Error(), 1, 1)
	}
	defer os.RemoveAll(directory)
	wrapper, sourceLine := sqlJSWrapper(definition)
	inputPath := filepath.Join(directory, "function.js")
	outputPath := filepath.Join(directory, "function.wasm")
	if err := os.WriteFile(inputPath, []byte(wrapper), 0o600); err != nil {
		return nil, sqlJSError(definition, "cannot write JavaScript compiler input: "+err.Error(), 1, 1)
	}
	compileContext, cancel := context.WithTimeout(context.Background(), options.JSCompileTimeout)
	defer cancel()
	command := exec.CommandContext(compileContext, compiler, "build", inputPath, "-o", outputPath)
	command.Dir = directory
	diagnostic, commandErr := command.CombinedOutput()
	if compileContext.Err() != nil {
		return nil, sqlJSError(definition, "JavaScript compilation exceeded "+options.JSCompileTimeout.String(), 1, 1)
	}
	if commandErr != nil {
		line, column := sqlJSDiagnosticPosition(diagnostic, sourceLine)
		return nil, sqlJSError(definition, "JavaScript compilation failed: "+sqlJSDiagnosticText(diagnostic, commandErr), line, column)
	}
	wasm, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, sqlJSError(definition, "JavaScript compiler produced no Wasm module: "+err.Error(), 1, 1)
	}
	runtime := wazero.NewRuntimeWithConfig(context.Background(), wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
	if _, err := wasi_snapshot_preview1.Instantiate(context.Background(), runtime); err != nil {
		runtime.Close(context.Background())
		return nil, sqlJSError(definition, "cannot initialize sandboxed WASI runtime: "+err.Error(), 1, 1)
	}
	module, err := runtime.CompileModule(context.Background(), wasm)
	if err != nil {
		runtime.Close(context.Background())
		return nil, sqlJSError(definition, "generated Wasm module is invalid: "+err.Error(), 1, 1)
	}
	return &sqlJSFunction{definition: definition, runtime: runtime, module: module, timeout: options.JSExecutionTimeout}, nil
}

func sqlJSCompilerPath(configured string) (string, error) {
	if configured != "" {
		info, err := os.Stat(configured)
		if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			return "", fmt.Errorf("JavaScript UDF compiler %q is not an executable Javy binary", configured)
		}
		return configured, nil
	}
	compiler, err := exec.LookPath("javy")
	if err != nil {
		return "", fmt.Errorf("LANGUAGE JS requires the Javy compiler; install javy or set SQLFunctionRegistryOptions.JavyPath")
	}
	return compiler, nil
}

func sqlJSWrapper(definition SQLFunctionDefinition) (string, int) {
	var source strings.Builder
	source.WriteString("const __hatrie_input = __hatrie_readInput();\n")
	source.WriteString("const __hatrie_output = __hatrie_input.map((__hatrie_row) => {\n")
	for index, argument := range definition.Arguments {
		fmt.Fprintf(&source, "  const %s = __hatrie_row[%d];\n", argument, index)
	}
	sourceLine := len(definition.Arguments) + 3
	for _, line := range strings.Split(definition.Source, "\n") {
		source.WriteString("  ")
		source.WriteString(line)
		source.WriteByte('\n')
	}
	source.WriteString("});\n")
	source.WriteString("Javy.IO.writeSync(1, new TextEncoder().encode(JSON.stringify(__hatrie_output)));\n")
	source.WriteString("function __hatrie_readInput() {\n")
	source.WriteString("  const chunks = []; let length = 0;\n")
	source.WriteString("  for (;;) { const chunk = new Uint8Array(8192); const read = Javy.IO.readSync(0, chunk); if (read === 0) break; chunks.push(chunk.subarray(0, read)); length += read; }\n")
	source.WriteString("  const result = new Uint8Array(length); let offset = 0; for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.length; }\n")
	source.WriteString("  return JSON.parse(new TextDecoder().decode(result));\n")
	source.WriteString("}\n")
	return source.String(), sourceLine
}

func (function *sqlJSFunction) Close() {
	function.mu.Lock()
	defer function.mu.Unlock()
	if function.runtime != nil {
		_ = function.module.Close(context.Background())
		_ = function.runtime.Close(context.Background())
		function.runtime = nil
		function.module = nil
	}
}

func (function *sqlJSFunction) Evaluate(calls []SQLFunctionCall) ([]interface{}, error) {
	function.mu.Lock()
	defer function.mu.Unlock()
	if function.runtime == nil || function.module == nil {
		return nil, sqlJSError(function.definition, "JavaScript runtime is closed", 1, 1)
	}
	input, err := sqlJSBatchInput(function.definition, calls)
	if err != nil {
		return nil, err
	}
	executionContext, cancel := context.WithTimeout(context.Background(), function.timeout)
	defer cancel()
	var output sqlJSLimitedBuffer
	output.limit = sqlJSMaximumJSONBytes
	var stderr sqlJSLimitedBuffer
	stderr.limit = 4096
	moduleName := "sql-js-" + strconv.FormatUint(atomic.AddUint64(&sqlJSModuleSequence, 1), 10)
	module, err := function.runtime.InstantiateModule(executionContext, function.module, wazero.NewModuleConfig().WithName(moduleName).WithStdin(bytes.NewReader(input)).WithStdout(&output).WithStderr(&stderr))
	if module != nil {
		defer module.Close(context.Background())
	}
	if err != nil {
		if executionContext.Err() != nil {
			return nil, sqlJSError(function.definition, "JavaScript execution exceeded "+function.timeout.String()+" batch limit", 1, 1)
		}
		if errors.Is(err, errSQLJSOutputTooLarge) {
			return nil, sqlJSError(function.definition, errSQLJSOutputTooLarge.Error(), 1, 1)
		}
		line, column := sqlJSDiagnosticPosition(stderr.Bytes(), len(function.definition.Arguments)+3)
		return nil, sqlJSError(function.definition, "JavaScript runtime error: "+sqlJSDiagnosticText(stderr.Bytes(), err), line, column)
	}
	values, err := sqlJSBatchOutput(output.Bytes(), len(calls))
	if err != nil {
		return nil, sqlJSError(function.definition, "JavaScript output is invalid: "+err.Error(), 1, 1)
	}
	return values, nil
}

func sqlJSBatchInput(definition SQLFunctionDefinition, calls []SQLFunctionCall) ([]byte, error) {
	rows := make([][]interface{}, len(calls))
	for row, call := range calls {
		if len(call.Arguments) != len(definition.Arguments) {
			return nil, sqlJSError(definition, fmt.Sprintf("expects %d arguments, got %d", len(definition.Arguments), len(call.Arguments)), 1, 1)
		}
		values := make([]interface{}, len(call.Arguments))
		for index, value := range call.Arguments {
			if err := sqlFunctionTypeError(definition, index, value); err != nil {
				return nil, err
			}
			converted, err := sqlJSValue(value)
			if err != nil {
				return nil, sqlJSError(definition, fmt.Sprintf("argument %q cannot be passed to JavaScript: %v", definition.Arguments[index], err), 1, 1)
			}
			values[index] = converted
		}
		rows[row] = values
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return nil, sqlJSError(definition, "cannot encode JavaScript batch: "+err.Error(), 1, 1)
	}
	if len(encoded) > sqlJSMaximumJSONBytes {
		return nil, sqlJSError(definition, "JavaScript input exceeds 16 MiB batch limit", 1, 1)
	}
	return encoded, nil
}

func sqlJSValue(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case nil, bool, string:
		return value, nil
	case int:
		return sqlJSInteger(int64(value))
	case int64:
		return sqlJSInteger(value)
	case float32:
		return sqlJSNumber(float64(value))
	case float64:
		return sqlJSNumber(value)
	case map[string]interface{}, []interface{}:
		return value, nil
	default:
		return nil, fmt.Errorf("%s is not JSON-compatible", sqlFunctionValueType(value))
	}
}

func sqlJSInteger(value int64) (int64, error) {
	if value < -9007199254740991 || value > 9007199254740991 {
		return 0, fmt.Errorf("INTEGER %d is outside JavaScript's exact number range; use LANGUAGE GO or WASM", value)
	}
	return value, nil
}

func sqlJSNumber(value float64) (float64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("NUMBER must be finite")
	}
	return value, nil
}

func sqlJSBatchOutput(encoded []byte, rows int) ([]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var values []interface{}
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, errors.New("must contain exactly one JSON value")
	}
	if len(values) != rows {
		return nil, fmt.Errorf("returned %d values for %d input rows", len(values), rows)
	}
	for index, value := range values {
		converted, err := sqlJSOutputValue(value)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", index+1, err)
		}
		values[index] = converted
	}
	return values, nil
}

func sqlJSOutputValue(value interface{}) (interface{}, error) {
	number, ok := value.(json.Number)
	if !ok {
		return value, nil
	}
	if integer, err := number.Int64(); err == nil {
		return integer, nil
	}
	decimal, err := number.Float64()
	if err != nil || math.IsNaN(decimal) || math.IsInf(decimal, 0) {
		return nil, fmt.Errorf("number %q is not finite", number)
	}
	return decimal, nil
}

func sqlJSError(definition SQLFunctionDefinition, message string, line, column int) error {
	return &SQLFunctionError{Definition: definition, Message: message, Line: line, Column: column}
}

func sqlJSDiagnosticPosition(diagnostic []byte, sourceLine int) (int, int) {
	match := sqlJSSourcePosition.FindSubmatch(diagnostic)
	if len(match) != 3 {
		return 1, 1
	}
	line, lineErr := strconv.Atoi(string(match[1]))
	column, columnErr := strconv.Atoi(string(match[2]))
	if lineErr != nil || columnErr != nil || line < sourceLine {
		return 1, 1
	}
	return line - sourceLine + 1, max(column-2, 1)
}

func sqlJSDiagnosticText(diagnostic []byte, fallback error) string {
	message := strings.TrimSpace(string(diagnostic))
	if message == "" {
		message = fallback.Error()
	}
	if len(message) > 4096 {
		message = message[:4096] + "…"
	}
	return message
}

type sqlJSLimitedBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *sqlJSLimitedBuffer) Write(data []byte) (int, error) {
	if buffer.Len()+len(data) > buffer.limit {
		return 0, errSQLJSOutputTooLarge
	}
	return buffer.Buffer.Write(data)
}
