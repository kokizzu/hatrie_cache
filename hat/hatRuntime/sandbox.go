// Package hatRuntime provides small, capability-free runtimes for untrusted
// deterministic expressions. Programs contain only typed values and control
// flow; they cannot call Go, access files, or reach the network.
package hatRuntime

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

var (
	ErrProgramInvalid    = errors.New("hatrie runtime: program is invalid")
	ErrArgumentInvalid   = errors.New("hatrie runtime: argument is invalid")
	ErrTypeMismatch      = errors.New("hatrie runtime: value type mismatch")
	ErrStackLimit        = errors.New("hatrie runtime: stack limit exceeded")
	ErrValueLimit        = errors.New("hatrie runtime: value limit exceeded")
	ErrStepLimit         = errors.New("hatrie runtime: step limit exceeded")
	ErrExecutionCanceled = errors.New("hatrie runtime: execution canceled")
	ErrIntegerOverflow   = errors.New("hatrie runtime: integer overflow")
)

// ValueKind is the closed set of values accepted by the runtime.
type ValueKind uint8

const (
	ValueNull ValueKind = iota
	ValueBool
	ValueInt64
	ValueString
)

func (kind ValueKind) String() string {
	switch kind {
	case ValueNull:
		return "null"
	case ValueBool:
		return "bool"
	case ValueInt64:
		return "int64"
	case ValueString:
		return "string"
	default:
		return "unknown"
	}
}

// Value is an immutable-by-convention scalar passed into and out of a Program.
// Constructors are intentionally the only way to create non-zero values.
type Value struct {
	kind   ValueKind
	bool   bool
	int64  int64
	string string
}

// Null returns the null value.
func Null() Value { return Value{kind: ValueNull} }

// Bool returns a boolean value.
func Bool(value bool) Value { return Value{kind: ValueBool, bool: value} }

// Int64 returns a signed integer value.
func Int64(value int64) Value { return Value{kind: ValueInt64, int64: value} }

// Text returns a bounded-runtime string value. The program's configured value
// limit is checked when the value is executed.
func Text(value string) Value { return Value{kind: ValueString, string: value} }

// Kind returns the value's scalar kind.
func (value Value) Kind() ValueKind { return value.kind }

// Bool returns the underlying boolean and whether the value is boolean.
func (value Value) Bool() (bool, bool) {
	if value.kind != ValueBool {
		return false, false
	}
	return value.bool, true
}

// Int64 returns the underlying integer and whether the value is an integer.
func (value Value) Int64() (int64, bool) {
	if value.kind != ValueInt64 {
		return 0, false
	}
	return value.int64, true
}

// Text returns the underlying string and whether the value is a string.
func (value Value) Text() (string, bool) {
	if value.kind != ValueString {
		return "", false
	}
	return value.string, true
}

func (value Value) String() string {
	switch value.kind {
	case ValueNull:
		return "null"
	case ValueBool:
		if value.bool {
			return "true"
		}
		return "false"
	case ValueInt64:
		return strconv.FormatInt(value.int64, 10)
	case ValueString:
		return value.string
	default:
		return "<invalid>"
	}
}

// OpCode is one instruction in a bounded Program.
type OpCode uint8

const (
	OpPushNull OpCode = iota + 1
	OpPushBool
	OpPushInt
	OpPushString
	OpLoadArg
	OpAddInt
	OpEqual
	OpConcat
	OpJump
	OpJumpIfFalse
	OpReturn
)

// Instruction is a scalar instruction. Operand is used for integer, boolean,
// argument, and absolute jump operands; Text is used only by OpPushString.
type Instruction struct {
	Op      OpCode
	Operand int64
	Text    string
}

// Limits bounds both compiled program shape and one execution.
type Limits struct {
	MaxArguments    int
	MaxInstructions int
	MaxStack        int
	MaxStringBytes  int
	MaxSteps        uint64
}

const (
	DefaultMaxArguments    = 32
	DefaultMaxInstructions = 4096
	DefaultMaxStack        = 256
	DefaultMaxStringBytes  = 64 << 10
	DefaultMaxSteps        = 1_000_000

	maxArgumentsCap    = 256
	maxInstructionsCap = 65_536
	maxStackCap        = 4096
	maxStringBytesCap  = 1 << 20
	maxStepsCap        = 100_000_000
)

// DefaultLimits returns the safe resource limits used by Compile.
func DefaultLimits() Limits {
	return Limits{
		MaxArguments:    DefaultMaxArguments,
		MaxInstructions: DefaultMaxInstructions,
		MaxStack:        DefaultMaxStack,
		MaxStringBytes:  DefaultMaxStringBytes,
		MaxSteps:        DefaultMaxSteps,
	}
}

func normalizeLimits(limits Limits) (Limits, error) {
	defaults := DefaultLimits()
	if limits.MaxArguments == 0 {
		limits.MaxArguments = defaults.MaxArguments
	}
	if limits.MaxInstructions == 0 {
		limits.MaxInstructions = defaults.MaxInstructions
	}
	if limits.MaxStack == 0 {
		limits.MaxStack = defaults.MaxStack
	}
	if limits.MaxStringBytes == 0 {
		limits.MaxStringBytes = defaults.MaxStringBytes
	}
	if limits.MaxSteps == 0 {
		limits.MaxSteps = defaults.MaxSteps
	}
	if limits.MaxArguments < 1 || limits.MaxArguments > maxArgumentsCap ||
		limits.MaxInstructions < 1 || limits.MaxInstructions > maxInstructionsCap ||
		limits.MaxStack < 1 || limits.MaxStack > maxStackCap ||
		limits.MaxStringBytes < 1 || limits.MaxStringBytes > maxStringBytesCap ||
		limits.MaxSteps > maxStepsCap {
		return Limits{}, fmt.Errorf("%w: limits exceed bounded runtime", ErrProgramInvalid)
	}
	return limits, nil
}

// Program is a validated immutable instruction sequence.
type Program struct {
	arguments    int
	instructions []Instruction
	limits       Limits
}

// Compile validates a program with DefaultLimits.
func Compile(arguments int, instructions []Instruction) (Program, error) {
	return CompileWithLimits(DefaultLimits(), arguments, instructions)
}

// CompileWithLimits validates and copies a program under explicit limits.
func CompileWithLimits(limits Limits, arguments int, instructions []Instruction) (Program, error) {
	limits, err := normalizeLimits(limits)
	if err != nil {
		return Program{}, err
	}
	if arguments < 0 || arguments > limits.MaxArguments {
		return Program{}, fmt.Errorf("%w: argument count %d", ErrProgramInvalid, arguments)
	}
	if len(instructions) == 0 || len(instructions) > limits.MaxInstructions {
		return Program{}, fmt.Errorf("%w: instruction count %d", ErrProgramInvalid, len(instructions))
	}
	program := Program{
		arguments:    arguments,
		instructions: append([]Instruction(nil), instructions...),
		limits:       limits,
	}
	hasReturn := false
	for index, instruction := range program.instructions {
		switch instruction.Op {
		case OpPushNull, OpAddInt, OpEqual, OpConcat, OpReturn:
			if instruction.Operand != 0 || instruction.Text != "" {
				return Program{}, fmt.Errorf("%w: instruction %d has unexpected operands", ErrProgramInvalid, index)
			}
		case OpPushBool:
			if instruction.Operand != 0 && instruction.Operand != 1 || instruction.Text != "" {
				return Program{}, fmt.Errorf("%w: instruction %d boolean operand", ErrProgramInvalid, index)
			}
		case OpPushInt:
			if instruction.Text != "" {
				return Program{}, fmt.Errorf("%w: instruction %d integer text", ErrProgramInvalid, index)
			}
		case OpPushString:
			if len(instruction.Text) > limits.MaxStringBytes {
				return Program{}, fmt.Errorf("%w: instruction %d string bytes", ErrProgramInvalid, index)
			}
			if instruction.Operand != 0 {
				return Program{}, fmt.Errorf("%w: instruction %d string operand", ErrProgramInvalid, index)
			}
		case OpLoadArg:
			if instruction.Operand < 0 || instruction.Operand >= int64(arguments) || instruction.Text != "" {
				return Program{}, fmt.Errorf("%w: instruction %d argument index", ErrProgramInvalid, index)
			}
		case OpJump, OpJumpIfFalse:
			if instruction.Operand < 0 || instruction.Operand >= int64(len(program.instructions)) || instruction.Text != "" {
				return Program{}, fmt.Errorf("%w: instruction %d jump target", ErrProgramInvalid, index)
			}
		default:
			return Program{}, fmt.Errorf("%w: instruction %d opcode %d", ErrProgramInvalid, index, instruction.Op)
		}
		if instruction.Op == OpReturn {
			hasReturn = true
		}
	}
	if !hasReturn {
		return Program{}, fmt.Errorf("%w: program has no return", ErrProgramInvalid)
	}
	return program, nil
}

// ArgumentCount returns the number of arguments accepted by the program.
func (program Program) ArgumentCount() int { return program.arguments }

// InstructionCount returns the number of validated instructions.
func (program Program) InstructionCount() int { return len(program.instructions) }

// Execute evaluates one program invocation. The context is checked at bounded
// intervals and the program's step/stack/value limits are always enforced.
func Execute(ctx context.Context, program Program, arguments []Value) (Value, error) {
	if ctx == nil {
		return Value{}, fmt.Errorf("%w: nil context", ErrExecutionCanceled)
	}
	if len(program.instructions) == 0 || program.arguments < 0 || program.arguments > program.limits.MaxArguments {
		return Value{}, ErrProgramInvalid
	}
	if len(arguments) != program.arguments {
		return Value{}, fmt.Errorf("%w: got %d arguments, want %d", ErrArgumentInvalid, len(arguments), program.arguments)
	}
	for index, argument := range arguments {
		if argument.kind > ValueString {
			return Value{}, fmt.Errorf("%w: argument %d kind", ErrArgumentInvalid, index)
		}
		if argument.kind == ValueString && len(argument.string) > program.limits.MaxStringBytes {
			return Value{}, fmt.Errorf("%w: argument %d string bytes", ErrValueLimit, index)
		}
	}
	stack := make([]Value, 0, minInt(program.limits.MaxStack, len(program.instructions)))
	pc := 0
	var steps uint64
	for pc >= 0 && pc < len(program.instructions) {
		if steps >= program.limits.MaxSteps {
			return Value{}, ErrStepLimit
		}
		if steps&63 == 0 {
			select {
			case <-ctx.Done():
				return Value{}, fmt.Errorf("%w: %v", ErrExecutionCanceled, ctx.Err())
			default:
			}
		}
		steps++
		instruction := program.instructions[pc]
		switch instruction.Op {
		case OpPushNull:
			var err error
			stack, err = pushValue(stack, Null(), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpPushBool:
			var err error
			stack, err = pushValue(stack, Bool(instruction.Operand == 1), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpPushInt:
			var err error
			stack, err = pushValue(stack, Int64(instruction.Operand), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpPushString:
			var err error
			stack, err = pushValue(stack, Text(instruction.Text), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpLoadArg:
			var err error
			stack, err = pushValue(stack, arguments[instruction.Operand], program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpAddInt:
			left, right, err := popIntPair(&stack)
			if err != nil {
				return Value{}, err
			}
			if (right > 0 && left > maxInt64-right) || (right < 0 && left < minInt64-right) {
				return Value{}, ErrIntegerOverflow
			}
			stack, err = pushValue(stack, Int64(left+right), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpEqual:
			left, right, err := popPair(&stack)
			if err != nil {
				return Value{}, err
			}
			stack, err = pushValue(stack, Bool(valuesEqual(left, right)), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpConcat:
			left, right, err := popPair(&stack)
			if err != nil {
				return Value{}, err
			}
			if left.kind != ValueString || right.kind != ValueString {
				return Value{}, ErrTypeMismatch
			}
			if len(left.string) > program.limits.MaxStringBytes-len(right.string) {
				return Value{}, ErrValueLimit
			}
			stack, err = pushValue(stack, Text(left.string+right.string), program.limits.MaxStack)
			if err != nil {
				return Value{}, err
			}
		case OpJump:
			pc = int(instruction.Operand)
			continue
		case OpJumpIfFalse:
			condition, err := popValue(&stack)
			if err != nil {
				return Value{}, err
			}
			if condition.kind != ValueBool {
				return Value{}, ErrTypeMismatch
			}
			if !condition.bool {
				pc = int(instruction.Operand)
				continue
			}
		case OpReturn:
			if len(stack) != 1 {
				return Value{}, fmt.Errorf("%w: return stack depth %d", ErrProgramInvalid, len(stack))
			}
			return stack[0], nil
		default:
			return Value{}, ErrProgramInvalid
		}
		pc++
	}
	return Value{}, ErrProgramInvalid
}

// ExecuteBatch evaluates each row independently under the program limits.
func ExecuteBatch(ctx context.Context, program Program, arguments [][]Value) ([]Value, error) {
	results := make([]Value, len(arguments))
	for index, row := range arguments {
		result, err := Execute(ctx, program, row)
		if err != nil {
			return nil, fmt.Errorf("batch row %d: %w", index, err)
		}
		results[index] = result
	}
	return results, nil
}

const (
	maxInt64 = int64(1<<63 - 1)
	minInt64 = -1 << 63
)

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func pushValue(stack []Value, value Value, limit int) ([]Value, error) {
	if len(stack) >= limit {
		return stack, ErrStackLimit
	}
	return append(stack, value), nil
}

func popValue(stack *[]Value) (Value, error) {
	values := *stack
	if len(values) == 0 {
		return Value{}, ErrStackLimit
	}
	value := values[len(values)-1]
	*stack = values[:len(values)-1]
	return value, nil
}

func popPair(stack *[]Value) (Value, Value, error) {
	right, err := popValue(stack)
	if err != nil {
		return Value{}, Value{}, err
	}
	left, err := popValue(stack)
	if err != nil {
		return Value{}, Value{}, err
	}
	return left, right, nil
}

func popIntPair(stack *[]Value) (int64, int64, error) {
	left, right, err := popPair(stack)
	if err != nil {
		return 0, 0, err
	}
	if left.kind != ValueInt64 || right.kind != ValueInt64 {
		return 0, 0, ErrTypeMismatch
	}
	return left.int64, right.int64, nil
}

func valuesEqual(left, right Value) bool {
	if left.kind != right.kind {
		return false
	}
	switch left.kind {
	case ValueNull:
		return true
	case ValueBool:
		return left.bool == right.bool
	case ValueInt64:
		return left.int64 == right.int64
	case ValueString:
		return left.string == right.string
	default:
		return false
	}
}
