package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSQLTriggerDefinitionInvalid reports malformed trigger DDL.
	ErrSQLTriggerDefinitionInvalid = errors.New("SQL trigger definition is invalid")
	// ErrSQLTriggerUnsupportedTiming reports a trigger timing that cannot be
	// represented by the caller-owned commit protocol.
	ErrSQLTriggerUnsupportedTiming = errors.New("SQL trigger timing is unsupported")
	// ErrSQLTriggerUnsupportedOperation reports a non-DML trigger operation.
	ErrSQLTriggerUnsupportedOperation = errors.New("SQL trigger operation is unsupported")
)

// SQLTriggerDefinition is the supported row-level CREATE TRIGGER shape. AFTER
// definitions prepare external actions for the transaction coordinator;
// BEFORE definitions validate or transform the primary row event.
type SQLTriggerDefinition struct {
	Name      string
	Timing    string
	Operation string
	Source    string
	Order     int
}

// ParseSQLTriggerDefinition parses a strict row-level CREATE TRIGGER
// statement:
//
//	CREATE TRIGGER name BEFORE|AFTER INSERT|UPDATE|DELETE|REPLACE ON source FOR EACH ROW
//
// Identifiers are currently unquoted. A trailing semicolon is optional, but
// additional statements are rejected.
func ParseSQLTriggerDefinition(source string) (SQLTriggerDefinition, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return SQLTriggerDefinition{}, fmt.Errorf("%w: %v", ErrSQLTriggerDefinitionInvalid, err)
	}
	parser := sqlQueryParser{tokens: tokens}
	if err := parser.expectKeyword("CREATE"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	if err := parser.expectKeyword("TRIGGER"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	name, err := parser.expectIdentifier("a trigger name", nil)
	if err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	var timing string
	switch {
	case parser.keyword("BEFORE"):
		timing = "BEFORE"
		parser.next()
	case parser.keyword("AFTER"):
		timing = "AFTER"
		parser.next()
	default:
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(parser.expected(parser.current(), "BEFORE or AFTER", []string{"BEFORE", "AFTER"}))
	}
	operation, err := parser.expectIdentifier("a trigger operation", []string{"INSERT", "UPDATE", "DELETE", "REPLACE"})
	if err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	operationName := strings.ToUpper(operation.text)
	switch operationName {
	case "INSERT", "UPDATE", "DELETE", "REPLACE":
	default:
		return SQLTriggerDefinition{}, fmt.Errorf("%w: %s", ErrSQLTriggerUnsupportedOperation, operation.text)
	}
	if err := parser.expectKeyword("ON"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	table, err := parser.expectIdentifier("a trigger source", nil)
	if err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	if err := parser.expectKeyword("FOR"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	if err := parser.expectKeyword("EACH"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	if err := parser.expectKeyword("ROW"); err != nil {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(err)
	}
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return SQLTriggerDefinition{}, sqlTriggerDefinitionError(parser.expected(parser.current(), "end of input", nil))
	}
	return SQLTriggerDefinition{
		Name:      name.text,
		Timing:    timing,
		Operation: operationName,
		Source:    table.text,
	}, nil
}

func sqlTriggerDefinitionError(err error) error {
	if err == nil {
		return ErrSQLTriggerDefinitionInvalid
	}
	return fmt.Errorf("%w: %v", ErrSQLTriggerDefinitionInvalid, err)
}

// RegisterDefinition adds an AFTER trigger definition to registry with the
// caller-supplied preparation callback. It keeps trigger side effects behind
// the existing SQLTriggerTransaction atomicity boundary.
func (registry *SQLTriggerRegistry) RegisterDefinition(definition SQLTriggerDefinition, prepare SQLTriggerPrepareFunc) error {
	if registry == nil {
		return ErrSQLTriggerRegistryNil
	}
	if !strings.EqualFold(strings.TrimSpace(definition.Timing), "AFTER") {
		return ErrSQLTriggerUnsupportedTiming
	}
	return registry.Register(SQLTrigger{
		Name:      definition.Name,
		Source:    definition.Source,
		Operation: definition.Operation,
		Order:     definition.Order,
		Prepare:   prepare,
	})
}

// RegisterBeforeDefinition adds a BEFORE trigger definition to registry with
// the caller-supplied pure row transformation callback. The callback runs
// before the primary mutation and cannot change event metadata.
func (registry *SQLTriggerRegistry) RegisterBeforeDefinition(definition SQLTriggerDefinition, before SQLTriggerBeforeFunc) error {
	if registry == nil {
		return ErrSQLTriggerRegistryNil
	}
	if !strings.EqualFold(strings.TrimSpace(definition.Timing), "BEFORE") {
		return ErrSQLTriggerUnsupportedTiming
	}
	return registry.Register(SQLTrigger{
		Name:      definition.Name,
		Source:    definition.Source,
		Operation: definition.Operation,
		Order:     definition.Order,
		Before:    before,
	})
}

// RegisterSQLTrigger parses and registers one supported CREATE TRIGGER
// statement. Trigger execution remains explicit through
// BeginSQLTriggerTransaction, matching the existing caller-owned mutation
// contract.
func (registry *SQLTriggerRegistry) RegisterSQLTrigger(source string, prepare SQLTriggerPrepareFunc) error {
	definition, err := ParseSQLTriggerDefinition(source)
	if err != nil {
		return err
	}
	return registry.RegisterDefinition(definition, prepare)
}

// RegisterSQLBeforeTrigger parses and registers one BEFORE CREATE TRIGGER
// statement. Its callback can reject or transform the cloned row event before
// the caller's primary mutation callback runs.
func (registry *SQLTriggerRegistry) RegisterSQLBeforeTrigger(source string, before SQLTriggerBeforeFunc) error {
	definition, err := ParseSQLTriggerDefinition(source)
	if err != nil {
		return err
	}
	return registry.RegisterBeforeDefinition(definition, before)
}
