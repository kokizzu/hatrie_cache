package hatSql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SQLSubscriptionMode selects the result shape for a SQL subscription
// statement. Snapshot mode publishes complete query results; differential mode
// publishes signed row changes.
type SQLSubscriptionMode string

const (
	SQLSubscriptionModeSnapshot     SQLSubscriptionMode = "snapshot"
	SQLSubscriptionModeDifferential SQLSubscriptionMode = "differential"
)

// ErrSQLSubscriptionStatementSyntax reports a malformed SUBSCRIBE or TAIL
// statement envelope.
var ErrSQLSubscriptionStatementSyntax = errors.New("hatSql: invalid SQL subscription statement")

// SQLSubscriptionStatement is the parsed envelope around an existing query
// subscription definition. Query validation and source resolution remain the
// responsibility of SubscribeSQL or SubscribeDifferentialSQL.
type SQLSubscriptionStatement struct {
	Mode       SQLSubscriptionMode
	Definition QuerySubscriptionDefinition
}

// ParseSQLSubscriptionStatement parses the opt-in SUBSCRIBE and TAIL
// statement forms without changing the existing query grammar.
//
// Supported forms are:
//
//	SUBSCRIBE <query>
//	SUBSCRIBE SNAPSHOT <query>
//	SUBSCRIBE DIFFERENTIAL <query>
//	TAIL <query>
//	TAIL <query> WITH (SNAPSHOT = <bool>, PROGRESS = <bool>, AS OF = <uint>, UP TO = <uint>, DETERMINISTIC = <bool>)
func ParseSQLSubscriptionStatement(source string) (SQLSubscriptionStatement, error) {
	keyword, rest := splitSQLSubscriptionWord(source)
	if keyword == "" {
		return SQLSubscriptionStatement{}, ErrSQLSubscriptionStatementSyntax
	}

	statement := SQLSubscriptionStatement{Mode: SQLSubscriptionModeSnapshot}
	switch {
	case strings.EqualFold(keyword, "TAIL"):
		statement.Mode = SQLSubscriptionModeDifferential
	case strings.EqualFold(keyword, "SUBSCRIBE"):
		modifier, query := splitSQLSubscriptionWord(rest)
		switch {
		case strings.EqualFold(modifier, "SNAPSHOT"):
			statement.Mode = SQLSubscriptionModeSnapshot
			rest = query
		case strings.EqualFold(modifier, "DIFFERENTIAL"):
			statement.Mode = SQLSubscriptionModeDifferential
			rest = query
		}
	default:
		return SQLSubscriptionStatement{}, ErrSQLSubscriptionStatementSyntax
	}

	rest = strings.TrimSpace(rest)
	if rest == "" {
		return SQLSubscriptionStatement{}, ErrSQLSubscriptionStatementSyntax
	}
	if rest[len(rest)-1] != ')' {
		statement.Definition.Query = rest
		return statement, nil
	}

	query, options, err := splitSQLSubscriptionOptions(rest)
	if err != nil {
		return SQLSubscriptionStatement{}, fmt.Errorf("%w: %v", ErrSQLSubscriptionStatementSyntax, err)
	}
	if query == "" {
		return SQLSubscriptionStatement{}, ErrSQLSubscriptionStatementSyntax
	}
	statement.Definition.Query = query
	if err := applySQLSubscriptionOptions(&statement.Definition, options); err != nil {
		return SQLSubscriptionStatement{}, fmt.Errorf("%w: %v", ErrSQLSubscriptionStatementSyntax, err)
	}
	return statement, nil
}

// splitSQLSubscriptionOptions removes one trailing WITH (...) envelope. The
// scan intentionally avoids case conversion so legacy statements stay on the
// parser's allocation-free path.
func splitSQLSubscriptionOptions(source string) (string, map[string]string, error) {
	marker := findSQLSubscriptionOptionsMarker(source)
	if marker < 0 {
		return source, nil, nil
	}
	if marker == 0 || source[len(source)-1] != ')' {
		return "", nil, errors.New("malformed trailing WITH options")
	}

	optionText := strings.TrimSpace(source[marker+len("WITH (") : len(source)-1])
	if optionText == "" {
		return "", nil, errors.New("empty trailing WITH options")
	}
	options := make(map[string]string)
	for _, part := range strings.Split(optionText, ",") {
		assignment := strings.SplitN(part, "=", 2)
		if len(assignment) != 2 {
			return "", nil, fmt.Errorf("option %q is not an assignment", strings.TrimSpace(part))
		}
		key := normalizeSQLSubscriptionOptionKey(assignment[0])
		value := strings.TrimSpace(assignment[1])
		if key == "" || value == "" {
			return "", nil, errors.New("subscription option has an empty key or value")
		}
		if _, exists := options[key]; exists {
			return "", nil, fmt.Errorf("duplicate subscription option %q", key)
		}
		options[key] = value
	}
	return strings.TrimSpace(source[:marker]), options, nil
}

func findSQLSubscriptionOptionsMarker(source string) int {
	const marker = "WITH ("
	for searchEnd := len(source); searchEnd >= len(marker); {
		index := strings.LastIndexByte(source[:searchEnd], 'w')
		if uppercase := strings.LastIndexByte(source[:searchEnd], 'W'); uppercase > index {
			index = uppercase
		}
		if index < 0 {
			return -1
		}
		if index > 0 && source[index-1] > ' ' {
			searchEnd = index
			continue
		}
		if index+len(marker) > len(source) {
			searchEnd = index
			continue
		}
		matched := true
		for offset := 0; offset < len(marker); offset++ {
			if asciiLower(source[index+offset]) != asciiLower(marker[offset]) {
				matched = false
				break
			}
		}
		if matched {
			return index
		}
		searchEnd = index
	}
	return -1
}

func asciiLower(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}

func normalizeSQLSubscriptionOptionKey(source string) string {
	return strings.Join(strings.Fields(strings.ToUpper(source)), "_")
}

func applySQLSubscriptionOptions(definition *QuerySubscriptionDefinition, options map[string]string) error {
	for key, value := range options {
		switch key {
		case "SNAPSHOT":
			snapshot, err := parseSQLSubscriptionBool(value)
			if err != nil {
				return fmt.Errorf("SNAPSHOT: %w", err)
			}
			definition.StartLive = !snapshot
		case "PROGRESS":
			progress, err := parseSQLSubscriptionBool(value)
			if err != nil {
				return fmt.Errorf("PROGRESS: %w", err)
			}
			definition.EmitProgress = progress
		case "AS_OF":
			asOf, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("AS OF: %w", err)
			}
			definition.AsOf = asOf
		case "UP_TO":
			upTo, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("UP TO: %w", err)
			}
			definition.UpTo = upTo
		case "DETERMINISTIC":
			deterministic, err := parseSQLSubscriptionBool(value)
			if err != nil {
				return fmt.Errorf("DETERMINISTIC: %w", err)
			}
			definition.DeterministicOrder = deterministic
		default:
			return fmt.Errorf("unknown subscription option %q", key)
		}
	}
	if definition.AsOf > 0 && definition.UpTo > 0 && definition.AsOf > definition.UpTo {
		return errors.New("AS OF cannot be greater than UP TO")
	}
	return nil
}

func parseSQLSubscriptionBool(source string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("expected true or false, got %q", source)
	}
}

func splitSQLSubscriptionWord(source string) (string, string) {
	source = strings.TrimSpace(source)
	for index := 0; index < len(source); index++ {
		if source[index] <= ' ' {
			return source[:index], strings.TrimSpace(source[index:])
		}
	}
	return source, ""
}
