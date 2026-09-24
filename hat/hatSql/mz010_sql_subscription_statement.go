package hatSql

import (
	"errors"
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

	if strings.TrimSpace(rest) == "" {
		return SQLSubscriptionStatement{}, ErrSQLSubscriptionStatementSyntax
	}
	statement.Definition.Query = strings.TrimSpace(rest)
	return statement, nil
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
