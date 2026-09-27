package hatSql

import "fmt"

func executeSQLSemiAntiJoin(rows []sqlExecRow, join sqlJoin, leftAliases []string, resolver SQLSourceResolver, ctes map[string][]SQLRow, control *sqlExecutionControl, maxRows int) ([]sqlExecRow, error) {
	leftQualifier, leftField, rightField, ok := sqlHashJoinFields(join.on, leftAliases, join.source.alias)
	if !ok {
		return nil, fmt.Errorf("%s JOIN requires an equality predicate", join.kind)
	}
	right, err := resolveSQLSource(join.source, resolver, ctes, nil, control)
	if err != nil {
		return nil, err
	}
	if len(right) > maxRows {
		return nil, fmt.Errorf("SQL source %q exceeds the %d row limit", join.source.alias, maxRows)
	}
	if control != nil {
		if err := sqlJoinMaterializedInputBudgetError(control.options, right); err != nil {
			return nil, err
		}
	}

	keys := make(map[string]struct{}, len(right))
	for _, candidate := range right {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		key, ok := sqlHashJoinKey(candidate[rightField])
		if ok {
			keys[key] = struct{}{}
		}
	}

	next := make([]sqlExecRow, 0, len(rows))
	for _, left := range rows {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		key, valid := sqlHashJoinKey(sqlField(left, leftQualifier, leftField))
		_, matched := keys[key]
		if !valid {
			matched = false
		}
		keep := matched
		if join.kind == "ANTI" {
			keep = !matched
		}
		if !keep {
			continue
		}
		next = append(next, left)
		if len(next) > maxRows {
			return nil, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
		}
	}
	return next, nil
}
