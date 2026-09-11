package hatSql

import (
	"fmt"
	"sort"
)

type sqlAsofJoinPlan struct {
	leftQualifier  string
	leftField      string
	rightField     string
	timeQualifier  string
	leftTimeField  string
	rightTimeField string
	operator       string
}

func (p *sqlQueryParser) parseAsofJoin() (sqlJoin, error) {
	p.next()
	left := false
	if p.keyword("LEFT") {
		left = true
		p.next()
	}
	if err := p.expectKeyword("JOIN"); err != nil {
		return sqlJoin{}, err
	}
	source, err := p.parseSource()
	if err != nil {
		return sqlJoin{}, err
	}
	if err := p.expectKeyword("ON"); err != nil {
		return sqlJoin{}, err
	}
	on, err := p.parseCondition()
	if err != nil {
		return sqlJoin{}, err
	}
	return sqlJoin{kind: "ASOF", source: source, on: on, asofLeft: left}, nil
}

func sqlAsofJoinFields(expression sqlExpr, leftAliases []string, rightAlias string) (sqlAsofJoinPlan, bool) {
	plan := sqlAsofJoinPlan{}
	equalitySeen := false
	timeSeen := false
	valid := true
	var collect func(sqlExpr)
	collect = func(current sqlExpr) {
		if !valid {
			return
		}
		if current.kind == "binary" && current.op == "AND" && current.left != nil && current.right != nil {
			collect(*current.left)
			collect(*current.right)
			return
		}
		if qualifier, leftField, rightField, ok := sqlHashJoinFields(current, leftAliases, rightAlias); ok {
			if equalitySeen {
				valid = false
				return
			}
			equalitySeen = true
			plan.leftQualifier = qualifier
			plan.leftField = leftField
			plan.rightField = rightField
			return
		}
		qualifier, leftField, rightField, operator, ok := sqlRangeJoinFields(current, leftAliases, rightAlias)
		if !ok || timeSeen {
			valid = false
			return
		}
		timeSeen = true
		plan.timeQualifier = qualifier
		plan.leftTimeField = leftField
		plan.rightTimeField = rightField
		plan.operator = operator
	}
	collect(expression)
	return plan, valid && equalitySeen && timeSeen
}

type sqlAsofJoinCandidate struct {
	row  sqlExecRow
	time interface{}
}

func executeSQLAsofJoin(rows []sqlExecRow, join sqlJoin, leftAliases []string, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, maxRows int) ([]sqlExecRow, error) {
	plan, ok := sqlAsofJoinFields(join.on, leftAliases, join.source.alias)
	if !ok {
		return nil, fmt.Errorf("ASOF JOIN requires one equality and one temporal inequality between the left and right sources")
	}
	right, err := resolveSQLSource(join.source, resolver, ctes, metrics, control)
	if err != nil {
		return nil, err
	}
	if len(right) > maxRows {
		return nil, fmt.Errorf("SQL source %q exceeds the %d row limit", join.source.alias, maxRows)
	}
	wrapped := wrapSQLSource(join.source, right)
	buckets := make(map[string][]sqlAsofJoinCandidate, len(wrapped))
	for _, candidate := range wrapped {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		key, ok := sqlHashJoinKey(sqlField(candidate, join.source.alias, plan.rightField))
		if !ok {
			continue
		}
		timeValue := sqlField(candidate, join.source.alias, plan.rightTimeField)
		if timeValue == nil {
			continue
		}
		buckets[key] = append(buckets[key], sqlAsofJoinCandidate{row: candidate, time: timeValue})
	}
	for key := range buckets {
		bucket := buckets[key]
		sort.SliceStable(bucket, func(left, right int) bool {
			return sqlCompare(bucket[left].time, bucket[right].time) < 0
		})
		buckets[key] = bucket
	}

	next := make([]sqlExecRow, 0, len(rows))
	for _, left := range rows {
		if err := control.addJoinWork(1); err != nil {
			return nil, err
		}
		key, keyOK := sqlHashJoinKey(sqlField(left, plan.leftQualifier, plan.leftField))
		leftTime := sqlField(left, plan.timeQualifier, plan.leftTimeField)
		var candidate sqlAsofJoinCandidate
		found := false
		if keyOK && leftTime != nil {
			bucket := buckets[key]
			index, match := sqlAsofJoinCandidateIndex(bucket, leftTime, plan.operator)
			if match {
				candidate = bucket[index]
				found = true
			}
		}
		if found {
			next = append(next, mergeSQLRows(left, candidate.row))
		} else if join.asofLeft {
			empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
			next = append(next, mergeSQLRows(left, empty))
		}
		if len(next) > maxRows {
			return nil, fmt.Errorf("SQL ASOF JOIN exceeds the %d row limit", maxRows)
		}
	}
	return next, nil
}

func sqlAsofJoinCandidateIndex(candidates []sqlAsofJoinCandidate, leftTime interface{}, operator string) (int, bool) {
	if len(candidates) == 0 {
		return 0, false
	}
	switch operator {
	case "<=":
		index := sort.Search(len(candidates), func(index int) bool {
			return sqlCompare(candidates[index].time, leftTime) > 0
		})
		if index == 0 {
			return 0, false
		}
		return index - 1, true
	case "<":
		index := sort.Search(len(candidates), func(index int) bool {
			return sqlCompare(candidates[index].time, leftTime) >= 0
		})
		if index == 0 {
			return 0, false
		}
		return index - 1, true
	case ">=":
		index := sort.Search(len(candidates), func(index int) bool {
			return sqlCompare(candidates[index].time, leftTime) >= 0
		})
		if index == len(candidates) {
			return 0, false
		}
		return index, true
	case ">":
		index := sort.Search(len(candidates), func(index int) bool {
			return sqlCompare(candidates[index].time, leftTime) > 0
		})
		if index == len(candidates) {
			return 0, false
		}
		return index, true
	default:
		return 0, false
	}
}

func joinDescription(join sqlJoin) string {
	detail := join.kind + " JOIN " + sqlExplainSource(join.source)
	if join.on.kind != "" {
		detail += " ON " + sqlExplainExpression(join.on)
	}
	return detail
}
