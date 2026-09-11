package hatSql

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
)

type sqlResultCacheSource struct {
	kind string
	key  string
}

func sqlResultCacheLookup(query *sqlQuery, source string, parameters []interface{}, resolver SQLSourceResolver, options SQLQueryOptions) (string, func() (string, bool), bool) {
	if options.ResultCache == nil || !sqlResultCacheQueryEligible(query) {
		return "", nil, false
	}
	key, ok := sqlResultCacheKey(source, parameters, options)
	if !ok {
		return "", nil, false
	}
	sources := sqlResultCacheSources(query)
	if len(sources) == 0 {
		return "", nil, false
	}
	if _, ok := resolver.(SourceVersionResolver); !ok {
		return "", nil, false
	}
	return key, func() (string, bool) {
		return sqlResultCacheVersion(sources, resolver)
	}, true
}

func sqlResultCacheKey(source string, parameters []interface{}, options SQLQueryOptions) (string, bool) {
	if !sqlResultCacheOptionsEligible(options) || sqlResultCacheSourceIsVolatile(source) {
		return "", false
	}
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(parameters); err != nil {
		return "", false
	}
	var key strings.Builder
	key.WriteString("hatsql-result-cache-v1")
	appendSQLResultCachePart(&key, source)
	appendSQLResultCachePart(&key, encoded.String())
	appendSQLResultCachePart(&key, string(options.Collation))
	appendSQLResultCachePart(&key, options.PreparedSchemaVersion)
	return key.String(), true
}

func appendSQLResultCachePart(key *strings.Builder, value string) {
	key.WriteString(strconv.Itoa(len(value)))
	key.WriteByte(':')
	key.WriteString(value)
}

func sqlResultCacheOptionsEligible(options SQLQueryOptions) bool {
	return options.compiledTemplate == nil &&
		options.MaxRows == 0 &&
		options.MaxJoinWork == 0 &&
		options.MaxJoinBytes == 0 &&
		!options.SpillBloom &&
		!options.RuntimeJoinBloomFilter &&
		options.MaxResultBytes == 0 &&
		options.Workers == 0 &&
		options.MaxSortBytes == 0 &&
		options.MaxGroupBytes == 0 &&
		options.MaxGroupRowsPerKey == 0 &&
		options.MaxGroupKeys == 0 &&
		options.MaxSetBytes == 0 &&
		options.SpillDirectory == "" &&
		options.MaxSpillBytes == 0 &&
		options.SpillFaults == nil &&
		options.SpillCipher == nil &&
		!options.CompressSpill &&
		options.TriggerRegistry == nil &&
		options.MaxRecursionDepth == 0 &&
		!options.DetectRecursiveCycles &&
		options.Timeout == 0 &&
		options.ConditionCache == nil &&
		options.AdaptivePlanner == nil &&
		options.Observer == nil &&
		options.IndexAdvisor == nil &&
		options.ProjectionAdvisor == nil &&
		options.ProjectionCatalog == nil &&
		options.IndexUseRecorder == nil &&
		options.IndexHint.Mode == "" &&
		options.Optimizer == nil &&
		options.SlowQueryRecorder == nil
}

func sqlResultCacheQueryEligible(query *sqlQuery) bool {
	if query == nil || query.explain || query.pipeline || query.analyze || query.sample != nil {
		return false
	}
	eligible := true
	var visitExpr func(sqlExpr)
	var visitQuery func(*sqlQuery)
	visitExpr = func(expr sqlExpr) {
		if !eligible {
			return
		}
		if expr.kind == "func" && !sqlBuiltinFunction(expr.name) {
			eligible = false
			return
		}
		if expr.query != nil {
			visitQuery(expr.query)
		}
		if expr.left != nil {
			visitExpr(*expr.left)
		}
		if expr.right != nil {
			visitExpr(*expr.right)
		}
		if expr.filter != nil {
			visitExpr(*expr.filter)
		}
		for _, argument := range expr.args {
			visitExpr(argument)
		}
		for _, branch := range expr.cases {
			visitExpr(branch.when)
			visitExpr(branch.then)
		}
		if expr.window != nil {
			for _, partition := range expr.window.partition {
				visitExpr(partition)
			}
			for _, order := range expr.window.order {
				visitExpr(order.expr)
			}
		}
	}
	visitQuery = func(candidate *sqlQuery) {
		if candidate == nil || !eligible {
			return
		}
		for _, cte := range candidate.ctes {
			visitQuery(cte.query)
		}
		if candidate.from != nil {
			visitQuery(candidate.from.query)
		}
		for _, join := range candidate.joins {
			visitQuery(join.source.query)
			visitExpr(join.on)
		}
		for _, item := range candidate.selects {
			visitExpr(item.expr)
		}
		visitExpr(candidate.where)
		for _, expression := range candidate.groupBy {
			visitExpr(expression)
		}
		for _, groupingSet := range candidate.groupingSets {
			for _, expression := range groupingSet {
				visitExpr(expression)
			}
		}
		for _, expression := range candidate.groupingDimensions {
			visitExpr(expression)
		}
		visitExpr(candidate.having)
		for _, order := range candidate.orderBy {
			visitExpr(order.expr)
		}
		if candidate.limitBy != nil {
			for _, expression := range candidate.limitBy.expressions {
				visitExpr(expression)
			}
		}
		for _, window := range candidate.windows {
			for _, partition := range window.partition {
				visitExpr(partition)
			}
			for _, order := range window.order {
				visitExpr(order.expr)
			}
		}
		for _, union := range candidate.unions {
			visitQuery(union.query)
		}
	}
	visitQuery(query)
	return eligible
}

func sqlResultCacheSources(query *sqlQuery) []sqlResultCacheSource {
	seen := make(map[string]struct{})
	sources := make([]sqlResultCacheSource, 0)
	appendSource := func(source sqlSource) {
		if source.kind != "CACHE" && source.kind != "EXTERNAL" || source.key == "" {
			return
		}
		identity := source.kind + "\x00" + source.key
		if _, ok := seen[identity]; ok {
			return
		}
		seen[identity] = struct{}{}
		sources = append(sources, sqlResultCacheSource{kind: source.kind, key: source.key})
	}
	var visitSource func(sqlSource)
	var visitQuery func(*sqlQuery)
	visitSource = func(source sqlSource) {
		appendSource(source)
		if source.kind == "SUBQUERY" {
			visitQuery(source.query)
		}
	}
	visitQuery = func(candidate *sqlQuery) {
		if candidate == nil {
			return
		}
		for _, cte := range candidate.ctes {
			visitQuery(cte.query)
		}
		if candidate.from != nil {
			visitSource(*candidate.from)
		}
		for _, join := range candidate.joins {
			visitSource(join.source)
		}
		for _, union := range candidate.unions {
			visitQuery(union.query)
		}
		var visitExpr func(sqlExpr)
		visitExpr = func(expr sqlExpr) {
			if expr.query != nil {
				visitQuery(expr.query)
			}
			if expr.left != nil {
				visitExpr(*expr.left)
			}
			if expr.right != nil {
				visitExpr(*expr.right)
			}
			if expr.filter != nil {
				visitExpr(*expr.filter)
			}
			for _, argument := range expr.args {
				visitExpr(argument)
			}
			for _, branch := range expr.cases {
				visitExpr(branch.when)
				visitExpr(branch.then)
			}
			if expr.window != nil {
				for _, partition := range expr.window.partition {
					visitExpr(partition)
				}
				for _, order := range expr.window.order {
					visitExpr(order.expr)
				}
			}
		}
		for _, item := range candidate.selects {
			visitExpr(item.expr)
		}
		visitExpr(candidate.where)
		for _, expression := range candidate.groupBy {
			visitExpr(expression)
		}
		for _, groupingSet := range candidate.groupingSets {
			for _, expression := range groupingSet {
				visitExpr(expression)
			}
		}
		for _, expression := range candidate.groupingDimensions {
			visitExpr(expression)
		}
		visitExpr(candidate.having)
		for _, order := range candidate.orderBy {
			visitExpr(order.expr)
		}
		for _, join := range candidate.joins {
			visitExpr(join.on)
		}
		if candidate.limitBy != nil {
			for _, expression := range candidate.limitBy.expressions {
				visitExpr(expression)
			}
		}
		for _, window := range candidate.windows {
			for _, partition := range window.partition {
				visitExpr(partition)
			}
			for _, order := range window.order {
				visitExpr(order.expr)
			}
		}
	}
	visitQuery(query)
	return sources
}

func sqlResultCacheVersion(sources []sqlResultCacheSource, resolver SQLSourceResolver) (string, bool) {
	versions, ok := resolver.(SourceVersionResolver)
	if !ok || len(sources) == 0 {
		return "", false
	}
	var result strings.Builder
	for _, source := range sources {
		version, available, err := versions.SQLSourceVersion(source.kind, source.key)
		if err != nil || !available || version == "" {
			return "", false
		}
		appendSQLResultCachePart(&result, source.kind)
		appendSQLResultCachePart(&result, source.key)
		appendSQLResultCachePart(&result, version)
	}
	return result.String(), true
}

func sqlResultCacheSourceIsVolatile(source string) bool {
	tokens, err := lexSQL(source)
	if err != nil {
		return true
	}
	for _, token := range tokens {
		if token.kind != sqlTokenIdentifier {
			continue
		}
		switch strings.ToUpper(token.text) {
		case "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "NOW", "RAND", "RANDOM", "UUID", "GENERATE_UUID":
			return true
		}
	}
	return false
}
