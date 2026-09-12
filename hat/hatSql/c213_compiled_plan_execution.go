package hatSql

// prepareSQLQueryForExecution selects the caller-provided immutable compiled
// template, the optional bounded compiled-plan cache, or the existing prepared
// template cache. Keeping this decision in one helper keeps materialized and
// streamed execution on the same parsing and binding contract.
func prepareSQLQueryForExecution(source string, parameters []interface{}, options *SQLQueryOptions) (*sqlQuery, error) {
	if options == nil {
		return parseSQLQueryWithCache(source, parameters, nil, "")
	}
	if options.compiledTemplate != nil {
		if options.compiledTemplateReadOnly {
			return options.compiledTemplate, nil
		}
		query, err := bindSQLQueryParameters(options.compiledTemplate, parameters)
		if err != nil {
			return nil, err
		}
		rewriteSQLQuery(query)
		return query, nil
	}
	if options.CompiledCache != nil {
		compiled, err := options.CompiledCache.CompileWithSchemaVersion(source, options.PreparedSchemaVersion)
		if err != nil {
			return nil, err
		}
		if compiled.readOnlyTemplateEligible(parameters, *options) {
			options.compiledTemplate = compiled.template
			options.compiledTemplateReadOnly = true
			return compiled.template, nil
		}
		query, err := bindSQLQueryParameters(compiled.template, parameters)
		if err != nil {
			return nil, err
		}
		rewriteSQLQuery(query)
		return query, nil
	}
	return parseSQLQueryWithCache(source, parameters, options.PreparedCache, options.PreparedSchemaVersion)
}
