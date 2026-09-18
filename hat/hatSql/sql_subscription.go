package hatSql

import (
	"context"
	"fmt"
	"sort"
)

// SQLQueryDependencies parses source and returns its static CACHE dependencies
// in deterministic order. SQL subscriptions use these names to decide which
// source changes require reevaluation.
func SQLQueryDependencies(source string, parameters []interface{}) ([]string, error) {
	query, err := parseSQLQueryParameters(source, parameters)
	if err != nil {
		return nil, err
	}
	dependencies := sqlQueryCacheDependencies(query)
	if len(dependencies) == 0 {
		return nil, fmt.Errorf("SQL subscription requires at least one CACHE source")
	}
	sort.Strings(dependencies)
	return dependencies, nil
}

// SubscribeSQL derives static CACHE dependencies when definition omits them
// and registers a normal bounded query-result subscription. Existing
// Subscribe callers can keep supplying explicit dependencies when their
// source graph is dynamic.
func (registry *QuerySubscriptions) SubscribeSQL(ctx context.Context, definition QuerySubscriptionDefinition, resolver SourceResolver, options QueryOptions) (*QuerySubscription, error) {
	definition, err := withSQLSubscriptionDependencies(definition)
	if err != nil {
		return nil, err
	}
	return registry.Subscribe(ctx, definition, resolver, options)
}

// SubscribeDifferentialSQL derives static CACHE dependencies when definition
// omits them and registers a differential query-result subscription.
func (registry *QuerySubscriptions) SubscribeDifferentialSQL(ctx context.Context, definition QuerySubscriptionDefinition, resolver SourceResolver, options QueryOptions) (*QueryDifferentialSubscription, error) {
	definition, err := withSQLSubscriptionDependencies(definition)
	if err != nil {
		return nil, err
	}
	return registry.SubscribeDifferential(ctx, definition, resolver, options)
}

func withSQLSubscriptionDependencies(definition QuerySubscriptionDefinition) (QuerySubscriptionDefinition, error) {
	if len(definition.Dependencies) != 0 {
		return definition, nil
	}
	dependencies, err := SQLQueryDependencies(definition.Query, definition.Parameters)
	if err != nil {
		return QuerySubscriptionDefinition{}, err
	}
	definition.Dependencies = dependencies
	return definition, nil
}
