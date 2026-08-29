package hatSql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// QueryAlertThreshold triggers when any result row's numeric column satisfies
// the configured comparison.
type QueryAlertThreshold struct {
	Column   string
	Operator string
	Value    float64
}

// QueryAlertRule evaluates either a missing-row expectation or a numeric
// threshold. Exactly one trigger form must be configured.
type QueryAlertRule struct {
	Name            string
	Query           string
	Parameters      []interface{}
	ExpectedMinRows int
	Threshold       *QueryAlertThreshold
}

// QueryAlertEvent is one privacy-safe alert evaluation diagnostic.
type QueryAlertEvent struct {
	RuleName        string
	At              time.Time
	Triggered       bool
	Reason          string
	OutputRows      int
	Plan            []ExplainStep
	ErrorDiagnostic string
}

// QueryAlertRules stores query-backed alert rules and bounded evaluation
// history. It is safe for concurrent evaluation.
type QueryAlertRules struct {
	mu       sync.RWMutex
	resolver SourceResolver
	options  QueryOptions
	capacity int
	rules    map[string]QueryAlertRule
	history  []QueryAlertEvent
}

func NewQueryAlertRules(resolver SourceResolver, options QueryOptions, historyCapacity int) (*QueryAlertRules, error) {
	if historyCapacity < 0 {
		return nil, fmt.Errorf("alert history capacity cannot be negative")
	}
	return &QueryAlertRules{resolver: resolver, options: options, capacity: historyCapacity, rules: make(map[string]QueryAlertRule)}, nil
}

func (rules *QueryAlertRules) Create(rule QueryAlertRule) error {
	if rules == nil {
		return fmt.Errorf("query alert rules are nil")
	}
	rule, err := normalizeQueryAlertRule(rule)
	if err != nil {
		return err
	}
	rules.mu.Lock()
	defer rules.mu.Unlock()
	if _, exists := rules.rules[rule.Name]; exists {
		return fmt.Errorf("alert rule %q already exists", rule.Name)
	}
	rules.rules[rule.Name] = rule
	return nil
}

func (rules *QueryAlertRules) Evaluate(ctx context.Context, name string) (QueryAlertEvent, error) {
	if rules == nil {
		return QueryAlertEvent{}, fmt.Errorf("query alert rules are nil")
	}
	rules.mu.RLock()
	rule, exists := rules.rules[strings.TrimSpace(name)]
	rules.mu.RUnlock()
	if !exists {
		return QueryAlertEvent{}, fmt.Errorf("alert rule %q does not exist", name)
	}
	options := rules.options
	capture := &jobPlanCapture{next: options.Observer}
	options.Observer = capture
	result, err := ExecuteQueryParameters(ctx, rule.Query, rules.resolver, cloneJobParameters(rule.Parameters), options)
	event := QueryAlertEvent{RuleName: rule.Name, At: time.Now().UTC(), OutputRows: len(result.Rows), Plan: capture.plan()}
	if len(event.Plan) == 0 {
		event.Plan = cloneMaterializedExplainSteps(result.Plan)
	}
	if err != nil {
		event.ErrorDiagnostic = err.Error()
		rules.record(event)
		return event, err
	}
	if rule.ExpectedMinRows > 0 {
		event.Triggered = len(result.Rows) < rule.ExpectedMinRows
		if event.Triggered {
			event.Reason = fmt.Sprintf("expected at least %d rows, got %d", rule.ExpectedMinRows, len(result.Rows))
		} else {
			event.Reason = fmt.Sprintf("expected at least %d rows", rule.ExpectedMinRows)
		}
	} else {
		threshold := rule.Threshold
		for _, row := range result.Rows {
			value, ok := sqlNumber(row[threshold.Column])
			if ok && sqlColumnarNumericMatches(value, threshold.Operator, threshold.Value) {
				event.Triggered = true
				break
			}
		}
		if event.Triggered {
			event.Reason = fmt.Sprintf("row matched %s %s %g", threshold.Column, threshold.Operator, threshold.Value)
		} else {
			event.Reason = fmt.Sprintf("no row matched %s %s %g", threshold.Column, threshold.Operator, threshold.Value)
		}
	}
	rules.record(event)
	return cloneQueryAlertEvent(event), nil
}

func (rules *QueryAlertRules) History() []QueryAlertEvent {
	if rules == nil {
		return nil
	}
	rules.mu.RLock()
	history := make([]QueryAlertEvent, len(rules.history))
	for index := range rules.history {
		history[index] = cloneQueryAlertEvent(rules.history[index])
	}
	rules.mu.RUnlock()
	return history
}

func (rules *QueryAlertRules) record(event QueryAlertEvent) {
	rules.mu.Lock()
	defer rules.mu.Unlock()
	if rules.capacity == 0 {
		return
	}
	if len(rules.history) == rules.capacity {
		copy(rules.history, rules.history[1:])
		rules.history[len(rules.history)-1] = cloneQueryAlertEvent(event)
		return
	}
	rules.history = append(rules.history, cloneQueryAlertEvent(event))
}

func normalizeQueryAlertRule(rule QueryAlertRule) (QueryAlertRule, error) {
	rule.Name = strings.TrimSpace(rule.Name)
	rule.Query = strings.TrimSpace(rule.Query)
	if rule.Name == "" || rule.Query == "" {
		return QueryAlertRule{}, fmt.Errorf("alert rule name and query are required")
	}
	if (rule.ExpectedMinRows > 0) == (rule.Threshold != nil) {
		return QueryAlertRule{}, fmt.Errorf("alert rule %q requires exactly one trigger", rule.Name)
	}
	if rule.Threshold != nil {
		threshold := *rule.Threshold
		threshold.Column = strings.TrimSpace(threshold.Column)
		if threshold.Column == "" || !sqlColumnarNumericOperator(threshold.Operator) {
			return QueryAlertRule{}, fmt.Errorf("alert rule %q has an invalid numeric threshold", rule.Name)
		}
		rule.Threshold = &threshold
	}
	rule.Parameters = cloneJobParameters(rule.Parameters)
	return rule, nil
}

func cloneQueryAlertEvent(event QueryAlertEvent) QueryAlertEvent {
	event.Plan = cloneMaterializedExplainSteps(event.Plan)
	return event
}
