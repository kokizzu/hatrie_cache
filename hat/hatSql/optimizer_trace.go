package hatSql

const (
	// SQLOptimizerTraceFormat identifies the versioned wire shape of an
	// optimizer trace.
	SQLOptimizerTraceFormat = "hatrie-cache-sql-optimizer-trace/v1"
	// DefaultSQLOptimizerTraceEntries bounds a trace when the caller enables it
	// without selecting a custom limit.
	DefaultSQLOptimizerTraceEntries = 64
	// MaxSQLOptimizerTraceEntries prevents one request from retaining an
	// unbounded optimizer-rule or explain-diagnostic history.
	MaxSQLOptimizerTraceEntries = 1024
)

// SQLOptimizerTraceOptions enables bounded optimizer diagnostics for one
// materialized query result. A nil option keeps the default path unchanged.
// MaxEntries <= 0 uses DefaultSQLOptimizerTraceEntries; larger values are
// capped at MaxSQLOptimizerTraceEntries.
type SQLOptimizerTraceOptions struct {
	MaxEntries int
}

// SQLOptimizerTraceHint is the detached, JSON-stable form of SQLIndexHint
// recorded before and after an optimizer rule.
type SQLOptimizerTraceHint struct {
	Source  string `json:"source,omitempty"`
	Field   string `json:"field,omitempty"`
	Cluster string `json:"cluster,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Mode    string `json:"mode,omitempty"`
}

// SQLOptimizerTraceEntry records one ordered rule application. Status is one
// of applied, no_change, skipped, or error.
type SQLOptimizerTraceEntry struct {
	Rule   int                   `json:"rule"`
	Status string                `json:"status"`
	Before SQLOptimizerTraceHint `json:"before"`
	After  SQLOptimizerTraceHint `json:"after"`
}

// SQLOptimizerTrace contains bounded rule transitions and explain diagnostics
// collected for an opt-in query. It deliberately records hints and planner
// alternatives rather than arbitrary rule-owned state.
type SQLOptimizerTrace struct {
	Format               string                   `json:"format"`
	Entries              []SQLOptimizerTraceEntry `json:"entries"`
	RejectedAlternatives []ExplainAlternative     `json:"rejected_alternatives"`
	Notices              []ExplainNotice          `json:"notices,omitempty"`
	FinalHint            *SQLOptimizerTraceHint   `json:"final_hint,omitempty"`
	Truncated            bool                     `json:"truncated,omitempty"`
	maxEntries           int
}

func newSQLOptimizerTrace(options *SQLOptimizerTraceOptions) *SQLOptimizerTrace {
	if options == nil {
		return nil
	}
	limit := options.MaxEntries
	if limit <= 0 {
		limit = DefaultSQLOptimizerTraceEntries
	}
	if limit > MaxSQLOptimizerTraceEntries {
		limit = MaxSQLOptimizerTraceEntries
	}
	capacity := limit
	if capacity > 4 {
		capacity = 4
	}
	return &SQLOptimizerTrace{
		Format:               SQLOptimizerTraceFormat,
		Entries:              make([]SQLOptimizerTraceEntry, 0, capacity),
		RejectedAlternatives: make([]ExplainAlternative, 0),
		maxEntries:           limit,
	}
}

func (trace *SQLOptimizerTrace) appendEntry(entry SQLOptimizerTraceEntry) {
	if trace == nil {
		return
	}
	if len(trace.Entries) >= trace.maxEntries {
		trace.Truncated = true
		return
	}
	trace.Entries = append(trace.Entries, entry)
}

func (trace *SQLOptimizerTrace) appendRejectedAlternative(alternative ExplainAlternative) {
	if trace == nil {
		return
	}
	if len(trace.RejectedAlternatives) >= trace.maxEntries {
		trace.Truncated = true
		return
	}
	trace.RejectedAlternatives = append(trace.RejectedAlternatives, alternative)
}

func (trace *SQLOptimizerTrace) appendNotice(notice ExplainNotice) {
	if trace == nil {
		return
	}
	if len(trace.Notices) >= trace.maxEntries {
		trace.Truncated = true
		return
	}
	trace.Notices = append(trace.Notices, notice)
}

func (trace *SQLOptimizerTrace) setFinalHint(hint SQLIndexHint) {
	if trace == nil {
		return
	}
	if hint.Source == "" && hint.Field == "" && hint.Cluster == "" && hint.Kind == "" && hint.Mode == "" {
		trace.FinalHint = nil
		return
	}
	detached := sqlOptimizerTraceHint(hint)
	trace.FinalHint = &detached
}

func sqlOptimizerTraceHint(hint SQLIndexHint) SQLOptimizerTraceHint {
	return SQLOptimizerTraceHint{
		Source:  hint.Source,
		Field:   hint.Field,
		Cluster: hint.Cluster,
		Kind:    hint.Kind,
		Mode:    string(hint.Mode),
	}
}

func appendSQLOptimizerTraceRule(trace *SQLOptimizerTrace, rule int, status string, before, after SQLIndexHint) {
	if trace == nil {
		return
	}
	trace.appendEntry(SQLOptimizerTraceEntry{
		Rule:   rule,
		Status: status,
		Before: sqlOptimizerTraceHint(before),
		After:  sqlOptimizerTraceHint(after),
	})
}

func attachSQLOptimizerTrace(result *SQLQueryResult, trace *SQLOptimizerTrace, steps []SQLExplainStep) {
	if result == nil || trace == nil {
		return
	}
	if len(steps) == 0 {
		steps = result.Plan
	}
	attached := trace
	for _, step := range steps {
		for _, alternative := range step.Alternatives {
			if !alternative.Selected {
				attached.appendRejectedAlternative(alternative)
			}
		}
		for _, notice := range step.Notices {
			attached.appendNotice(notice)
		}
	}
	result.OptimizerTrace = attached
}

func cloneSQLOptimizerTrace(trace *SQLOptimizerTrace) *SQLOptimizerTrace {
	if trace == nil {
		return nil
	}
	clone := *trace
	clone.Entries = append([]SQLOptimizerTraceEntry(nil), trace.Entries...)
	clone.RejectedAlternatives = append([]ExplainAlternative(nil), trace.RejectedAlternatives...)
	clone.Notices = append([]ExplainNotice(nil), trace.Notices...)
	if trace.FinalHint != nil {
		finalHint := *trace.FinalHint
		clone.FinalHint = &finalHint
	}
	return &clone
}
