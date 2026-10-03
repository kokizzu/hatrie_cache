package hatSql

import (
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLIncrementalJoinSelectorCapacity bounds retained join metadata
	// when the selector is constructed with zero capacity.
	DefaultSQLIncrementalJoinSelectorCapacity = 128
	// DefaultSQLIncrementalJoinSelectorMinSamples avoids selecting a candidate
	// from one potentially noisy observation.
	DefaultSQLIncrementalJoinSelectorMinSamples uint64 = 2
	maxSQLIncrementalJoinSelectorCapacity              = 1024
	maxSQLIncrementalJoinSelectorTextBytes             = 128
)

// SQLIncrementalJoinSelectorOptions configures bounded, opt-in join
// arrangement selection. The selector only returns a decision; it never
// creates, drops, or mutates a join arrangement.
type SQLIncrementalJoinSelectorOptions struct {
	Capacity   int
	MinSamples uint64
	CostModel  SQLArrangementCostModelOptions
}

// SQLIncrementalJoinCandidate describes one caller-maintained incremental
// join arrangement. SourceGeneration is the newest source change included in
// the arrangement's state.
type SQLIncrementalJoinCandidate struct {
	Key              string
	LeftSource       string
	RightSource      string
	LeftField        string
	RightField       string
	SourceGeneration uint64
	BuildCostNanos   uint64
	ProbeCostNanos   uint64
	ScanCostNanos    uint64
	MaintenanceNanos uint64
	ExpectedReads    uint64
	ExpectedWrites   uint64
	MemoryBytes      uint64
}

// SQLIncrementalJoinRequest identifies the join arrangement required by one
// query and the source generation it must cover.
type SQLIncrementalJoinRequest struct {
	LeftSource       string
	RightSource      string
	LeftField        string
	RightField       string
	SourceGeneration uint64
}

// SQLIncrementalJoinAction is the safe next action for a selected candidate.
type SQLIncrementalJoinAction string

const (
	SQLIncrementalJoinCreate  SQLIncrementalJoinAction = "create"
	SQLIncrementalJoinReuse   SQLIncrementalJoinAction = "reuse"
	SQLIncrementalJoinRefresh SQLIncrementalJoinAction = "refresh"
)

// SQLIncrementalJoinDecision is a deterministic selection result. Create is
// returned when no candidate has enough observations and a reusable cost
// score. Refresh is returned for reusable state behind the requested source
// generation.
type SQLIncrementalJoinDecision struct {
	Action    SQLIncrementalJoinAction
	Candidate SQLIncrementalJoinCandidate
	Score     SQLArrangementCostScore
	Samples   uint64
	Stale     bool
	Reason    string
}

// SQLIncrementalJoinSnapshot is an independent view of one retained
// candidate and its observation count.
type SQLIncrementalJoinSnapshot struct {
	Candidate SQLIncrementalJoinCandidate
	Samples   uint64
}

// SQLIncrementalJoinSelector keeps a bounded, indexed catalog of join
// arrangements. The index avoids scanning unrelated join definitions when a
// query changes its join predicate, while the cost model prevents selecting a
// candidate that cannot pay back its maintenance cost.
type SQLIncrementalJoinSelector struct {
	capacity   int
	minSamples uint64
	model      *SQLArrangementCostModel
	mu         sync.RWMutex
	candidates map[sqlIncrementalJoinCandidateKey]sqlIncrementalJoinStats
	byJoin     map[sqlIncrementalJoinSignature][]sqlIncrementalJoinCandidateKey
}

type sqlIncrementalJoinSignature struct {
	leftSource  string
	rightSource string
	leftField   string
	rightField  string
}

type sqlIncrementalJoinCandidateKey struct {
	signature sqlIncrementalJoinSignature
	key       string
}

type sqlIncrementalJoinStats struct {
	candidate SQLIncrementalJoinCandidate
	samples   uint64
}

// NewSQLIncrementalJoinSelector creates an opt-in, bounded join selector.
// Zero Capacity and MinSamples use conservative defaults. Capacity is capped
// so untrusted workload identities cannot grow the selector without bound.
func NewSQLIncrementalJoinSelector(options SQLIncrementalJoinSelectorOptions) *SQLIncrementalJoinSelector {
	if options.Capacity <= 0 {
		options.Capacity = DefaultSQLIncrementalJoinSelectorCapacity
	}
	if options.Capacity > maxSQLIncrementalJoinSelectorCapacity {
		options.Capacity = maxSQLIncrementalJoinSelectorCapacity
	}
	if options.MinSamples == 0 {
		options.MinSamples = DefaultSQLIncrementalJoinSelectorMinSamples
	}
	return &SQLIncrementalJoinSelector{
		capacity:   options.Capacity,
		minSamples: options.MinSamples,
		model:      NewSQLArrangementCostModel(options.CostModel),
		candidates: make(map[sqlIncrementalJoinCandidateKey]sqlIncrementalJoinStats, options.Capacity),
		byJoin:     make(map[sqlIncrementalJoinSignature][]sqlIncrementalJoinCandidateKey),
	}
}

// ObserveJoin records one cost observation for a candidate. Cost and memory
// values are retained conservatively at their largest observed values, while
// expected reads and writes are accumulated for the payback model. Older
// out-of-order observations cannot move SourceGeneration backwards.
func (selector *SQLIncrementalJoinSelector) ObserveJoin(candidate SQLIncrementalJoinCandidate) bool {
	if selector == nil {
		return false
	}
	normalized, signature, ok := normalizeSQLIncrementalJoinCandidate(candidate)
	if !ok {
		return false
	}
	key := sqlIncrementalJoinCandidateKey{signature: signature, key: normalized.Key}
	selector.mu.Lock()
	defer selector.mu.Unlock()
	stats, exists := selector.candidates[key]
	if !exists {
		if len(selector.candidates) >= selector.capacity {
			return false
		}
		stats.candidate = normalized
		selector.byJoin[signature] = append(selector.byJoin[signature], key)
	}
	if normalized.SourceGeneration > stats.candidate.SourceGeneration {
		stats.candidate.SourceGeneration = normalized.SourceGeneration
	}
	if normalized.BuildCostNanos > stats.candidate.BuildCostNanos {
		stats.candidate.BuildCostNanos = normalized.BuildCostNanos
	}
	if normalized.ProbeCostNanos > stats.candidate.ProbeCostNanos {
		stats.candidate.ProbeCostNanos = normalized.ProbeCostNanos
	}
	if normalized.ScanCostNanos > stats.candidate.ScanCostNanos {
		stats.candidate.ScanCostNanos = normalized.ScanCostNanos
	}
	if normalized.MaintenanceNanos > stats.candidate.MaintenanceNanos {
		stats.candidate.MaintenanceNanos = normalized.MaintenanceNanos
	}
	if normalized.MemoryBytes > stats.candidate.MemoryBytes {
		stats.candidate.MemoryBytes = normalized.MemoryBytes
	}
	stats.candidate.ExpectedReads = sqlIncrementalJoinSaturatingAdd(stats.candidate.ExpectedReads, normalized.ExpectedReads)
	stats.candidate.ExpectedWrites = sqlIncrementalJoinSaturatingAdd(stats.candidate.ExpectedWrites, normalized.ExpectedWrites)
	stats.samples = sqlIncrementalJoinSaturatingAdd(stats.samples, 1)
	selector.candidates[key] = stats
	return true
}

// SelectJoin returns the best reusable candidate for a join request. Future
// generations are ignored; stale candidates are explicitly marked refresh so
// callers can hydrate them before use.
func (selector *SQLIncrementalJoinSelector) SelectJoin(request SQLIncrementalJoinRequest) SQLIncrementalJoinDecision {
	if selector == nil {
		return SQLIncrementalJoinDecision{Action: SQLIncrementalJoinCreate, Reason: "selector is nil"}
	}
	_, signature, ok := normalizeSQLIncrementalJoinRequest(request)
	if !ok {
		return SQLIncrementalJoinDecision{Action: SQLIncrementalJoinCreate, Reason: "invalid join request"}
	}
	selector.mu.RLock()
	keys := selector.byJoin[signature]
	var best SQLIncrementalJoinDecision
	found := false
	for _, key := range keys {
		stats, exists := selector.candidates[key]
		if !exists || stats.samples < selector.minSamples || stats.candidate.SourceGeneration > request.SourceGeneration {
			continue
		}
		score := selector.model.Evaluate(sqlIncrementalJoinCostCandidate(stats.candidate))
		if !score.Reusable {
			continue
		}
		stale := stats.candidate.SourceGeneration < request.SourceGeneration
		current := SQLIncrementalJoinDecision{
			Action:    SQLIncrementalJoinReuse,
			Candidate: stats.candidate,
			Score:     score,
			Samples:   stats.samples,
			Stale:     stale,
		}
		if stale {
			current.Action = SQLIncrementalJoinRefresh
		}
		if !found || sqlIncrementalJoinDecisionBetter(current, best) {
			best = current
			found = true
		}
	}
	selector.mu.RUnlock()
	if !found {
		return SQLIncrementalJoinDecision{
			Action: SQLIncrementalJoinCreate,
			Reason: "no compatible reusable join arrangement",
		}
	}
	if best.Stale {
		best.Reason = "reusable join arrangement requires incremental refresh"
	} else {
		best.Reason = "reused fresh join arrangement"
	}
	return best
}

// Snapshot returns a deterministic, independent view of the retained
// candidates. It is intended for EXPLAIN and operational diagnostics.
func (selector *SQLIncrementalJoinSelector) Snapshot() []SQLIncrementalJoinSnapshot {
	if selector == nil {
		return nil
	}
	selector.mu.RLock()
	snapshot := make([]SQLIncrementalJoinSnapshot, 0, len(selector.candidates))
	for _, stats := range selector.candidates {
		snapshot = append(snapshot, SQLIncrementalJoinSnapshot{Candidate: stats.candidate, Samples: stats.samples})
	}
	selector.mu.RUnlock()
	sort.Slice(snapshot, func(left, right int) bool {
		return sqlIncrementalJoinCandidateLess(snapshot[left].Candidate, snapshot[right].Candidate)
	})
	return snapshot
}

// Reset clears observations while retaining the selector configuration.
func (selector *SQLIncrementalJoinSelector) Reset() {
	if selector == nil {
		return
	}
	selector.mu.Lock()
	clear(selector.candidates)
	clear(selector.byJoin)
	selector.mu.Unlock()
}

// Len returns the number of retained candidates.
func (selector *SQLIncrementalJoinSelector) Len() int {
	if selector == nil {
		return 0
	}
	selector.mu.RLock()
	length := len(selector.candidates)
	selector.mu.RUnlock()
	return length
}

func normalizeSQLIncrementalJoinCandidate(candidate SQLIncrementalJoinCandidate) (SQLIncrementalJoinCandidate, sqlIncrementalJoinSignature, bool) {
	key := strings.TrimSpace(candidate.Key)
	if !validSQLIncrementalJoinText(key) {
		return SQLIncrementalJoinCandidate{}, sqlIncrementalJoinSignature{}, false
	}
	leftSource, ok := normalizeSQLIncrementalJoinSource(candidate.LeftSource)
	if !ok {
		return SQLIncrementalJoinCandidate{}, sqlIncrementalJoinSignature{}, false
	}
	rightSource, ok := normalizeSQLIncrementalJoinSource(candidate.RightSource)
	if !ok {
		return SQLIncrementalJoinCandidate{}, sqlIncrementalJoinSignature{}, false
	}
	leftField, ok := normalizeSQLIncrementalJoinField(candidate.LeftField)
	if !ok {
		return SQLIncrementalJoinCandidate{}, sqlIncrementalJoinSignature{}, false
	}
	rightField, ok := normalizeSQLIncrementalJoinField(candidate.RightField)
	if !ok {
		return SQLIncrementalJoinCandidate{}, sqlIncrementalJoinSignature{}, false
	}
	candidate.Key = key
	candidate.LeftSource = leftSource
	candidate.RightSource = rightSource
	candidate.LeftField = leftField
	candidate.RightField = rightField
	signature := sqlIncrementalJoinSignature{
		leftSource: leftSource, rightSource: rightSource,
		leftField: leftField, rightField: rightField,
	}
	return candidate, signature, true
}

func normalizeSQLIncrementalJoinRequest(request SQLIncrementalJoinRequest) (SQLIncrementalJoinRequest, sqlIncrementalJoinSignature, bool) {
	candidate, signature, ok := normalizeSQLIncrementalJoinCandidate(SQLIncrementalJoinCandidate{
		Key:              "request",
		LeftSource:       request.LeftSource,
		RightSource:      request.RightSource,
		LeftField:        request.LeftField,
		RightField:       request.RightField,
		SourceGeneration: request.SourceGeneration,
	})
	if !ok {
		return SQLIncrementalJoinRequest{}, sqlIncrementalJoinSignature{}, false
	}
	return SQLIncrementalJoinRequest{
		LeftSource:       candidate.LeftSource,
		RightSource:      candidate.RightSource,
		LeftField:        candidate.LeftField,
		RightField:       candidate.RightField,
		SourceGeneration: candidate.SourceGeneration,
	}, signature, true
}

func normalizeSQLIncrementalJoinSource(source string) (string, bool) {
	source = strings.TrimSpace(source)
	if !validSQLIncrementalJoinText(source) {
		return "", false
	}
	return strings.ToLower(source), true
}

func normalizeSQLIncrementalJoinField(field string) (string, bool) {
	field = strings.TrimSpace(field)
	if separator := strings.LastIndexByte(field, '.'); separator >= 0 {
		field = field[separator+1:]
	}
	if !validSQLIncrementalJoinText(field) {
		return "", false
	}
	return strings.ToLower(field), true
}

func validSQLIncrementalJoinText(value string) bool {
	return value != "" && len(value) <= maxSQLIncrementalJoinSelectorTextBytes
}

func sqlIncrementalJoinDecisionBetter(left, right SQLIncrementalJoinDecision) bool {
	if left.Stale != right.Stale {
		return !left.Stale
	}
	if left.Score.NetBenefitNanos != right.Score.NetBenefitNanos {
		return left.Score.NetBenefitNanos > right.Score.NetBenefitNanos
	}
	if left.Score.PaybackReads != right.Score.PaybackReads {
		return left.Score.PaybackReads < right.Score.PaybackReads
	}
	if left.Candidate.MemoryBytes != right.Candidate.MemoryBytes {
		return left.Candidate.MemoryBytes < right.Candidate.MemoryBytes
	}
	if left.Candidate.Key != right.Candidate.Key {
		return left.Candidate.Key < right.Candidate.Key
	}
	return left.Candidate.SourceGeneration > right.Candidate.SourceGeneration
}

func sqlIncrementalJoinCandidateLess(left, right SQLIncrementalJoinCandidate) bool {
	if left.LeftSource != right.LeftSource {
		return left.LeftSource < right.LeftSource
	}
	if left.RightSource != right.RightSource {
		return left.RightSource < right.RightSource
	}
	if left.LeftField != right.LeftField {
		return left.LeftField < right.LeftField
	}
	if left.RightField != right.RightField {
		return left.RightField < right.RightField
	}
	return left.Key < right.Key
}

func sqlIncrementalJoinCostCandidate(candidate SQLIncrementalJoinCandidate) SQLArrangementCostCandidate {
	return SQLArrangementCostCandidate{
		Key:              candidate.Key,
		Field:            candidate.LeftField + "=" + candidate.RightField,
		Kind:             "join",
		BuildCostNanos:   candidate.BuildCostNanos,
		ProbeCostNanos:   candidate.ProbeCostNanos,
		ScanCostNanos:    candidate.ScanCostNanos,
		MaintenanceNanos: candidate.MaintenanceNanos,
		ExpectedReads:    candidate.ExpectedReads,
		ExpectedWrites:   candidate.ExpectedWrites,
		MemoryBytes:      candidate.MemoryBytes,
	}
}

func sqlIncrementalJoinSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
