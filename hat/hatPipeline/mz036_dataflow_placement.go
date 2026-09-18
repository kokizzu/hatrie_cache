package hatPipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// DefaultDataflowPlacementMaxOperators bounds one placement request when
	// DataflowPlacementOptions.MaxOperators is zero.
	DefaultDataflowPlacementMaxOperators = 4096
	// DefaultDataflowPlacementMaxWorkers bounds one placement request when
	// DataflowPlacementOptions.MaxWorkers is zero.
	DefaultDataflowPlacementMaxWorkers = 256
	// DefaultDataflowPlacementMaxAssignments bounds the returned plan when
	// DataflowPlacementOptions.MaxAssignments is zero.
	DefaultDataflowPlacementMaxAssignments = 1 << 20

	maxDataflowPlacementOperators   = 1 << 20
	maxDataflowPlacementWorkers     = 1 << 16
	maxDataflowPlacementAssignments = 1 << 24
	maxDataflowPlacementIDSize      = 256
	maxDataflowPlacementDomainSize  = 256
)

var (
	// ErrDataflowPlacementInvalid reports malformed placement metadata or
	// invalid bounds.
	ErrDataflowPlacementInvalid = errors.New("hatPipeline: invalid dataflow placement input")
	// ErrDataflowPlacementUnsatisfied reports a valid request that cannot fit
	// the available worker capacity and failure-domain constraints.
	ErrDataflowPlacementUnsatisfied = errors.New("hatPipeline: dataflow placement constraints are unsatisfied")
)

// DataflowPlacementWorker describes one worker that may host operator
// replicas. Capacity is the total number of replicas the worker may receive;
// zero selects a capacity of one.
type DataflowPlacementWorker struct {
	ID            string
	FailureDomain string
	Capacity      int
}

// DataflowPlacementOperator describes the replica and failure-domain
// requirements for one dataflow operator. An empty AllowedFailureDomains list
// permits every worker domain.
type DataflowPlacementOperator struct {
	ID                    string
	Replicas              int
	MinFailureDomains     int
	AllowedFailureDomains []string
}

// DataflowPlacementOptions bounds a placement request. Zero values select the
// documented defaults. The planner is deterministic and does not move running
// operators; callers decide when and how to apply the returned plan.
type DataflowPlacementOptions struct {
	MaxOperators   int
	MaxWorkers     int
	MaxAssignments int
}

// DataflowPlacementAssignment places one replica on one worker.
type DataflowPlacementAssignment struct {
	OperatorID    string
	Replica       int
	WorkerID      string
	FailureDomain string
}

// DataflowPlacementPlan is a deterministic detached placement plan. Entries
// are sorted by operator ID and then replica number.
type DataflowPlacementPlan struct {
	Assignments []DataflowPlacementAssignment
}

type normalizedDataflowPlacementWorker struct {
	DataflowPlacementWorker
	remaining int
	load      int
}

type normalizedDataflowPlacementOperator struct {
	DataflowPlacementOperator
	allowed map[string]struct{}
}

// PlanDataflowOperatorPlacement assigns every requested replica to a distinct
// worker for its operator, preferring unused failure domains until the
// operator's minimum is met and then preferring the least-loaded worker.
// Operator and worker input order is ignored. No worker is used twice for the
// same operator, and a worker's capacity is shared across all operators.
func PlanDataflowOperatorPlacement(
	operators []DataflowPlacementOperator,
	workers []DataflowPlacementWorker,
	options DataflowPlacementOptions,
) (DataflowPlacementPlan, error) {
	maxOperators, maxWorkers, maxAssignments, err := normalizeDataflowPlacementOptions(options)
	if err != nil {
		return DataflowPlacementPlan{}, err
	}
	if len(operators) == 0 || len(operators) > maxOperators || len(workers) == 0 || len(workers) > maxWorkers {
		return DataflowPlacementPlan{}, fmt.Errorf("%w: operator or worker count", ErrDataflowPlacementInvalid)
	}

	normalizedOperators, totalAssignments, err := normalizeDataflowPlacementOperators(operators, maxAssignments)
	if err != nil {
		return DataflowPlacementPlan{}, err
	}
	normalizedWorkers, err := normalizeDataflowPlacementWorkers(workers)
	if err != nil {
		return DataflowPlacementPlan{}, err
	}

	assignments := make([]DataflowPlacementAssignment, 0, totalAssignments)
	for _, operator := range normalizedOperators {
		selectedWorkers := make(map[string]struct{}, operator.Replicas)
		usedDomains := make(map[string]struct{}, operator.MinFailureDomains)
		for replica := 0; replica < operator.Replicas; replica++ {
			needNewDomain := len(usedDomains) < operator.MinFailureDomains
			workerIndex := chooseDataflowPlacementWorker(normalizedWorkers, operator, selectedWorkers, usedDomains, needNewDomain)
			if workerIndex < 0 {
				return DataflowPlacementPlan{}, fmt.Errorf("%w: operator %q replica %d", ErrDataflowPlacementUnsatisfied, operator.ID, replica)
			}

			worker := &normalizedWorkers[workerIndex]
			worker.remaining--
			worker.load++
			selectedWorkers[worker.ID] = struct{}{}
			usedDomains[worker.FailureDomain] = struct{}{}
			assignments = append(assignments, DataflowPlacementAssignment{
				OperatorID:    operator.ID,
				Replica:       replica,
				WorkerID:      worker.ID,
				FailureDomain: worker.FailureDomain,
			})
		}
	}

	return DataflowPlacementPlan{Assignments: assignments}, nil
}

func normalizeDataflowPlacementOptions(options DataflowPlacementOptions) (int, int, int, error) {
	maxOperators, err := normalizeDataflowPlacementLimit(options.MaxOperators, DefaultDataflowPlacementMaxOperators, maxDataflowPlacementOperators)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: max operators", ErrDataflowPlacementInvalid)
	}
	maxWorkers, err := normalizeDataflowPlacementLimit(options.MaxWorkers, DefaultDataflowPlacementMaxWorkers, maxDataflowPlacementWorkers)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: max workers", ErrDataflowPlacementInvalid)
	}
	maxAssignments, err := normalizeDataflowPlacementLimit(options.MaxAssignments, DefaultDataflowPlacementMaxAssignments, maxDataflowPlacementAssignments)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: max assignments", ErrDataflowPlacementInvalid)
	}
	return maxOperators, maxWorkers, maxAssignments, nil
}

func normalizeDataflowPlacementLimit(value, defaultValue, maximum int) (int, error) {
	if value == 0 {
		return defaultValue, nil
	}
	if value < 1 || value > maximum {
		return 0, errors.New("invalid placement limit")
	}
	return value, nil
}

func normalizeDataflowPlacementOperators(input []DataflowPlacementOperator, maxAssignments int) ([]normalizedDataflowPlacementOperator, int, error) {
	operators := make([]normalizedDataflowPlacementOperator, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	totalAssignments := 0
	for _, inputOperator := range input {
		id := strings.TrimSpace(inputOperator.ID)
		if id == "" || len(id) > maxDataflowPlacementIDSize {
			return nil, 0, fmt.Errorf("%w: operator ID", ErrDataflowPlacementInvalid)
		}
		if _, exists := seen[id]; exists {
			return nil, 0, fmt.Errorf("%w: duplicate operator %q", ErrDataflowPlacementInvalid, id)
		}
		seen[id] = struct{}{}

		replicas := inputOperator.Replicas
		if replicas == 0 {
			replicas = 1
		}
		minDomains := inputOperator.MinFailureDomains
		if minDomains == 0 {
			minDomains = 1
		}
		if replicas < 1 || minDomains < 1 || minDomains > replicas {
			return nil, 0, fmt.Errorf("%w: operator %q replica or domain requirement", ErrDataflowPlacementInvalid, id)
		}
		if replicas > maxAssignments-totalAssignments {
			return nil, 0, fmt.Errorf("%w: assignment count", ErrDataflowPlacementInvalid)
		}

		allowed, err := normalizeDataflowPlacementDomains(inputOperator.AllowedFailureDomains)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: operator %q allowed domains", ErrDataflowPlacementInvalid, id)
		}
		operators = append(operators, normalizedDataflowPlacementOperator{
			DataflowPlacementOperator: DataflowPlacementOperator{
				ID:                    id,
				Replicas:              replicas,
				MinFailureDomains:     minDomains,
				AllowedFailureDomains: allowedList(allowed),
			},
			allowed: allowed,
		})
		totalAssignments += replicas
	}
	sort.Slice(operators, func(i, j int) bool {
		return operators[i].ID < operators[j].ID
	})
	return operators, totalAssignments, nil
}

func normalizeDataflowPlacementWorkers(input []DataflowPlacementWorker) ([]normalizedDataflowPlacementWorker, error) {
	workers := make([]normalizedDataflowPlacementWorker, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, inputWorker := range input {
		id := strings.TrimSpace(inputWorker.ID)
		domain := strings.TrimSpace(inputWorker.FailureDomain)
		if id == "" || len(id) > maxDataflowPlacementIDSize || domain == "" || len(domain) > maxDataflowPlacementDomainSize {
			return nil, fmt.Errorf("%w: worker ID or failure domain", ErrDataflowPlacementInvalid)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("%w: duplicate worker %q", ErrDataflowPlacementInvalid, id)
		}
		seen[id] = struct{}{}
		capacity := inputWorker.Capacity
		if capacity == 0 {
			capacity = 1
		}
		if capacity < 1 {
			return nil, fmt.Errorf("%w: worker %q capacity", ErrDataflowPlacementInvalid, id)
		}
		workers = append(workers, normalizedDataflowPlacementWorker{
			DataflowPlacementWorker: DataflowPlacementWorker{ID: id, FailureDomain: domain, Capacity: capacity},
			remaining:               capacity,
		})
	}
	sort.Slice(workers, func(i, j int) bool {
		if workers[i].ID != workers[j].ID {
			return workers[i].ID < workers[j].ID
		}
		return workers[i].FailureDomain < workers[j].FailureDomain
	})
	return workers, nil
}

func normalizeDataflowPlacementDomains(input []string) (map[string]struct{}, error) {
	if len(input) == 0 {
		return nil, nil
	}
	domains := make(map[string]struct{}, len(input))
	for _, inputDomain := range input {
		domain := strings.TrimSpace(inputDomain)
		if domain == "" || len(domain) > maxDataflowPlacementDomainSize {
			return nil, errors.New("invalid failure domain")
		}
		domains[domain] = struct{}{}
	}
	return domains, nil
}

func allowedList(domains map[string]struct{}) []string {
	if len(domains) == 0 {
		return nil
	}
	result := make([]string, 0, len(domains))
	for domain := range domains {
		result = append(result, domain)
	}
	sort.Strings(result)
	return result
}

func chooseDataflowPlacementWorker(
	workers []normalizedDataflowPlacementWorker,
	operator normalizedDataflowPlacementOperator,
	selectedWorkers map[string]struct{},
	usedDomains map[string]struct{},
	needNewDomain bool,
) int {
	best := -1
	for index := range workers {
		worker := workers[index]
		if worker.remaining == 0 {
			continue
		}
		if _, selected := selectedWorkers[worker.ID]; selected {
			continue
		}
		if len(operator.allowed) > 0 {
			if _, allowed := operator.allowed[worker.FailureDomain]; !allowed {
				continue
			}
		}
		if needNewDomain {
			if _, used := usedDomains[worker.FailureDomain]; used {
				continue
			}
		}
		if best < 0 || dataflowPlacementWorkerLess(worker, workers[best]) {
			best = index
		}
	}
	return best
}

func dataflowPlacementWorkerLess(candidate, current normalizedDataflowPlacementWorker) bool {
	if candidate.load != current.load {
		return candidate.load < current.load
	}
	if candidate.ID != current.ID {
		return candidate.ID < current.ID
	}
	return candidate.FailureDomain < current.FailureDomain
}
