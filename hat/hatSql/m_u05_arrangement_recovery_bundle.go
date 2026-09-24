package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	// TypedTableArrangementRecoveryCheckpointVersion identifies the envelope
	// format used when several arrangement catalogs are recovered together.
	TypedTableArrangementRecoveryCheckpointVersion uint8 = 1
	// MaxTypedTableArrangementRecoveryCheckpoints bounds one recovery envelope
	// before it is routed into live arrangement catalogs.
	MaxTypedTableArrangementRecoveryCheckpoints = 1_000_000
)

var (
	ErrTypedTableArrangementRecoveryInvalid         = errors.New("typed table arrangement recovery checkpoint invalid")
	ErrTypedTableArrangementRecoveryCatalogNotFound = errors.New("typed table arrangement recovery catalog not found")
	ErrTypedTableArrangementRecoveryTargetActive    = errors.New("typed table arrangement recovery target is active")
	ErrTypedTableArrangementRecoveryLimit           = errors.New("typed table arrangement recovery checkpoint limit exceeded")
)

// TypedTableArrangementRecoveryCheckpoint is a detached checkpoint for all
// aggregate and join arrangements supplied to one capture operation. The
// nested checkpoints retain the existing source-version fence and wire format.
type TypedTableArrangementRecoveryCheckpoint struct {
	Version    uint8                                      `json:"version"`
	Aggregates []TypedTableAggregateArrangementCheckpoint `json:"aggregates"`
	Joins      []TypedTableJoinArrangementCheckpoint      `json:"joins"`
}

// TypedTableArrangementRecoveryLease owns all arrangements restored from one
// recovery checkpoint. Releasing it is idempotent and releases every nested
// arrangement, including arrangements restored before a later failure.
type TypedTableArrangementRecoveryLease struct {
	aggregates []*TypedTableAggregateArrangement
	joins      []*TypedTableJoinArrangement
	once       sync.Once
}

// AggregateArrangements returns the restored aggregate leases. The returned
// slice is independent, while its arrangement pointers remain owned by the
// recovery lease.
func (lease *TypedTableArrangementRecoveryLease) AggregateArrangements() []*TypedTableAggregateArrangement {
	if lease == nil {
		return nil
	}
	return append([]*TypedTableAggregateArrangement(nil), lease.aggregates...)
}

// JoinArrangements returns the restored join leases. The returned slice is
// independent, while its arrangement pointers remain owned by the recovery
// lease.
func (lease *TypedTableArrangementRecoveryLease) JoinArrangements() []*TypedTableJoinArrangement {
	if lease == nil {
		return nil
	}
	return append([]*TypedTableJoinArrangement(nil), lease.joins...)
}

// Release releases every arrangement owned by the recovery lease. It returns
// true only for the first call.
func (lease *TypedTableArrangementRecoveryLease) Release() bool {
	if lease == nil {
		return false
	}
	released := false
	lease.once.Do(func() {
		for _, arrangement := range lease.aggregates {
			if arrangement != nil {
				arrangement.Release()
			}
		}
		for _, arrangement := range lease.joins {
			if arrangement != nil {
				arrangement.Release()
			}
		}
		released = true
	})
	return released
}

// CaptureTypedTableArrangementRecovery captures every active arrangement in
// the supplied catalogs without rereading source rows. The result is sorted by
// source identity and arrangement definition for deterministic persistence.
func CaptureTypedTableArrangementRecovery(
	aggregateCatalogs []*TypedTableAggregateArrangements,
	joinCatalogs []*TypedTableJoinArrangements,
) (TypedTableArrangementRecoveryCheckpoint, error) {
	checkpoint := TypedTableArrangementRecoveryCheckpoint{
		Version: TypedTableArrangementRecoveryCheckpointVersion,
	}
	aggregateByName, err := typedTableArrangementRecoveryAggregateCatalogs(aggregateCatalogs)
	if err != nil {
		return TypedTableArrangementRecoveryCheckpoint{}, err
	}
	joinByName, err := typedTableArrangementRecoveryJoinCatalogs(joinCatalogs)
	if err != nil {
		return TypedTableArrangementRecoveryCheckpoint{}, err
	}

	seenAggregates := make(map[string]struct{})
	for _, name := range sortedTypedTableArrangementRecoveryAggregateCatalogNames(aggregateByName) {
		captured, err := aggregateByName[name].CaptureCheckpoints()
		if err != nil {
			return TypedTableArrangementRecoveryCheckpoint{}, err
		}
		if len(checkpoint.Aggregates)+len(captured) > MaxTypedTableArrangementRecoveryCheckpoints {
			return TypedTableArrangementRecoveryCheckpoint{}, ErrTypedTableArrangementRecoveryLimit
		}
		for _, arrangement := range captured {
			key := typedTableArrangementRecoveryAggregateKey(arrangement)
			if _, exists := seenAggregates[key]; exists {
				return TypedTableArrangementRecoveryCheckpoint{}, ErrTypedTableArrangementCheckpointDuplicate
			}
			seenAggregates[key] = struct{}{}
			checkpoint.Aggregates = append(checkpoint.Aggregates, arrangement)
		}
	}

	seenJoins := make(map[string]struct{})
	for _, name := range sortedTypedTableArrangementRecoveryJoinCatalogNames(joinByName) {
		captured, err := joinByName[name].CaptureCheckpoints()
		if err != nil {
			return TypedTableArrangementRecoveryCheckpoint{}, err
		}
		if len(checkpoint.Aggregates)+len(checkpoint.Joins)+len(captured) > MaxTypedTableArrangementRecoveryCheckpoints {
			return TypedTableArrangementRecoveryCheckpoint{}, ErrTypedTableArrangementRecoveryLimit
		}
		for _, arrangement := range captured {
			key := typedTableArrangementRecoveryJoinKey(arrangement)
			if _, exists := seenJoins[key]; exists {
				return TypedTableArrangementRecoveryCheckpoint{}, ErrTypedTableArrangementCheckpointDuplicate
			}
			seenJoins[key] = struct{}{}
			checkpoint.Joins = append(checkpoint.Joins, arrangement)
		}
	}
	return checkpoint, nil
}

// RestoreTypedTableArrangementRecovery restores all aggregate and join
// arrangements in a checkpoint without source replay. All catalog identities,
// duplicate definitions, and target activity are validated before mutation;
// if a later catalog fails, every lease created by this call is released.
func RestoreTypedTableArrangementRecovery(
	aggregateCatalogs []*TypedTableAggregateArrangements,
	joinCatalogs []*TypedTableJoinArrangements,
	checkpoint TypedTableArrangementRecoveryCheckpoint,
) (*TypedTableArrangementRecoveryLease, error) {
	if err := validateTypedTableArrangementRecoveryCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	aggregateByName, err := typedTableArrangementRecoveryAggregateCatalogs(aggregateCatalogs)
	if err != nil {
		return nil, err
	}
	joinByName, err := typedTableArrangementRecoveryJoinCatalogs(joinCatalogs)
	if err != nil {
		return nil, err
	}

	aggregateCheckpoints := make(map[string][]TypedTableAggregateArrangementCheckpoint)
	seenAggregates := make(map[string]struct{}, len(checkpoint.Aggregates))
	for _, arrangement := range checkpoint.Aggregates {
		key := typedTableArrangementRecoveryAggregateKey(arrangement)
		if _, exists := seenAggregates[key]; exists {
			return nil, ErrTypedTableArrangementCheckpointDuplicate
		}
		seenAggregates[key] = struct{}{}
		catalog := aggregateByName[arrangement.TableName]
		if catalog == nil {
			return nil, fmt.Errorf("%w: aggregate table %q", ErrTypedTableArrangementRecoveryCatalogNotFound, arrangement.TableName)
		}
		aggregateCheckpoints[arrangement.TableName] = append(aggregateCheckpoints[arrangement.TableName], arrangement)
	}

	joinCheckpoints := make(map[string][]TypedTableJoinArrangementCheckpoint)
	seenJoins := make(map[string]struct{}, len(checkpoint.Joins))
	for _, arrangement := range checkpoint.Joins {
		key := typedTableArrangementRecoveryJoinKey(arrangement)
		if _, exists := seenJoins[key]; exists {
			return nil, ErrTypedTableArrangementCheckpointDuplicate
		}
		seenJoins[key] = struct{}{}
		catalogKey := typedTableArrangementRecoveryJoinCatalogKey(arrangement.LeftTableName, arrangement.RightTableName)
		catalog := joinByName[catalogKey]
		if catalog == nil {
			return nil, fmt.Errorf("%w: join %q", ErrTypedTableArrangementRecoveryCatalogNotFound, catalogKey)
		}
		joinCheckpoints[catalogKey] = append(joinCheckpoints[catalogKey], arrangement)
	}

	for name := range aggregateCheckpoints {
		if aggregateByName[name].Active() != 0 {
			return nil, fmt.Errorf("%w: aggregate table %q", ErrTypedTableArrangementRecoveryTargetActive, name)
		}
	}
	for name := range joinCheckpoints {
		if joinByName[name].Active() != 0 {
			return nil, fmt.Errorf("%w: join %q", ErrTypedTableArrangementRecoveryTargetActive, name)
		}
	}

	lease := &TypedTableArrangementRecoveryLease{}
	for _, name := range sortedTypedTableArrangementRecoveryAggregateCheckpointNames(aggregateCheckpoints) {
		restored, err := aggregateByName[name].RestoreCheckpoints(aggregateCheckpoints[name])
		if err != nil {
			lease.Release()
			return nil, fmt.Errorf("restore aggregate table %q: %w", name, err)
		}
		lease.aggregates = append(lease.aggregates, restored...)
	}
	for _, name := range sortedTypedTableArrangementRecoveryJoinCheckpointNames(joinCheckpoints) {
		restored, err := joinByName[name].RestoreCheckpoints(joinCheckpoints[name])
		if err != nil {
			lease.Release()
			return nil, fmt.Errorf("restore join %q: %w", name, err)
		}
		lease.joins = append(lease.joins, restored...)
	}
	return lease, nil
}

func validateTypedTableArrangementRecoveryCheckpoint(checkpoint TypedTableArrangementRecoveryCheckpoint) error {
	if checkpoint.Version != TypedTableArrangementRecoveryCheckpointVersion {
		return ErrTypedTableArrangementRecoveryInvalid
	}
	if len(checkpoint.Aggregates)+len(checkpoint.Joins) > MaxTypedTableArrangementRecoveryCheckpoints {
		return ErrTypedTableArrangementRecoveryLimit
	}
	return nil
}

func typedTableArrangementRecoveryAggregateCatalogs(catalogs []*TypedTableAggregateArrangements) (map[string]*TypedTableAggregateArrangements, error) {
	byName := make(map[string]*TypedTableAggregateArrangements, len(catalogs))
	for _, catalog := range catalogs {
		if catalog == nil || catalog.table == nil {
			return nil, ErrTypedTableArrangementRecoveryInvalid
		}
		name := catalog.table.schema.Name
		if _, exists := byName[name]; exists {
			return nil, fmt.Errorf("%w: duplicate aggregate table %q", ErrTypedTableArrangementRecoveryInvalid, name)
		}
		byName[name] = catalog
	}
	return byName, nil
}

func typedTableArrangementRecoveryJoinCatalogs(catalogs []*TypedTableJoinArrangements) (map[string]*TypedTableJoinArrangements, error) {
	byName := make(map[string]*TypedTableJoinArrangements, len(catalogs))
	for _, catalog := range catalogs {
		if catalog == nil || catalog.left == nil || catalog.right == nil {
			return nil, ErrTypedTableArrangementRecoveryInvalid
		}
		key := typedTableArrangementRecoveryJoinCatalogKey(catalog.left.schema.Name, catalog.right.schema.Name)
		if _, exists := byName[key]; exists {
			return nil, fmt.Errorf("%w: duplicate join %q", ErrTypedTableArrangementRecoveryInvalid, key)
		}
		byName[key] = catalog
	}
	return byName, nil
}

func typedTableArrangementRecoveryAggregateKey(checkpoint TypedTableAggregateArrangementCheckpoint) string {
	return checkpoint.TableName + "\x00" + typedTableAggregateArrangementKey(checkpoint.Definition)
}

func typedTableArrangementRecoveryJoinCatalogKey(left, right string) string {
	return left + "\x00" + right
}

func typedTableArrangementRecoveryJoinKey(checkpoint TypedTableJoinArrangementCheckpoint) string {
	return typedTableArrangementRecoveryJoinCatalogKey(checkpoint.LeftTableName, checkpoint.RightTableName) + "\x00" + checkpoint.Definition.LeftField + "\x00" + checkpoint.Definition.RightField
}

func sortedTypedTableArrangementRecoveryAggregateCatalogNames(catalogs map[string]*TypedTableAggregateArrangements) []string {
	names := make([]string, 0, len(catalogs))
	for name := range catalogs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTypedTableArrangementRecoveryJoinCatalogNames(catalogs map[string]*TypedTableJoinArrangements) []string {
	names := make([]string, 0, len(catalogs))
	for name := range catalogs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTypedTableArrangementRecoveryAggregateCheckpointNames(checkpoints map[string][]TypedTableAggregateArrangementCheckpoint) []string {
	names := make([]string, 0, len(checkpoints))
	for name := range checkpoints {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTypedTableArrangementRecoveryJoinCheckpointNames(checkpoints map[string][]TypedTableJoinArrangementCheckpoint) []string {
	names := make([]string, 0, len(checkpoints))
	for name := range checkpoints {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
