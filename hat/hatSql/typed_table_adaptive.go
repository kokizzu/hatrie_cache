package hatSql

// TypedTableChangesAreMonotone reports whether every change is an insert-only
// change with a non-empty after row. Empty batches are monotone. Callers can
// use this proof to select the cheaper insert-only maintenance path.
func TypedTableChangesAreMonotone(changes []TypedTableChange) bool {
	for _, change := range changes {
		if len(change.Before) != 0 || len(change.After) == 0 {
			return false
		}
	}
	return true
}

// ApplyAuto selects ApplyMonotone for a proven insert-only batch and retains
// the general Apply implementation for updates, deletes, and empty changes.
func (aggregate *TypedTableAggregate) ApplyAuto(changes []TypedTableChange) error {
	if TypedTableChangesAreMonotone(changes) {
		return aggregate.ApplyMonotone(changes)
	}
	return aggregate.Apply(changes)
}
