package hatSql

// TypedTableChangeMetadata describes source history relevant to maintenance
// path selection. AppendOnly remains true only while the source has performed
// inserts and no updates or deletes.
type TypedTableChangeMetadata struct {
	AppendOnly bool
}

// ChangeMetadata returns the current source-history hint. The zero value is
// conservative and selects general maintenance when supplied by callers.
func (table *TypedTable) ChangeMetadata() TypedTableChangeMetadata {
	if table == nil {
		return TypedTableChangeMetadata{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return TypedTableChangeMetadata{AppendOnly: table.appendOnly}
}

// ApplyWithMetadata selects the monotone maintenance path from trusted source
// metadata. ApplyMonotone still validates each change, so an incorrect hint
// returns a validation error instead of changing aggregate state incorrectly.
func (aggregate *TypedTableAggregate) ApplyWithMetadata(changes []TypedTableChange, metadata TypedTableChangeMetadata) error {
	if metadata.AppendOnly {
		return aggregate.ApplyMonotone(changes)
	}
	return aggregate.Apply(changes)
}
