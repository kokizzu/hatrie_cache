package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"strings"
)

var (
	ErrUpsertChangefeedNil                     = errors.New("hatSql: upsert changefeed is nil")
	ErrUpsertChangefeedKeyColumnsRequired      = errors.New("hatSql: upsert changefeed key columns are required")
	ErrUpsertChangefeedKeyColumnEmpty          = errors.New("hatSql: upsert changefeed key column is empty")
	ErrUpsertChangefeedDuplicateKeyColumn      = errors.New("hatSql: upsert changefeed key column is duplicated")
	ErrUpsertChangefeedDuplicateKey            = errors.New("hatSql: upsert changefeed key is not unique")
	ErrUpsertChangefeedUnsupportedMultiplicity = errors.New("hatSql: upsert changefeed requires unit row multiplicity")
	ErrUpsertChangefeedUnknownDelete           = errors.New("hatSql: upsert changefeed deleted an unknown key")
)

// UpsertEnvelope is the compact row shape for consumers that maintain a
// keyed table. Key is always present; Current is the complete current row for
// an upsert and nil for a deletion. The row maps are detached from the
// changefeed and may be mutated by the consumer.
type UpsertEnvelope struct {
	Key      Row    `json:"key"`
	Current  Row    `json:"current,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
	ID       uint64 `json:"id,omitempty"`
	Revision uint64 `json:"revision,omitempty"`
	Frontier uint64 `json:"frontier,omitempty"`
}

// UpsertChangefeedOptions configures the stateful differential-to-upsert
// adapter. KeyColumns must identify at most one current row per key.
type UpsertChangefeedOptions struct {
	KeyColumns []string
}

// UpsertChangefeed converts differential query batches into compact keyed
// envelopes. It retains only one current row per stable key and does not
// retain before images. It is caller-owned and not safe for concurrent Apply
// calls.
type UpsertChangefeed struct {
	keyColumns  []string
	rows        map[string]Row
	initialized bool
}

// NewUpsertChangefeed creates an opt-in differential-to-upsert adapter.
func NewUpsertChangefeed(options UpsertChangefeedOptions) (*UpsertChangefeed, error) {
	if len(options.KeyColumns) == 0 {
		return nil, ErrUpsertChangefeedKeyColumnsRequired
	}
	keyColumns := make([]string, 0, len(options.KeyColumns))
	seen := make(map[string]struct{}, len(options.KeyColumns))
	for _, column := range options.KeyColumns {
		column = strings.TrimSpace(column)
		if column == "" {
			return nil, ErrUpsertChangefeedKeyColumnEmpty
		}
		if _, exists := seen[column]; exists {
			return nil, ErrUpsertChangefeedDuplicateKeyColumn
		}
		seen[column] = struct{}{}
		keyColumns = append(keyColumns, column)
	}
	return &UpsertChangefeed{
		keyColumns: keyColumns,
		rows:       make(map[string]Row),
	}, nil
}

// Apply converts one differential batch. The first non-progress batch is an
// upsert snapshot. Later batches produce one current image per changed key or
// one tombstone for a deletion. Reset batches replace the retained state.
func (feed *UpsertChangefeed) Apply(batch QuerySubscriptionDeltaBatch) ([]UpsertEnvelope, error) {
	if feed == nil {
		return nil, ErrUpsertChangefeedNil
	}
	if batch.Progress {
		return nil, nil
	}
	folded, err := foldQuerySubscriptionDeltaBatch(batch, false)
	if err != nil {
		return nil, err
	}
	batch = folded
	if !feed.initialized {
		rows, order, err := feed.initialRows(batch.Deltas)
		if err != nil {
			return nil, err
		}
		feed.rows = rows
		feed.initialized = true
		events := make([]UpsertEnvelope, 0, len(order))
		for _, key := range order {
			row := rows[key]
			events = append(events, feed.envelope(batch, keyRow(row, feed.keyColumns), row, false))
		}
		return events, nil
	}
	if batch.Reset {
		return feed.applyReset(batch)
	}
	return feed.applyDeltaBatch(batch)
}

type upsertDeltaState struct {
	key           string
	keyRow        Row
	positive      Row
	positiveCount int
	negativeCount int
	net           int64
}

func (feed *UpsertChangefeed) initialRows(deltas []QuerySubscriptionDelta) (map[string]Row, []string, error) {
	rows := make(map[string]Row, len(deltas))
	order := make([]string, 0, len(deltas))
	for _, delta := range deltas {
		if delta.Diff != 1 {
			return nil, nil, ErrUpsertChangefeedUnsupportedMultiplicity
		}
		key, err := feed.rowKey(delta.Row)
		if err != nil {
			return nil, nil, err
		}
		if _, exists := rows[key]; exists {
			return nil, nil, ErrUpsertChangefeedDuplicateKey
		}
		row := cloneDebeziumRow(delta.Row)
		rows[key] = row
		order = append(order, key)
	}
	return rows, order, nil
}

func (feed *UpsertChangefeed) applyDeltaBatch(batch QuerySubscriptionDeltaBatch) ([]UpsertEnvelope, error) {
	deltas := make(map[string]*upsertDeltaState, len(batch.Deltas))
	order := make([]string, 0, len(batch.Deltas))
	for _, delta := range batch.Deltas {
		if delta.Diff != 1 && delta.Diff != -1 {
			return nil, ErrUpsertChangefeedUnsupportedMultiplicity
		}
		key, err := feed.rowKey(delta.Row)
		if err != nil {
			return nil, err
		}
		state := deltas[key]
		if state == nil {
			state = &upsertDeltaState{key: key, keyRow: keyRow(delta.Row, feed.keyColumns)}
			deltas[key] = state
			order = append(order, key)
		}
		if delta.Diff > 0 {
			state.positiveCount++
			state.positive = cloneDebeziumRow(delta.Row)
		} else {
			state.negativeCount++
		}
		state.net += delta.Diff
		if state.positiveCount > 1 || state.negativeCount > 1 {
			return nil, ErrUpsertChangefeedDuplicateKey
		}
	}

	events := make([]UpsertEnvelope, 0, len(order))
	for _, key := range order {
		state := deltas[key]
		before, existed := feed.rows[key]
		switch {
		case !existed && state.net == 1 && state.positive != nil:
			events = append(events, feed.envelope(batch, state.keyRow, state.positive, false))
		case existed && state.net == -1 && state.positive == nil:
			events = append(events, feed.envelope(batch, state.keyRow, nil, true))
		case existed && state.net == 0 && state.positive != nil:
			if reflect.DeepEqual(before, state.positive) {
				continue
			}
			events = append(events, feed.envelope(batch, state.keyRow, state.positive, false))
		default:
			if state.net < 0 && !existed {
				return nil, ErrUpsertChangefeedUnknownDelete
			}
			return nil, ErrUpsertChangefeedUnsupportedMultiplicity
		}
	}
	for _, key := range order {
		state := deltas[key]
		switch {
		case state.net == -1 && state.positive == nil:
			delete(feed.rows, key)
		case state.positive != nil:
			// state.positive is already detached from the input batch and is
			// never exposed directly; transfer ownership into retained state.
			feed.rows[key] = state.positive
		}
	}
	return events, nil
}

func (feed *UpsertChangefeed) applyReset(batch QuerySubscriptionDeltaBatch) ([]UpsertEnvelope, error) {
	next, order, err := feed.initialRows(batch.Deltas)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(order))
	events := make([]UpsertEnvelope, 0, len(feed.rows)+len(order))
	for _, key := range order {
		seen[key] = struct{}{}
		before, existed := feed.rows[key]
		after := next[key]
		if !existed || !reflect.DeepEqual(before, after) {
			events = append(events, feed.envelope(batch, keyRow(after, feed.keyColumns), after, false))
		}
	}
	oldKeys := make([]string, 0, len(feed.rows))
	for key := range feed.rows {
		if _, exists := seen[key]; !exists {
			oldKeys = append(oldKeys, key)
		}
	}
	sort.Strings(oldKeys)
	for _, key := range oldKeys {
		events = append(events, feed.envelope(batch, keyRow(feed.rows[key], feed.keyColumns), nil, true))
	}
	feed.rows = next
	return events, nil
}

func (feed *UpsertChangefeed) rowKey(row Row) (string, error) {
	for _, column := range feed.keyColumns {
		value, exists := row[column]
		if !exists || value == nil {
			return "", ErrUpsertChangefeedKeyColumnEmpty
		}
	}
	return querySubscriptionRowKey(keyRow(row, feed.keyColumns)), nil
}

func (feed *UpsertChangefeed) envelope(batch QuerySubscriptionDeltaBatch, key, current Row, deleted bool) UpsertEnvelope {
	return UpsertEnvelope{
		Key:      cloneDebeziumRow(key),
		Current:  cloneDebeziumRow(current),
		Deleted:  deleted,
		ID:       batch.ID,
		Revision: batch.Revision,
		Frontier: batch.Frontier,
	}
}
