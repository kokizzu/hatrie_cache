package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"
)

var (
	ErrDebeziumChangefeedNil            = errors.New("hatSql: debezium changefeed is nil")
	ErrDebeziumKeyColumnsRequired       = errors.New("hatSql: debezium key columns are required")
	ErrDebeziumKeyColumnEmpty           = errors.New("hatSql: debezium key column is empty")
	ErrDebeziumDuplicateKey             = errors.New("hatSql: debezium changefeed key is not unique")
	ErrDebeziumUnsupportedMultiplicity  = errors.New("hatSql: debezium changefeed requires unit row multiplicity")
	ErrDebeziumUnknownDelete            = errors.New("hatSql: debezium changefeed deleted an unknown key")
	ErrDebeziumInvalidChangefeedOptions = errors.New("hatSql: debezium changefeed options are invalid")
)

// DebeziumOperation identifies the row transition represented by a change.
type DebeziumOperation string

const (
	DebeziumCreate DebeziumOperation = "c"
	DebeziumUpdate DebeziumOperation = "u"
	DebeziumDelete DebeziumOperation = "d"
	DebeziumRead   DebeziumOperation = "r"
)

// DebeziumSource identifies the producer of a changefeed payload. Snapshot is
// true only for the initial read events emitted by a changefeed.
type DebeziumSource struct {
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Connector string `json:"connector,omitempty"`
	Server    string `json:"server,omitempty"`
	Snapshot  bool   `json:"snapshot"`
}

// DebeziumPayload is the standard row-change portion of a Debezium envelope.
// Before is populated for updates and deletes; After is populated for reads,
// creates, and updates.
type DebeziumPayload struct {
	Before Row               `json:"before"`
	After  Row               `json:"after"`
	Source DebeziumSource    `json:"source"`
	Op     DebeziumOperation `json:"op"`
	TsMs   int64             `json:"ts_ms"`
}

// DebeziumChange is a key plus a Debezium payload. Frontier, Revision, and ID
// preserve the source subscription position for durable downstream consumers.
type DebeziumChange struct {
	Key      Row             `json:"key"`
	Payload  DebeziumPayload `json:"payload"`
	ID       uint64          `json:"id,omitempty"`
	Revision uint64          `json:"revision,omitempty"`
	Frontier uint64          `json:"frontier,omitempty"`
}

// DebeziumChangefeedOptions configures the stateful differential-to-envelope
// adapter. KeyColumns must identify at most one result row; this is required to
// produce unambiguous before/after images.
type DebeziumChangefeedOptions struct {
	KeyColumns []string
	Source     DebeziumSource
	Clock      func() time.Time
}

// DebeziumChangefeed converts differential query batches into Debezium-style
// row events. It is intentionally caller-owned and not safe for concurrent
// Apply calls. The adapter retains only the keyed current result state.
type DebeziumChangefeed struct {
	keyColumns  []string
	source      DebeziumSource
	clock       func() time.Time
	rows        map[string]Row
	initialized bool
}

// NewDebeziumChangefeed creates an opt-in differential changefeed adapter.
func NewDebeziumChangefeed(options DebeziumChangefeedOptions) (*DebeziumChangefeed, error) {
	if len(options.KeyColumns) == 0 {
		return nil, ErrDebeziumKeyColumnsRequired
	}
	keyColumns := make([]string, 0, len(options.KeyColumns))
	seen := make(map[string]struct{}, len(options.KeyColumns))
	for _, column := range options.KeyColumns {
		column = strings.TrimSpace(column)
		if column == "" {
			return nil, ErrDebeziumKeyColumnEmpty
		}
		if _, exists := seen[column]; exists {
			return nil, ErrDebeziumInvalidChangefeedOptions
		}
		seen[column] = struct{}{}
		keyColumns = append(keyColumns, column)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	return &DebeziumChangefeed{
		keyColumns: keyColumns,
		source:     options.Source,
		clock:      clock,
		rows:       make(map[string]Row),
	}, nil
}

// Apply converts one differential batch. The first non-progress batch is
// emitted as snapshot reads. Later batches produce creates, updates, and
// deletes; progress-only batches produce no changes. Reset batches replace the
// retained state and emit the resulting transitions in deterministic order.
func (feed *DebeziumChangefeed) Apply(batch QuerySubscriptionDeltaBatch) ([]DebeziumChange, error) {
	if feed == nil {
		return nil, ErrDebeziumChangefeedNil
	}
	if batch.Progress {
		return nil, nil
	}
	if !feed.initialized {
		rows, order, err := feed.initialRows(batch.Deltas)
		if err != nil {
			return nil, err
		}
		feed.rows = rows
		feed.initialized = true
		changes := make([]DebeziumChange, 0, len(order))
		for _, key := range order {
			row := rows[key]
			changes = append(changes, feed.change(batch, keyRow(row, feed.keyColumns), nil, row, DebeziumRead, true))
		}
		return changes, nil
	}
	if batch.Reset {
		return feed.applyReset(batch)
	}
	return feed.applyDeltaBatch(batch)
}

type debeziumDeltaState struct {
	key           string
	keyRow        Row
	positive      Row
	positiveCount int
	negativeCount int
	net           int64
}

func (feed *DebeziumChangefeed) initialRows(deltas []QuerySubscriptionDelta) (map[string]Row, []string, error) {
	rows := make(map[string]Row, len(deltas))
	order := make([]string, 0, len(deltas))
	for _, delta := range deltas {
		if delta.Diff != 1 {
			return nil, nil, ErrDebeziumUnsupportedMultiplicity
		}
		key, err := feed.rowKey(delta.Row)
		if err != nil {
			return nil, nil, err
		}
		if _, exists := rows[key]; exists {
			return nil, nil, ErrDebeziumDuplicateKey
		}
		row := cloneDebeziumRow(delta.Row)
		rows[key] = row
		order = append(order, key)
	}
	return rows, order, nil
}

func (feed *DebeziumChangefeed) applyDeltaBatch(batch QuerySubscriptionDeltaBatch) ([]DebeziumChange, error) {
	deltas := make(map[string]*debeziumDeltaState, len(batch.Deltas))
	order := make([]string, 0, len(batch.Deltas))
	for _, delta := range batch.Deltas {
		if delta.Diff != 1 && delta.Diff != -1 {
			return nil, ErrDebeziumUnsupportedMultiplicity
		}
		key, err := feed.rowKey(delta.Row)
		if err != nil {
			return nil, err
		}
		state := deltas[key]
		if state == nil {
			state = &debeziumDeltaState{key: key, keyRow: keyRow(delta.Row, feed.keyColumns)}
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
			return nil, ErrDebeziumDuplicateKey
		}
	}

	changes := make([]DebeziumChange, 0, len(order))
	for _, key := range order {
		state := deltas[key]
		before, existed := feed.rows[key]
		var after Row
		var operation DebeziumOperation
		switch {
		case !existed && state.net == 1 && state.positive != nil:
			after = state.positive
			operation = DebeziumCreate
		case existed && state.net == -1 && state.positive == nil:
			operation = DebeziumDelete
		case existed && state.net == 0 && state.positive != nil:
			after = state.positive
			if reflect.DeepEqual(before, after) {
				continue
			}
			operation = DebeziumUpdate
		default:
			if state.net < 0 && !existed {
				return nil, ErrDebeziumUnknownDelete
			}
			return nil, ErrDebeziumUnsupportedMultiplicity
		}
		changes = append(changes, feed.change(batch, state.keyRow, before, after, operation, false))
	}
	for _, key := range order {
		state := deltas[key]
		switch {
		case state.net == -1 && state.positive == nil:
			delete(feed.rows, key)
		case state.positive != nil:
			feed.rows[key] = state.positive
		}
	}
	return changes, nil
}

func (feed *DebeziumChangefeed) applyReset(batch QuerySubscriptionDeltaBatch) ([]DebeziumChange, error) {
	next, order, err := feed.initialRows(batch.Deltas)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(order))
	changes := make([]DebeziumChange, 0, len(feed.rows)+len(order))
	for _, key := range order {
		seen[key] = struct{}{}
		before, existed := feed.rows[key]
		after := next[key]
		if !existed {
			changes = append(changes, feed.change(batch, keyRow(after, feed.keyColumns), nil, after, DebeziumCreate, false))
		} else if !reflect.DeepEqual(before, after) {
			changes = append(changes, feed.change(batch, keyRow(after, feed.keyColumns), before, after, DebeziumUpdate, false))
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
		before := feed.rows[key]
		changes = append(changes, feed.change(batch, keyRow(before, feed.keyColumns), before, nil, DebeziumDelete, false))
	}
	feed.rows = next
	return changes, nil
}

func (feed *DebeziumChangefeed) rowKey(row Row) (string, error) {
	for _, column := range feed.keyColumns {
		value, exists := row[column]
		if !exists || value == nil {
			return "", ErrDebeziumKeyColumnEmpty
		}
	}
	return querySubscriptionRowKey(keyRow(row, feed.keyColumns)), nil
}

func (feed *DebeziumChangefeed) change(batch QuerySubscriptionDeltaBatch, key Row, before, after Row, operation DebeziumOperation, snapshot bool) DebeziumChange {
	source := feed.source
	source.Snapshot = snapshot
	return DebeziumChange{
		Key:      cloneDebeziumRow(key),
		ID:       batch.ID,
		Revision: batch.Revision,
		Frontier: batch.Frontier,
		Payload: DebeziumPayload{
			Before: cloneDebeziumRow(before),
			After:  cloneDebeziumRow(after),
			Source: source,
			Op:     operation,
			TsMs:   feed.clock().UnixMilli(),
		},
	}
}

func keyRow(row Row, columns []string) Row {
	key := make(Row, len(columns))
	for _, column := range columns {
		key[column] = cloneDebeziumValue(row[column])
	}
	return key
}

func cloneDebeziumRows(rows map[string]Row) map[string]Row {
	clone := make(map[string]Row, len(rows))
	for key, row := range rows {
		clone[key] = cloneDebeziumRow(row)
	}
	return clone
}

func cloneDebeziumRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = cloneDebeziumValue(value)
	}
	return clone
}

func cloneDebeziumValue(value interface{}) interface{} {
	switch value := value.(type) {
	case Row:
		return cloneDebeziumRow(value)
	case map[string]interface{}:
		clone := make(map[string]interface{}, len(value))
		for key, child := range value {
			clone[key] = cloneDebeziumValue(child)
		}
		return clone
	case []interface{}:
		clone := make([]interface{}, len(value))
		for index, child := range value {
			clone[index] = cloneDebeziumValue(child)
		}
		return clone
	default:
		return value
	}
}
