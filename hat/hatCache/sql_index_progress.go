package hatCache

import (
	"context"
	"fmt"
)

// SQLJSONIndexRebuildState identifies one cooperative rebuild lifecycle state.
type SQLJSONIndexRebuildState string

const (
	SQLJSONIndexRebuildStateQueued    SQLJSONIndexRebuildState = "queued"
	SQLJSONIndexRebuildStateRunning   SQLJSONIndexRebuildState = "running"
	SQLJSONIndexRebuildStateCompleted SQLJSONIndexRebuildState = "completed"
	SQLJSONIndexRebuildStateFailed    SQLJSONIndexRebuildState = "failed"
	SQLJSONIndexRebuildStateCanceled  SQLJSONIndexRebuildState = "canceled"
)

// SQLJSONIndexRebuildProgress reports one queue-level rebuild transition. It
// deliberately contains no source values or indexed row data.
type SQLJSONIndexRebuildProgress struct {
	Key           string                   `json:"key"`
	Field         string                   `json:"field"`
	State         SQLJSONIndexRebuildState `json:"state"`
	QueuePosition int                      `json:"queue_position"`
	QueueLength   int                      `json:"queue_length"`
	Processed     int                      `json:"processed"`
	Total         int                      `json:"total"`
}

// RunScheduledSQLJSONIndexRebuildsWithProgress cooperatively processes queued
// index rebuilds and reports queue-level progress. Cancellation is checked
// between atomic rebuild units; a canceled or failed request remains pending
// and can be retried by a later call. The legacy runner and its defaults are
// unchanged.
func (ht *HatTrie) RunScheduledSQLJSONIndexRebuildsWithProgress(ctx context.Context, limit int, report func(SQLJSONIndexRebuildProgress)) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if ctx == nil {
		return 0, fmt.Errorf("SQL JSON index rebuild context is nil")
	}
	processed, total := 0, 0
	for limit <= 0 || processed < limit {
		request, queueLength, ok := ht.takeSQLJSONIndexRebuildRequest()
		if !ok {
			return processed, nil
		}
		if total == 0 {
			total = queueLength
		}
		ht.reportSQLJSONIndexRebuildProgress(report, SQLJSONIndexRebuildProgress{
			Key: request.key, Field: request.field, State: SQLJSONIndexRebuildStateQueued,
			QueuePosition: 1, QueueLength: queueLength, Processed: processed, Total: total,
		})
		if err := ctx.Err(); err != nil {
			ht.requeueSQLJSONIndexRebuildRequest(request)
			ht.reportSQLJSONIndexRebuildProgress(report, SQLJSONIndexRebuildProgress{
				Key: request.key, Field: request.field, State: SQLJSONIndexRebuildStateCanceled,
				QueuePosition: 1, QueueLength: queueLength, Processed: processed, Total: total,
			})
			return processed, err
		}
		ht.reportSQLJSONIndexRebuildProgress(report, SQLJSONIndexRebuildProgress{
			Key: request.key, Field: request.field, State: SQLJSONIndexRebuildStateRunning,
			QueuePosition: 1, QueueLength: queueLength - 1, Processed: processed, Total: total,
		})
		if err := ht.executeSQLJSONIndexRebuildRequest(request); err != nil {
			ht.reportSQLJSONIndexRebuildProgress(report, SQLJSONIndexRebuildProgress{
				Key: request.key, Field: request.field, State: SQLJSONIndexRebuildStateFailed,
				QueuePosition: 1, QueueLength: queueLength, Processed: processed, Total: total,
			})
			return processed, err
		}
		processed++
		ht.reportSQLJSONIndexRebuildProgress(report, SQLJSONIndexRebuildProgress{
			Key: request.key, Field: request.field, State: SQLJSONIndexRebuildStateCompleted,
			QueuePosition: 1, QueueLength: queueLength - 1, Processed: processed, Total: total,
		})
	}
	return processed, nil
}

func (ht *HatTrie) reportSQLJSONIndexRebuildProgress(report func(SQLJSONIndexRebuildProgress), progress SQLJSONIndexRebuildProgress) {
	if report != nil {
		report(progress)
	}
}

func (ht *HatTrie) takeSQLJSONIndexRebuildRequest() (sqlJSONIndexRebuildRequest, int, bool) {
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if len(ht.sqlJSONIndexRebuildQueue) == 0 {
		return sqlJSONIndexRebuildRequest{}, 0, false
	}
	queueLength := len(ht.sqlJSONIndexRebuildQueue)
	request := ht.sqlJSONIndexRebuildQueue[0]
	ht.sqlJSONIndexRebuildQueue[0] = sqlJSONIndexRebuildRequest{}
	ht.sqlJSONIndexRebuildQueue = ht.sqlJSONIndexRebuildQueue[1:]
	delete(ht.sqlJSONIndexRebuildPending[request.key], request.field)
	return request, queueLength, true
}

func (ht *HatTrie) requeueSQLJSONIndexRebuildRequest(request sqlJSONIndexRebuildRequest) {
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONIndexRebuildPending == nil {
		ht.sqlJSONIndexRebuildPending = map[string]map[string]bool{}
	}
	if ht.sqlJSONIndexRebuildPending[request.key] == nil {
		ht.sqlJSONIndexRebuildPending[request.key] = map[string]bool{}
	}
	if ht.sqlJSONIndexRebuildPending[request.key][request.field] {
		return
	}
	ht.sqlJSONIndexRebuildPending[request.key][request.field] = true
	ht.sqlJSONIndexRebuildQueue = append([]sqlJSONIndexRebuildRequest{request}, ht.sqlJSONIndexRebuildQueue...)
}

func (ht *HatTrie) executeSQLJSONIndexRebuildRequest(request sqlJSONIndexRebuildRequest) error {
	source, err := ht.sqlJSONSource(request.key)
	if err != nil {
		ht.requeueSQLJSONIndexRebuildRequest(request)
		return err
	}
	ht.sqlIndexMu.Lock()
	rebuilt, err := ht.refreshSQLJSONIndexesLocked(request.key, request.field, source)
	if err != nil {
		ht.sqlIndexMu.Unlock()
		ht.requeueSQLJSONIndexRebuildRequest(request)
		return err
	}
	maintenance := ht.sqlJSONIndexMaintenanceLocked(request.key, request.field)
	maintenance.runs++
	maintenance.rebuilds += uint64(rebuilt)
	ht.sqlIndexMu.Unlock()
	return nil
}
