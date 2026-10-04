package hatCache

import "net/http"

func (handler *MonitoringHandler) handleSQLQueryLogFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	queryLog := handler.options.QueryLog
	if queryLog == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable, commandError("SQL query log monitoring is not configured"))
		return
	}
	if err := queryLog.Sync(); err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "sql.query_log.flush", OK: false, Status: http.StatusServiceUnavailable, Message: err.Error()})
		writeJSONStatus(w, http.StatusServiceUnavailable, commandError(err.Error()))
		return
	}
	handler.auditHTTP(r, AuditEvent{Action: "sql.query_log.flush", OK: true, Status: http.StatusOK})
	writeJSON(w, map[string]bool{"flushed": true})
}
