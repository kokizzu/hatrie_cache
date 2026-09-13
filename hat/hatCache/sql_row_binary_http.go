package hatCache

import (
	"mime"
	"net/http"
	"strconv"
	"strings"

	"hatrie_cache/hat/hatSql"
)

const defaultSQLRowBinaryImportMaxBytes int64 = 1 << 30

func monitoringSQLRequestAcceptsRowBinary(accept string) bool {
	for _, item := range strings.Split(accept, ",") {
		mediaType, parameters, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil || !strings.EqualFold(mediaType, hatSql.SQLRowBinaryStreamContentType) {
			continue
		}
		if strings.TrimSpace(parameters["q"]) == "0" {
			continue
		}
		return true
	}
	return false
}

func (handler *MonitoringHandler) handleSQLRowBinaryStream(w http.ResponseWriter, r *http.Request, request SQLQueryRequest, query *hatSql.ParsedQuery, sources []string) {
	w.Header().Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	w.Header().Set("Vary", "Accept")
	writer := hatSql.NewSQLRowBinaryStreamWriter(w, SQLQueryColumns(query))
	flusher, _ := w.(http.Flusher)
	rows := 0
	resolver := monitoringSQLResolver{source: handler.trie, functions: handler.sqlFunctions}
	err := ExecuteSQLQueryRows(r.Context(), request.Query, resolver, request.Parameters, handler.options.SQLQueryOptions, func(_ []string, row SQLRow) error {
		if err := writer.WriteRow(row); err != nil {
			return err
		}
		rows++
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	if err != nil {
		handler.auditSQLQuery(r, request, sources, rows, false, http.StatusBadRequest, err.Error())
		if rows == 0 {
			writeJSONStatus(w, http.StatusBadRequest, commandError(FormatSQLDiagnostic(request.Query, err)))
		}
		return
	}
	if err := writer.Finish(); err != nil {
		handler.auditSQLQuery(r, request, sources, rows, false, http.StatusInternalServerError, err.Error())
		if rows == 0 {
			writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		}
		return
	}
	handler.auditSQLQuery(r, request, sources, rows, true, http.StatusOK, "")
}

func (handler *MonitoringHandler) handleSQLRowBinaryImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) || !handler.requireTrie(w) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "sql.import", map[string]interface{}{"format": hatSql.SQLRowBinaryStreamContentType}) {
		return
	}
	if handler.options.MaintenanceReadOnly {
		handler.auditHTTP(r, AuditEvent{Action: "sql.import", Command: "SQL", OK: false, Status: http.StatusLocked, Message: maintenanceReadOnlyMessage})
		writeJSONStatus(w, http.StatusLocked, commandError(maintenanceReadOnlyMessage))
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, hatSql.SQLRowBinaryStreamContentType) {
		writeJSONStatus(w, http.StatusUnsupportedMediaType, commandError("SQL RowBinary import requires Content-Type "+hatSql.SQLRowBinaryStreamContentType))
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if query == "" {
		writeJSONStatus(w, http.StatusBadRequest, commandError("SQL RowBinary import requires the query URL parameter"))
		return
	}
	if len(query) > maxMonitoringJSONRequestBytes {
		writeJSONStatus(w, http.StatusRequestURITooLong, commandError("SQL RowBinary import query is too large"))
		return
	}
	if !handler.authorizeSQLRowBinaryImport(r) {
		handler.auditHTTP(r, AuditEvent{Action: "sql.import", Command: "SQL", OK: false, Status: http.StatusForbidden, Message: "forbidden by RBAC policy", Details: map[string]interface{}{"query": query}})
		writeJSONStatus(w, http.StatusForbidden, commandError("forbidden"))
		return
	}
	batchSize := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("batch_size")); raw != "" {
		batchSize, err = strconv.Atoi(raw)
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, commandError("batch_size must be an integer"))
			return
		}
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, handler.options.SQLRowBinaryImportMaxBytes)
	result, err := ExecuteSQLRowBinaryInsert(r.Context(), handler.trie, query, r.Body, SQLRowBinaryImportOptions{BatchSize: batchSize})
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "request body too large") {
			status = http.StatusRequestEntityTooLarge
		}
		handler.auditSQLRowBinaryImport(r, query, result, false, status, err.Error())
		writeJSONStatus(w, status, commandError(err.Error()))
		return
	}
	handler.auditSQLRowBinaryImport(r, query, result, true, http.StatusOK, "")
	writeJSON(w, result)
}

func (handler *MonitoringHandler) authorizeSQLRowBinaryImport(r *http.Request) bool {
	return handler.options.RBACPolicy.Authorize(handler.monitoringRequestPrincipal(r), "SQL", "", "")
}

func (handler *MonitoringHandler) auditSQLRowBinaryImport(r *http.Request, query string, result SQLRowBinaryImportResult, ok bool, status int, message string) {
	details := map[string]interface{}{
		"query":    query,
		"affected": result.Affected,
		"batches":  result.Batches,
		"format":   hatSql.SQLRowBinaryStreamContentType,
	}
	handler.auditHTTP(r, AuditEvent{Action: "sql.import", Command: "SQL", OK: ok, Status: status, Message: message, Details: details})
}
