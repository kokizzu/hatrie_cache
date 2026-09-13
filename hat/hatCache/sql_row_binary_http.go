package hatCache

import (
	"mime"
	"net/http"
	"strings"

	"hatrie_cache/hat/hatSql"
)

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
