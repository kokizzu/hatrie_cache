package hatCache

import (
	"encoding/json"
	"net/http"
)

// handleSQLFunctions owns SQL UDF registration independently from query
// execution so the two HTTP surfaces can evolve and be reviewed separately.
func (handler *MonitoringHandler) handleSQLFunctions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) || !handler.requireTrie(w) {
		return
	}
	defer r.Body.Close()
	var definition SQLFunctionDefinition
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxMonitoringJSONRequestBytes))
	if err := decoder.Decode(&definition); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError("invalid SQL function request: "+err.Error()))
		return
	}
	if err := handler.sqlFunctions.Register(definition); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(FormatSQLFunctionDiagnostic(definition, err)))
		return
	}
	writeJSON(w, definition)
}
