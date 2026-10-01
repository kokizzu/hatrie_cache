package hatAuth

import "strings"

// RoleCatalogStoredFunctionAuthorizer adapts the bounded role catalog to the
// stored-function registry. Function grants are represented as object
// selectors using the stable "function:<name>" namespace. A nil catalog and
// unknown operations fail closed.
func RoleCatalogStoredFunctionAuthorizer(catalog *RoleCatalog) StoredFunctionAuthorizer {
	return func(principal, operation, function string) bool {
		if catalog == nil {
			return false
		}
		principal = strings.TrimSpace(principal)
		function = strings.TrimSpace(function)
		operation = strings.ToUpper(strings.TrimSpace(operation))
		if principal == "" || function == "" {
			return false
		}
		if operation != StoredFunctionOperationRegister && operation != StoredFunctionOperationCall {
			return false
		}
		return catalog.Authorize(principal, AuthorizationRequest{
			Command: operation,
			Object:  "function:" + function,
		})
	}
}
