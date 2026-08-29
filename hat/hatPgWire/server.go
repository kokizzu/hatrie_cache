// Package hatPgWire implements the PostgreSQL v3 wire protocol for SQL
// servers that need compatibility with existing PostgreSQL clients.
package hatPgWire

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	protocolVersion3  = 196608
	sslRequestCode    = 80877103
	defaultMaxMessage = 16 << 20

	OIDBool   = 16
	OIDInt2   = 21
	OIDInt8   = 20
	OIDInt4   = 23
	OIDText   = 25
	OIDFloat4 = 700
	OIDFloat8 = 701
)

// Startup is the client information supplied before authentication.
type Startup struct {
	User       string
	Database   string
	Parameters map[string]string
}

// Authenticator validates a PostgreSQL clear-text password exchange. A nil
// authenticator enables trust authentication and must only be used on a
// protected listener such as loopback, mTLS, or a trusted proxy boundary.
type Authenticator func(context.Context, Startup, string) error

// ServerOptions configures one PostgreSQL wire connection.
type ServerOptions struct {
	Authenticator         Authenticator
	MaxMessageBytes       int
	MaxPreparedStatements int
	MaxPortals            int
	MaxPortalResultBytes  int
}

// Field describes a PostgreSQL text-format result column.
type Field struct {
	Name        string
	DataTypeOID uint32
}

// QueryResult is the result of one simple-query request. Nil cells encode as
// SQL NULL; all non-nil cells use PostgreSQL's text transfer format.
type QueryResult struct {
	Fields     []Field
	Rows       [][]*string
	CommandTag string
}

// QueryHandler executes a PostgreSQL simple-query SQL string.
type QueryHandler interface {
	Query(context.Context, string) (QueryResult, error)
}

// ParameterizedQueryHandler executes a PostgreSQL extended query with its
// bound values kept separate from the SQL text. PostgreSQL text parameters are
// represented as strings and SQL NULL is represented as nil.
type ParameterizedQueryHandler interface {
	QueryHandler
	QueryParameters(context.Context, string, []interface{}) (QueryResult, error)
}

// QueryHandlerFunc adapts a function to QueryHandler.
type QueryHandlerFunc func(context.Context, string) (QueryResult, error)

func (handler QueryHandlerFunc) Query(ctx context.Context, query string) (QueryResult, error) {
	return handler(ctx, query)
}

// ServeConn serves one PostgreSQL v3 connection until the client terminates,
// the context is cancelled, or the connection fails. It supports startup,
// optional clear-text password authentication, simple queries, text-format
// prepared queries, and termination.
func ServeConn(ctx context.Context, connection net.Conn, handler QueryHandler, options ServerOptions) error {
	if connection == nil {
		return errors.New("PostgreSQL wire connection is nil")
	}
	if handler == nil {
		return errors.New("PostgreSQL wire query handler is nil")
	}
	maxMessage := options.MaxMessageBytes
	if maxMessage == 0 {
		maxMessage = defaultMaxMessage
	}
	if maxMessage < 8 {
		return fmt.Errorf("PostgreSQL wire max message bytes %d is too small", maxMessage)
	}

	startup, err := readStartup(connection, maxMessage)
	for err == errSSLRequest {
		if err := writeRaw(connection, []byte{'N'}); err != nil {
			return err
		}
		startup, err = readStartup(connection, maxMessage)
	}
	if err != nil {
		return err
	}
	if options.Authenticator != nil {
		if err := writeAuthenticationCleartextPassword(connection); err != nil {
			return err
		}
		messageType, body, err := readFrontendMessage(connection, maxMessage)
		if err != nil {
			return err
		}
		if messageType != 'p' {
			return writeFatalAndReturn(connection, "08P01", "expected password message")
		}
		password, err := requiredCString(body)
		if err != nil {
			return writeFatalAndReturn(connection, "08P01", "invalid password message")
		}
		if err := options.Authenticator(ctx, startup, password); err != nil {
			return writeFatalAndReturn(connection, "28P01", "password authentication failed")
		}
	}
	if err := writeAuthenticationOK(connection); err != nil {
		return err
	}
	if err := writeStartupComplete(connection); err != nil {
		return err
	}

	prepared := make(map[string]preparedStatement)
	portals := make(map[string]portalQuery)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		messageType, body, err := readFrontendMessage(connection, maxMessage)
		if err != nil {
			return err
		}
		switch messageType {
		case 'X':
			return nil
		case 'H':
			// Flush requests buffered output only; protocol responses remain unchanged.
			continue
		case 'Q':
			query, err := requiredCString(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", "invalid query message"); err != nil {
					return err
				}
				continue
			}
			if strings.TrimSpace(query) == "" {
				if err := writeMessage(connection, 'I', nil); err != nil {
					return err
				}
				if err := writeReadyForQuery(connection); err != nil {
					return err
				}
				continue
			}
			if isSessionSetupQuery(query) {
				if err := writeQueryResult(connection, QueryResult{CommandTag: "SET"}); err != nil {
					return err
				}
				if err := writeReadyForQuery(connection); err != nil {
					return err
				}
				continue
			}
			result, queryErr := handler.Query(ctx, query)
			if queryErr == nil {
				queryErr = validateQueryResult(result)
			}
			if queryErr != nil {
				if err := writeErrorAndReady(connection, "XX000", queryErr.Error()); err != nil {
					return err
				}
				continue
			}
			if err := writeQueryResult(connection, result); err != nil {
				return err
			}
			if err := writeReadyForQuery(connection); err != nil {
				return err
			}
		case 'P':
			name, query, parameterTypes, err := parseParseMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", err.Error()); err != nil {
					return err
				}
				continue
			}
			if _, exists := prepared[name]; !exists && options.MaxPreparedStatements > 0 && len(prepared) >= options.MaxPreparedStatements {
				if err := writeErrorAndReady(connection, "54000", "PostgreSQL prepared statement limit exceeded"); err != nil {
					return err
				}
				continue
			}
			prepared[name] = preparedStatement{query: query, parameterTypes: parameterTypes}
			if err := writeMessage(connection, '1', nil); err != nil {
				return err
			}
		case 'B':
			portal, statement, parameters, err := parseBindMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "0A000", err.Error()); err != nil {
					return err
				}
				continue
			}
			preparedQuery, ok := prepared[statement]
			if !ok {
				if err := writeErrorAndReady(connection, "26000", "prepared statement does not exist"); err != nil {
					return err
				}
				continue
			}
			if len(preparedQuery.parameterTypes) != 0 && len(parameters) != len(preparedQuery.parameterTypes) {
				if err := writeErrorAndReady(connection, "08P01", "bind parameter count does not match prepared statement"); err != nil {
					return err
				}
				continue
			}
			parameters, err = decodeTextParameters(parameters, preparedQuery.parameterTypes)
			if err != nil {
				if err := writeErrorAndReady(connection, "22P02", err.Error()); err != nil {
					return err
				}
				continue
			}
			if _, exists := portals[portal]; !exists && options.MaxPortals > 0 && len(portals) >= options.MaxPortals {
				if err := writeErrorAndReady(connection, "54000", "PostgreSQL portal limit exceeded"); err != nil {
					return err
				}
				continue
			}
			portals[portal] = portalQuery{query: preparedQuery.query, parameters: parameters}
			if err := writeMessage(connection, '2', nil); err != nil {
				return err
			}
		case 'D':
			if len(body) < 2 || (body[0] != 'S' && body[0] != 'P') {
				if err := writeErrorAndReady(connection, "08P01", "invalid describe message"); err != nil {
					return err
				}
				continue
			}
			name, err := requiredCString(body[1:])
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", "invalid describe message"); err != nil {
					return err
				}
				continue
			}
			if body[0] == 'S' {
				statement, ok := prepared[name]
				if !ok {
					if err := writeErrorAndReady(connection, "26000", "prepared statement does not exist"); err != nil {
						return err
					}
					continue
				}
				if err := writeParameterDescription(connection, statement.parameterTypes); err != nil {
					return err
				}
				if err := writeMessage(connection, 'n', nil); err != nil {
					return err
				}
			} else {
				boundQuery, ok := portals[name]
				if !ok {
					if err := writeErrorAndReady(connection, "34000", "portal does not exist"); err != nil {
						return err
					}
					continue
				}
				if err := executePortal(ctx, handler, &boundQuery, options.MaxPortalResultBytes); err != nil {
					if err := writeErrorAndReady(connection, pgWireExecutionErrorCode(err), err.Error()); err != nil {
						return err
					}
					continue
				}
				portals[name] = boundQuery
				if len(boundQuery.result.Fields) == 0 {
					if err := writeMessage(connection, 'n', nil); err != nil {
						return err
					}
				} else if err := writeRowDescription(connection, boundQuery.result.Fields); err != nil {
					return err
				}
			}
		case 'E':
			portal, maxRows, err := parseExecuteMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", err.Error()); err != nil {
					return err
				}
				continue
			}
			boundQuery, ok := portals[portal]
			if !ok {
				if err := writeErrorAndReady(connection, "34000", "portal does not exist"); err != nil {
					return err
				}
				continue
			}
			if err := executePortal(ctx, handler, &boundQuery, options.MaxPortalResultBytes); err != nil {
				if err := writeErrorAndReady(connection, pgWireExecutionErrorCode(err), err.Error()); err != nil {
					return err
				}
				continue
			}
			endRow := len(boundQuery.result.Rows)
			if maxRows != 0 && uint64(endRow-boundQuery.nextRow) > uint64(maxRows) {
				endRow = boundQuery.nextRow + int(maxRows)
			}
			complete := endRow == len(boundQuery.result.Rows)
			if err := writeQueryResultRange(connection, boundQuery.result, boundQuery.nextRow, endRow, false, complete); err != nil {
				return err
			}
			boundQuery.nextRow = endRow
			portals[portal] = boundQuery
		case 'C':
			target, name, err := parseCloseMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", err.Error()); err != nil {
					return err
				}
				continue
			}
			switch target {
			case 'S':
				if _, ok := prepared[name]; !ok {
					if err := writeErrorAndReady(connection, "26000", "prepared statement does not exist"); err != nil {
						return err
					}
					continue
				}
				delete(prepared, name)
			case 'P':
				if _, ok := portals[name]; !ok {
					if err := writeErrorAndReady(connection, "34000", "portal does not exist"); err != nil {
						return err
					}
					continue
				}
				delete(portals, name)
			}
			if err := writeMessage(connection, '3', nil); err != nil {
				return err
			}
		case 'S':
			if err := writeReadyForQuery(connection); err != nil {
				return err
			}
		default:
			if err := writeErrorAndReady(connection, "0A000", "PostgreSQL extended protocol is not supported"); err != nil {
				return err
			}
		}
	}
}

func isSessionSetupQuery(query string) bool {
	fields := strings.Fields(query)
	return len(fields) > 0 && strings.EqualFold(fields[0], "SET")
}

type preparedStatement struct {
	query          string
	parameterTypes []uint32
}

type portalQuery struct {
	query      string
	parameters []interface{}
	result     QueryResult
	nextRow    int
	executed   bool
}

var errPortalResultLimit = errors.New("PostgreSQL portal result byte limit exceeded")

func executePortal(ctx context.Context, handler QueryHandler, portal *portalQuery, maxResultBytes int) error {
	if portal.executed {
		return nil
	}
	if isSessionSetupQuery(portal.query) {
		portal.result = QueryResult{CommandTag: "SET"}
		portal.executed = true
		return nil
	}
	var err error
	if parameterizedHandler, ok := handler.(ParameterizedQueryHandler); ok {
		portal.result, err = parameterizedHandler.QueryParameters(ctx, portal.query, portal.parameters)
	} else if len(portal.parameters) != 0 {
		err = errors.New("PostgreSQL bind parameters require a parameterized query handler")
	} else {
		portal.result, err = handler.Query(ctx, portal.query)
	}
	if err != nil {
		return err
	}
	if err := validateQueryResult(portal.result); err != nil {
		return err
	}
	if maxResultBytes > 0 && portalResultBytes(portal.result) > maxResultBytes {
		return errPortalResultLimit
	}
	portal.executed = true
	return nil
}

func pgWireExecutionErrorCode(err error) string {
	if errors.Is(err, errPortalResultLimit) {
		return "54000"
	}
	return "XX000"
}

func portalResultBytes(result QueryResult) int {
	bytes := 0
	for _, field := range result.Fields {
		bytes += len(field.Name)
	}
	for _, row := range result.Rows {
		for _, value := range row {
			if value != nil {
				bytes += len(*value)
			}
		}
	}
	return bytes
}

func parseParseMessage(body []byte) (string, string, []uint32, error) {
	name, rest, ok := splitCString(body)
	if !ok {
		return "", "", nil, errors.New("invalid parse message")
	}
	query, rest, ok := splitCString(rest)
	if !ok || len(rest) < 2 {
		return "", "", nil, errors.New("invalid parse message")
	}
	parameterCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) != parameterCount*4 {
		return "", "", nil, errors.New("invalid parse parameter type list")
	}
	parameterTypes := make([]uint32, parameterCount)
	for index := range parameterTypes {
		parameterTypes[index] = binary.BigEndian.Uint32(rest[index*4:])
	}
	return name, query, parameterTypes, nil
}

func parseBindMessage(body []byte) (string, string, []interface{}, error) {
	portal, rest, ok := splitCString(body)
	if !ok {
		return "", "", nil, errors.New("invalid bind message")
	}
	statement, rest, ok := splitCString(rest)
	if !ok || len(rest) < 2 {
		return "", "", nil, errors.New("invalid bind message")
	}
	formatCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if formatCount > 1 || len(rest) < formatCount*2 {
		return "", "", nil, errors.New("only text bind parameter formats are supported")
	}
	if formatCount == 1 && binary.BigEndian.Uint16(rest[:2]) != 0 {
		return "", "", nil, errors.New("only text bind parameter formats are supported")
	}
	rest = rest[formatCount*2:]
	if len(rest) < 2 {
		return "", "", nil, errors.New("invalid bind parameter count")
	}
	parameterCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	parameters := make([]interface{}, parameterCount)
	for index := range parameters {
		if len(rest) < 4 {
			return "", "", nil, errors.New("invalid bind parameter value")
		}
		length := int(int32(binary.BigEndian.Uint32(rest[:4])))
		rest = rest[4:]
		if length == -1 {
			continue
		}
		if length < 0 || len(rest) < length {
			return "", "", nil, errors.New("invalid bind parameter value")
		}
		parameters[index] = string(rest[:length])
		rest = rest[length:]
	}
	if len(rest) < 2 {
		return "", "", nil, errors.New("invalid bind result format")
	}
	resultFormatCount := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if resultFormatCount > 1 || len(rest) != resultFormatCount*2 {
		return "", "", nil, errors.New("only text bind result formats are supported")
	}
	if resultFormatCount == 1 && binary.BigEndian.Uint16(rest) != 0 {
		return "", "", nil, errors.New("only text bind result formats are supported")
	}
	return portal, statement, parameters, nil
}

func parseExecuteMessage(body []byte) (string, uint32, error) {
	portal, rest, ok := splitCString(body)
	if !ok || len(rest) != 4 {
		return "", 0, errors.New("invalid execute message")
	}
	return portal, binary.BigEndian.Uint32(rest), nil
}

func parseCloseMessage(body []byte) (byte, string, error) {
	if len(body) < 2 || (body[0] != 'S' && body[0] != 'P') {
		return 0, "", errors.New("invalid close message")
	}
	name, err := requiredCString(body[1:])
	if err != nil {
		return 0, "", errors.New("invalid close message")
	}
	return body[0], name, nil
}

func decodeTextParameters(parameters []interface{}, parameterTypes []uint32) ([]interface{}, error) {
	decoded := append([]interface{}(nil), parameters...)
	for index, parameter := range decoded {
		if parameter == nil {
			continue
		}
		value, ok := parameter.(string)
		if !ok {
			return nil, errors.New("invalid text bind parameter")
		}
		parameterType := uint32(0)
		if index < len(parameterTypes) {
			parameterType = parameterTypes[index]
		}
		switch parameterType {
		case OIDBool:
			switch strings.ToLower(value) {
			case "1", "t", "true":
				decoded[index] = true
			case "0", "f", "false":
				decoded[index] = false
			default:
				return nil, fmt.Errorf("invalid PostgreSQL boolean parameter %q", value)
			}
		case OIDInt2:
			parsed, err := strconv.ParseInt(value, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid PostgreSQL int2 parameter %q", value)
			}
			decoded[index] = parsed
		case OIDInt4:
			parsed, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid PostgreSQL int4 parameter %q", value)
			}
			decoded[index] = parsed
		case OIDInt8:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid PostgreSQL int8 parameter %q", value)
			}
			decoded[index] = parsed
		case OIDFloat4:
			parsed, err := strconv.ParseFloat(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid PostgreSQL float4 parameter %q", value)
			}
			decoded[index] = parsed
		case OIDFloat8:
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid PostgreSQL float8 parameter %q", value)
			}
			decoded[index] = parsed
		}
	}
	return decoded, nil
}

func writeParameterDescription(connection net.Conn, parameterTypes []uint32) error {
	body := make([]byte, 2+len(parameterTypes)*4)
	binary.BigEndian.PutUint16(body[:2], uint16(len(parameterTypes)))
	for index, parameterType := range parameterTypes {
		binary.BigEndian.PutUint32(body[2+index*4:], parameterType)
	}
	return writeMessage(connection, 't', body)
}

var errSSLRequest = errors.New("PostgreSQL SSL request")

func readStartup(connection net.Conn, maxMessage int) (Startup, error) {
	length, err := readLength(connection)
	if err != nil {
		return Startup{}, err
	}
	if length < 8 || length > maxMessage {
		return Startup{}, fmt.Errorf("invalid PostgreSQL startup length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(connection, body); err != nil {
		return Startup{}, err
	}
	code := binary.BigEndian.Uint32(body[:4])
	if code == sslRequestCode && len(body) == 4 {
		return Startup{}, errSSLRequest
	}
	if code != protocolVersion3 {
		return Startup{}, fmt.Errorf("unsupported PostgreSQL protocol version %d", code)
	}
	parameters, err := parseStartupParameters(body[4:])
	if err != nil {
		return Startup{}, err
	}
	return Startup{User: parameters["user"], Database: parameters["database"], Parameters: parameters}, nil
}

func parseStartupParameters(data []byte) (map[string]string, error) {
	parameters := make(map[string]string)
	for len(data) > 0 {
		key, rest, ok := splitCString(data)
		if !ok {
			return nil, errors.New("invalid PostgreSQL startup parameter")
		}
		if key == "" {
			if len(rest) != 0 {
				return nil, errors.New("trailing PostgreSQL startup bytes")
			}
			return parameters, nil
		}
		value, remaining, ok := splitCString(rest)
		if !ok {
			return nil, errors.New("invalid PostgreSQL startup value")
		}
		parameters[key] = value
		data = remaining
	}
	return nil, errors.New("PostgreSQL startup terminator is missing")
}

func readFrontendMessage(connection net.Conn, maxMessage int) (byte, []byte, error) {
	header := [5]byte{}
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:]))
	if length < 4 || length > maxMessage {
		return 0, nil, fmt.Errorf("invalid PostgreSQL message length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(connection, body); err != nil {
		return 0, nil, err
	}
	return header[0], body, nil
}

func readLength(connection net.Conn) (int, error) {
	buffer := [4]byte{}
	if _, err := io.ReadFull(connection, buffer[:]); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint32(buffer[:])), nil
}

func writeStartupComplete(connection net.Conn) error {
	for key, value := range map[string]string{
		"client_encoding":             "UTF8",
		"server_version":              "16.0",
		"standard_conforming_strings": "on",
	} {
		if err := writeParameterStatus(connection, key, value); err != nil {
			return err
		}
	}
	keyData := make([]byte, 8)
	_, _ = rand.Read(keyData)
	if err := writeMessage(connection, 'K', keyData); err != nil {
		return err
	}
	return writeReadyForQuery(connection)
}

func writeAuthenticationOK(connection net.Conn) error {
	body := make([]byte, 4)
	return writeMessage(connection, 'R', body)
}

func writeAuthenticationCleartextPassword(connection net.Conn) error {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 3)
	return writeMessage(connection, 'R', body)
}

func writeParameterStatus(connection net.Conn, key string, value string) error {
	return writeMessage(connection, 'S', appendCString(appendCString(nil, key), value))
}

func writeReadyForQuery(connection net.Conn) error {
	return writeMessage(connection, 'Z', []byte{'I'})
}

func writeQueryResult(connection net.Conn, result QueryResult) error {
	return writeQueryResultRange(connection, result, 0, len(result.Rows), true, true)
}

func writeQueryResultRange(connection net.Conn, result QueryResult, startRow int, endRow int, includeRowDescription bool, complete bool) error {
	if startRow < 0 || endRow < startRow || endRow > len(result.Rows) {
		return errors.New("invalid PostgreSQL result row range")
	}
	if includeRowDescription && len(result.Fields) > 0 {
		if err := writeRowDescription(connection, result.Fields); err != nil {
			return err
		}
	}
	for _, row := range result.Rows[startRow:endRow] {
		if err := writeDataRow(connection, row); err != nil {
			return err
		}
	}
	if !complete {
		return writeMessage(connection, 's', nil)
	}
	tag := result.CommandTag
	if tag == "" {
		tag = fmt.Sprintf("SELECT %d", len(result.Rows))
	}
	return writeMessage(connection, 'C', appendCString(nil, tag))
}

func validateQueryResult(result QueryResult) error {
	if len(result.Fields) == 0 && len(result.Rows) != 0 {
		return errors.New("PostgreSQL result rows require fields")
	}
	for index, field := range result.Fields {
		if strings.TrimSpace(field.Name) == "" || strings.IndexByte(field.Name, 0) >= 0 {
			return fmt.Errorf("PostgreSQL result field %d has invalid name", index+1)
		}
	}
	for index, row := range result.Rows {
		if len(row) != len(result.Fields) {
			return fmt.Errorf("PostgreSQL result row %d has %d cells, want %d", index+1, len(row), len(result.Fields))
		}
	}
	return nil
}

func writeRowDescription(connection net.Conn, fields []Field) error {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, uint16(len(fields)))
	for _, field := range fields {
		body = appendCString(body, field.Name)
		body = appendUint32(body, 0)
		body = appendUint16(body, 0)
		dataTypeOID := field.DataTypeOID
		if dataTypeOID == 0 {
			dataTypeOID = OIDText
		}
		body = appendUint32(body, dataTypeOID)
		body = appendUint16(body, 0xffff)
		body = appendUint32(body, 0xffffffff)
		body = appendUint16(body, 0)
	}
	return writeMessage(connection, 'T', body)
}

func writeDataRow(connection net.Conn, row []*string) error {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, uint16(len(row)))
	for _, value := range row {
		if value == nil {
			body = appendUint32(body, 0xffffffff)
			continue
		}
		body = appendUint32(body, uint32(len(*value)))
		body = append(body, (*value)...)
	}
	return writeMessage(connection, 'D', body)
}

func writeErrorAndReady(connection net.Conn, code string, message string) error {
	if err := writeError(connection, 'E', code, message); err != nil {
		return err
	}
	return writeReadyForQuery(connection)
}

func writeFatal(connection net.Conn, code string, message string) error {
	return writeError(connection, 'E', code, message)
}

func writeFatalAndReturn(connection net.Conn, code string, message string) error {
	if err := writeFatal(connection, code, message); err != nil {
		return err
	}
	return errors.New(message)
}

func writeError(connection net.Conn, messageType byte, code string, message string) error {
	body := []byte{'S'}
	body = appendCString(body, "ERROR")
	body = append(body, 'C')
	body = appendCString(body, code)
	body = append(body, 'M')
	body = appendCString(body, strings.ReplaceAll(message, "\x00", ""))
	body = append(body, 0)
	return writeMessage(connection, messageType, body)
}

func writeMessage(connection net.Conn, messageType byte, body []byte) error {
	packet := make([]byte, 5+len(body))
	packet[0] = messageType
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	return writeRaw(connection, packet)
}

func writeRaw(connection net.Conn, data []byte) error {
	for len(data) > 0 {
		count, err := connection.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func requiredCString(data []byte) (string, error) {
	value, rest, ok := splitCString(data)
	if !ok || len(rest) != 0 {
		return "", errors.New("invalid PostgreSQL cstring")
	}
	return value, nil
}

func splitCString(data []byte) (string, []byte, bool) {
	for index, value := range data {
		if value == 0 {
			return string(data[:index]), data[index+1:], true
		}
	}
	return "", nil, false
}

func appendCString(data []byte, value string) []byte {
	data = append(data, value...)
	return append(data, 0)
}

func appendUint16(data []byte, value uint16) []byte {
	return binary.BigEndian.AppendUint16(data, value)
}

func appendUint32(data []byte, value uint32) []byte {
	return binary.BigEndian.AppendUint32(data, value)
}
