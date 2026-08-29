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
	"strings"
)

const (
	protocolVersion3  = 196608
	sslRequestCode    = 80877103
	defaultMaxMessage = 16 << 20

	OIDBool   = 16
	OIDInt8   = 20
	OIDText   = 25
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
	Authenticator   Authenticator
	MaxMessageBytes int
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

// QueryHandlerFunc adapts a function to QueryHandler.
type QueryHandlerFunc func(context.Context, string) (QueryResult, error)

func (handler QueryHandlerFunc) Query(ctx context.Context, query string) (QueryResult, error) {
	return handler(ctx, query)
}

// ServeConn serves one PostgreSQL v3 connection until the client terminates,
// the context is cancelled, or the connection fails. It supports startup,
// optional clear-text password authentication, simple queries, and termination.
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

	prepared := make(map[string]string)
	portals := make(map[string]string)
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
			name, query, err := parseParseMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", err.Error()); err != nil {
					return err
				}
				continue
			}
			prepared[name] = query
			if err := writeMessage(connection, '1', nil); err != nil {
				return err
			}
		case 'B':
			portal, statement, err := parseBindMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "0A000", err.Error()); err != nil {
					return err
				}
				continue
			}
			query, ok := prepared[statement]
			if !ok {
				if err := writeErrorAndReady(connection, "26000", "prepared statement does not exist"); err != nil {
					return err
				}
				continue
			}
			portals[portal] = query
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
			if err := writeMessage(connection, 't', []byte{0, 0}); err != nil {
				return err
			}
			if err := writeMessage(connection, 'n', nil); err != nil {
				return err
			}
		case 'E':
			portal, err := parseExecuteMessage(body)
			if err != nil {
				if err := writeErrorAndReady(connection, "08P01", err.Error()); err != nil {
					return err
				}
				continue
			}
			query, ok := portals[portal]
			if !ok {
				if err := writeErrorAndReady(connection, "34000", "portal does not exist"); err != nil {
					return err
				}
				continue
			}
			result, err := handler.Query(ctx, query)
			if err == nil {
				err = validateQueryResult(result)
			}
			if err != nil {
				if err := writeErrorAndReady(connection, "XX000", err.Error()); err != nil {
					return err
				}
				continue
			}
			if err := writeQueryResult(connection, result); err != nil {
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

func parseParseMessage(body []byte) (string, string, error) {
	name, rest, ok := splitCString(body)
	if !ok {
		return "", "", errors.New("invalid parse message")
	}
	query, rest, ok := splitCString(rest)
	if !ok || len(rest) < 2 || binary.BigEndian.Uint16(rest[:2]) != 0 {
		return "", "", errors.New("only zero-parameter parse is supported")
	}
	return name, query, nil
}

func parseBindMessage(body []byte) (string, string, error) {
	portal, rest, ok := splitCString(body)
	if !ok {
		return "", "", errors.New("invalid bind message")
	}
	statement, rest, ok := splitCString(rest)
	if !ok || len(rest) < 6 || binary.BigEndian.Uint16(rest[:2]) != 0 || binary.BigEndian.Uint16(rest[2:4]) != 0 || binary.BigEndian.Uint16(rest[4:6]) != 0 {
		return "", "", errors.New("only zero-parameter text bind is supported")
	}
	return portal, statement, nil
}

func parseExecuteMessage(body []byte) (string, error) {
	portal, rest, ok := splitCString(body)
	if !ok || len(rest) != 4 || binary.BigEndian.Uint32(rest) != 0 {
		return "", errors.New("only unlimited execute is supported")
	}
	return portal, nil
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
	if len(result.Fields) > 0 {
		if err := writeRowDescription(connection, result.Fields); err != nil {
			return err
		}
		for _, row := range result.Rows {
			if err := writeDataRow(connection, row); err != nil {
				return err
			}
		}
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
