// hatrie-sql-lsp serves parser-backed SQL editor features over the Language
// Server Protocol's stdio transport.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"hatrie_cache/hat/hatSql"
)

const (
	lspMaxHeaderSize  = 8 << 10
	lspMaxMessageSize = 8 << 20
)

type lspServer struct {
	tooling   *hatSql.LanguageServer
	documents map[string]string
	output    *bufio.Writer
}

type lspRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type lspPosition struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

type lspRange struct {
	Start lspPosition `json:"start"`
	End   lspPosition `json:"end"`
}

type lspDiagnostic struct {
	Range    lspRange `json:"range"`
	Severity int      `json:"severity"`
	Code     string   `json:"code,omitempty"`
	Source   string   `json:"source"`
	Message  string   `json:"message"`
}

func main() {
	if err := newLSPServer(os.Stdout).serve(os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, "hatrie-sql-lsp:", err)
		os.Exit(1)
	}
}

func newLSPServer(output io.Writer) *lspServer {
	return &lspServer{
		tooling:   hatSql.NewLanguageServer(),
		documents: make(map[string]string),
		output:    bufio.NewWriter(output),
	}
}

func (server *lspServer) serve(input io.Reader) error {
	reader := bufio.NewReaderSize(input, lspMaxHeaderSize)
	for {
		payload, err := readLSPFrame(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var request lspRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			if writeErr := server.writeError(json.RawMessage("null"), -32700, "parse error"); writeErr != nil {
				return writeErr
			}
			continue
		}
		if request.JSONRPC != "2.0" || request.Method == "" {
			if err := server.writeError(request.responseID(), -32600, "invalid request"); err != nil {
				return err
			}
			continue
		}
		if err := server.handle(request); err != nil {
			return err
		}
		if request.Method == "exit" {
			return nil
		}
	}
}

func (server *lspServer) handle(request lspRequest) error {
	switch request.Method {
	case "initialize":
		return server.writeResult(request.ID, map[string]any{
			"capabilities": map[string]any{
				"textDocumentSync":   1,
				"completionProvider": map[string]bool{"resolveProvider": false},
				"diagnosticProvider": map[string]bool{"interFileDependencies": false, "workspaceDiagnostics": false},
			},
		})
	case "initialized":
		return nil
	case "shutdown":
		return server.writeResult(request.ID, nil)
	case "exit":
		return nil
	case "textDocument/didOpen":
		var params struct {
			TextDocument struct {
				URI  string `json:"uri"`
				Text string `json:"text"`
			} `json:"textDocument"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.TextDocument.URI == "" {
			return server.invalidParams(request)
		}
		server.documents[params.TextDocument.URI] = params.TextDocument.Text
		return server.publishDiagnostics(params.TextDocument.URI)
	case "textDocument/didChange":
		var params struct {
			TextDocument struct {
				URI string `json:"uri"`
			} `json:"textDocument"`
			ContentChanges []struct {
				Text string `json:"text"`
			} `json:"contentChanges"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.TextDocument.URI == "" || len(params.ContentChanges) == 0 {
			return server.invalidParams(request)
		}
		server.documents[params.TextDocument.URI] = params.ContentChanges[len(params.ContentChanges)-1].Text
		return server.publishDiagnostics(params.TextDocument.URI)
	case "textDocument/didClose":
		var params struct {
			TextDocument struct {
				URI string `json:"uri"`
			} `json:"textDocument"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.TextDocument.URI == "" {
			return server.invalidParams(request)
		}
		delete(server.documents, params.TextDocument.URI)
		return server.writeNotification("textDocument/publishDiagnostics", map[string]any{"uri": params.TextDocument.URI, "diagnostics": []lspDiagnostic{}})
	case "textDocument/completion":
		var params struct {
			TextDocument struct {
				URI string `json:"uri"`
			} `json:"textDocument"`
			Position lspPosition `json:"position"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.TextDocument.URI == "" {
			return server.invalidParams(request)
		}
		source, ok := server.documents[params.TextDocument.URI]
		if !ok {
			return server.invalidParams(request)
		}
		items := server.tooling.Completion(source, hatSql.Position{
			Line:      params.Position.Line,
			Character: lspByteCharacter(source, params.Position),
		})
		return server.writeResult(request.ID, map[string]any{"isIncomplete": false, "items": lspCompletionItems(items)})
	case "textDocument/diagnostic":
		var params struct {
			TextDocument struct {
				URI string `json:"uri"`
			} `json:"textDocument"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.TextDocument.URI == "" {
			return server.invalidParams(request)
		}
		source, ok := server.documents[params.TextDocument.URI]
		if !ok {
			return server.invalidParams(request)
		}
		return server.writeResult(request.ID, map[string]any{"kind": "full", "items": server.lspDiagnostics(source)})
	default:
		if len(request.ID) == 0 {
			return nil
		}
		return server.writeError(request.ID, -32601, "method not found")
	}
}

func (request lspRequest) responseID() json.RawMessage {
	if len(request.ID) == 0 {
		return json.RawMessage("null")
	}
	return request.ID
}

func (server *lspServer) invalidParams(request lspRequest) error {
	if len(request.ID) == 0 {
		return nil
	}
	return server.writeError(request.ID, -32602, "invalid params")
}

func (server *lspServer) publishDiagnostics(uri string) error {
	source := server.documents[uri]
	return server.writeNotification("textDocument/publishDiagnostics", map[string]any{
		"uri":         uri,
		"diagnostics": server.lspDiagnostics(source),
	})
}

func (server *lspServer) lspDiagnostics(source string) []lspDiagnostic {
	diagnostics := server.tooling.Diagnostics(source)
	result := make([]lspDiagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		severity := 1
		if diagnostic.Severity == hatSql.DiagnosticSeverityWarning {
			severity = 2
		}
		result = append(result, lspDiagnostic{
			Range:    lspRangeFromSQLRange(source, diagnostic.Range),
			Severity: severity,
			Code:     string(diagnostic.Code),
			Source:   "hatrie-sql",
			Message:  diagnostic.Message,
		})
	}
	return result
}

func lspCompletionItems(items []hatSql.CompletionItem) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{"label": item.Label, "kind": 14, "detail": item.Detail})
	}
	return result
}

func (server *lspServer) writeResult(id json.RawMessage, result any) error {
	if len(id) == 0 {
		return nil
	}
	return server.writeMessage(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "result": result})
}

func (server *lspServer) writeError(id json.RawMessage, code int, message string) error {
	return server.writeMessage(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(id),
		"error":   map[string]any{"code": code, "message": message},
	})
}

func (server *lspServer) writeNotification(method string, params any) error {
	return server.writeMessage(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (server *lspServer) writeMessage(message any) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if _, err := server.output.WriteString("Content-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n"); err != nil {
		return err
	}
	if _, err := server.output.Write(payload); err != nil {
		return err
	}
	return server.output.Flush()
}

func readLSPFrame(reader *bufio.Reader) ([]byte, error) {
	length := -1
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 && length == -1 {
				return nil, io.EOF
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				return nil, errors.New("LSP header exceeds maximum size")
			}
			return nil, io.ErrUnexpectedEOF
		}
		line = []byte(strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r"))
		if len(line) == 0 {
			break
		}
		name, value, ok := strings.Cut(string(line), ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "Content-Length") || length >= 0 {
			return nil, errors.New("invalid LSP headers")
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || parsed < 0 || parsed > lspMaxMessageSize {
			return nil, errors.New("invalid LSP content length")
		}
		length = parsed
	}
	if length < 0 {
		return nil, errors.New("missing LSP content length")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func lspByteCharacter(source string, position lspPosition) int {
	line := lspLine(source, position.Line)
	if line == "" || position.Character <= 0 {
		return 0
	}
	units := 0
	for offset, runeValue := range line {
		if units >= position.Character {
			return offset
		}
		units++
		if runeValue > 0xFFFF {
			units++
		}
		if units >= position.Character {
			return offset + utf8.RuneLen(runeValue)
		}
	}
	return len(line)
}

func lspRangeFromSQLRange(source string, sourceRange hatSql.Range) lspRange {
	return lspRange{
		Start: lspPosition{Line: sourceRange.Start.Line, Character: lspUTF16Character(source, sourceRange.Start)},
		End:   lspPosition{Line: sourceRange.End.Line, Character: lspUTF16Character(source, sourceRange.End)},
	}
}

func lspUTF16Character(source string, position hatSql.Position) int {
	line := lspLine(source, position.Line)
	column := position.Character
	if column < 0 {
		return 0
	}
	if column > len(line) {
		column = len(line)
	}
	units := 0
	for len(line) > 0 && column > 0 {
		runeValue, size := utf8.DecodeRuneInString(line)
		if size > column {
			break
		}
		units++
		if runeValue > 0xFFFF {
			units++
		}
		line = line[size:]
		column -= size
	}
	return units
}

func lspLine(source string, lineNumber int) string {
	if lineNumber < 0 {
		return ""
	}
	lines := strings.Split(source, "\n")
	if lineNumber >= len(lines) {
		return ""
	}
	return lines[lineNumber]
}
