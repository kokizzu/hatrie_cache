package main

import (
	"bufio"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	hatriecache "hatrie_cache"
	"hatrie_cache/hat/hatSql"
)

const defaultSQLHistoryFile = ".hatrie-cache-sql-history.json"

func runSQLREPL(ctx context.Context, client *http.Client, addr string, input io.Reader, stdout io.Writer, stderr io.Writer, historyPath string) error {
	if input == nil {
		return errors.New("SQL REPL input is required")
	}
	stdout = cliWriter(stdout)
	stderr = cliWriter(stderr)
	if historyPath == "" {
		historyPath = defaultSQLHistoryPath()
	}
	history, err := loadSQLHistory(historyPath)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var statement strings.Builder
	for {
		if statement.Len() == 0 {
			_, _ = fmt.Fprint(stdout, "hatrie-sql> ")
		} else {
			_, _ = fmt.Fprint(stdout, "          > ")
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			if strings.TrimSpace(statement.String()) != "" {
				return errors.New("SQL statement is incomplete; terminate it with ; or use \\q")
			}
			return nil
		}
		line := scanner.Text()
		if statement.Len() == 0 && strings.HasPrefix(strings.TrimSpace(line), "\\") {
			quit, err := runSQLREPLMetaCommand(ctx, client, addr, strings.TrimSpace(line), stdout, stderr, history)
			if err != nil {
				return err
			}
			if quit {
				return nil
			}
			continue
		}
		if statement.Len() > 0 {
			statement.WriteByte('\n')
		}
		statement.WriteString(line)
		if !sqlREPLStatementComplete(statement.String()) {
			continue
		}
		query := strings.TrimSpace(statement.String())
		if err := executeSQLREPLQuery(ctx, client, addr, query, stdout); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
		} else {
			history = append(history, query)
			if err := saveSQLHistory(historyPath, history); err != nil {
				return err
			}
		}
		statement.Reset()
	}
}

func runSQLREPLMetaCommand(ctx context.Context, client *http.Client, addr string, command string, stdout io.Writer, stderr io.Writer, history []string) (bool, error) {
	name, argument, _ := strings.Cut(command, " ")
	argument = strings.TrimSpace(argument)
	switch strings.ToLower(name) {
	case "\\q", "\\quit", "\\exit":
		return true, nil
	case "\\history":
		for index, query := range history {
			_, _ = fmt.Fprintf(stdout, "%d\t%s\n", index+1, query)
		}
		return false, nil
	case "\\complete":
		for _, item := range hatSql.NewLanguageServer().Completion(argument, hatSql.Position{Line: 0, Character: len(argument)}) {
			_, _ = fmt.Fprintln(stdout, item.Label)
		}
		return false, nil
	case "\\describe":
		if argument == "" {
			return false, errors.New("\\describe requires a SQL source, for example CACHE('users')")
		}
		if err := executeSQLREPLQuery(ctx, client, addr, "EXPLAIN SELECT * FROM "+argument, stdout); err != nil {
			return false, err
		}
		return false, nil
	case "\\help":
		_, _ = fmt.Fprintln(stdout, "\\q  \\history  \\complete <SQL prefix>  \\describe <SQL source>")
		return false, nil
	default:
		return false, fmt.Errorf("unknown SQL REPL command %q", name)
	}
}

func executeSQLREPLQuery(ctx context.Context, client *http.Client, addr string, source string, stdout io.Writer) error {
	query := sqlREPLTrimTerminator(source)
	if diagnostics := hatSql.LintSQL(query); len(diagnostics) > 0 && diagnostics[0].Severity == hatSql.DiagnosticSeverityError {
		return errors.New(hatSql.FormatDiagnostic(query, hatSql.ValidateSQLQuery(query)))
	}
	body, err := stdjson.Marshal(hatriecache.SQLQueryRequest{Query: query})
	if err != nil {
		return err
	}
	return postJSON(ctx, cliHTTPClient(client), addr, "/api/sql", body, stdout)
}

func sqlREPLStatementComplete(source string) bool {
	quoted := false
	depth := 0
	for index := 0; index < len(source); index++ {
		switch source[index] {
		case '\'':
			if quoted && index+1 < len(source) && source[index+1] == '\'' {
				index++
				continue
			}
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted && depth > 0 {
				depth--
			}
		}
	}
	return !quoted && depth == 0 && strings.HasSuffix(strings.TrimSpace(source), ";")
}

func sqlREPLTrimTerminator(source string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(source), ";"))
}

func defaultSQLHistoryPath() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, defaultSQLHistoryFile)
	}
	return defaultSQLHistoryFile
}

func loadSQLHistory(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read SQL history: %w", err)
	}
	var history []string
	if err := stdjson.Unmarshal(data, &history); err != nil {
		return nil, fmt.Errorf("decode SQL history: %w", err)
	}
	return history, nil
}

func saveSQLHistory(path string, history []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil && filepath.Dir(path) != "." {
		return fmt.Errorf("create SQL history directory: %w", err)
	}
	data, err := stdjson.Marshal(history)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}
