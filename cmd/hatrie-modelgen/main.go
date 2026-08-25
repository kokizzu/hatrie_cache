// Command hatrie-modelgen generates typed Go models from a hatSchema JSON file.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"hatrie_cache/hat/hatSchema"

	json "github.com/goccy/go-json"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("hatrie-modelgen", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	schemaPath := flags.String("schema", "", "schema JSON path")
	packageName := flags.String("package", "", "generated Go package")
	outPath := flags.String("out", "", "generated Go file path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *schemaPath == "" || *packageName == "" || *outPath == "" {
		return errors.New("usage: hatrie-modelgen -schema schema.json -package models -out models_gen.go")
	}
	data, err := os.ReadFile(*schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	var schema hatSchema.Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}
	generated, err := hatSchema.GenerateGoModels(schema, hatSchema.ModelOptions{Package: *packageName})
	if err != nil {
		return err
	}
	if *outPath == "-" {
		_, err := output.Write(generated)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(*outPath, generated, 0o644); err != nil {
		return fmt.Errorf("write generated model: %w", err)
	}
	return nil
}
