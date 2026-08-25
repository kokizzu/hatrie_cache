// hatrie-sqlbench runs deterministic SQL execution scenarios and emits a
// portable report for performance regression review.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"hatrie_cache/hat/hatSql"
)

type report struct {
	Schema     string           `json:"schema"`
	CreatedAt  string           `json:"created_at"`
	Rows       int              `json:"rows"`
	Iterations int              `json:"iterations"`
	Results    []scenarioResult `json:"results"`
}

type scenarioResult struct {
	Name           string   `json:"name"`
	Rows           int      `json:"rows"`
	Iterations     int      `json:"iterations"`
	ElapsedNanos   int64    `json:"elapsed_nanos"`
	RowsPerSecond  float64  `json:"rows_per_second"`
	BytesAllocated uint64   `json:"bytes_allocated"`
	Allocations    uint64   `json:"allocations"`
	BytesPerRun    float64  `json:"bytes_per_run"`
	AllocsPerRun   float64  `json:"allocations_per_run"`
	SpillBytes     int64    `json:"spill_bytes"`
	PlanSignature  string   `json:"plan_signature"`
	Plan           []string `json:"plan"`
}

type scenario struct {
	name    string
	query   string
	options hatSql.QueryOptions
}

func main() {
	rows := flag.Int("rows", 1000, "number of VALUES rows per scenario")
	iterations := flag.Int("iterations", 5, "measured executions per scenario")
	out := flag.String("out", "", "write JSON report to this path instead of stdout")
	flag.Parse()
	if *rows < 2 || *iterations < 1 {
		fmt.Fprintln(os.Stderr, "hatrie-sqlbench: -rows must be at least 2 and -iterations must be positive")
		os.Exit(2)
	}
	report, err := run(context.Background(), *rows, *iterations)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hatrie-sqlbench: %v\n", err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "hatrie-sqlbench: encode report: %v\n", err)
		os.Exit(1)
	}
	encoded = append(encoded, '\n')
	if *out == "" {
		_, _ = os.Stdout.Write(encoded)
		return
	}
	if err := os.WriteFile(*out, encoded, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "hatrie-sqlbench: write report: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rows, iterations int) (report, error) {
	directory, err := os.MkdirTemp("", "hatrie-sqlbench-*")
	if err != nil {
		return report{}, fmt.Errorf("create spill directory: %w", err)
	}
	defer os.RemoveAll(directory)
	values := sqlValues(rows)
	scenarios := []scenario{
		{name: "scan", query: values + " WHERE id >= 0 SELECT id"},
		{name: "external_sort", query: values + " SELECT id ORDER BY id", options: hatSql.QueryOptions{MaxSortBytes: 1, SpillDirectory: directory, MaxSpillBytes: 64 << 20}},
		{name: "external_group", query: values + " GROUP BY bucket SELECT bucket, COUNT(*) AS total ORDER BY bucket", options: hatSql.QueryOptions{MaxGroupBytes: 1, SpillDirectory: directory, MaxSpillBytes: 64 << 20}},
	}
	output := report{Schema: "hatrie-cache-sql-benchmark/v1", CreatedAt: time.Now().UTC().Format(time.RFC3339), Rows: rows, Iterations: iterations}
	for _, scenario := range scenarios {
		measured, err := runScenario(ctx, scenario, iterations)
		if err != nil {
			return report{}, fmt.Errorf("%s: %w", scenario.name, err)
		}
		output.Results = append(output.Results, measured)
	}
	return output, nil
}

func sqlValues(rows int) string {
	var builder strings.Builder
	builder.Grow(rows * 12)
	builder.WriteString("FROM VALUES ")
	for index := 0; index < rows; index++ {
		if index > 0 {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "(%d, %d)", rows-index, index%16)
	}
	builder.WriteString(" AS values(id, bucket)")
	return builder.String()
}

func runScenario(ctx context.Context, scenario scenario, iterations int) (scenarioResult, error) {
	analysis, err := hatSql.ExecuteSQLQueryContext(ctx, "EXPLAIN ANALYZE "+scenario.query, nil, scenario.options)
	if err != nil {
		return scenarioResult{}, err
	}
	plan, spillBytes := summarizePlan(analysis.Plan)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	outputRows := 0
	for iteration := 0; iteration < iterations; iteration++ {
		result, err := hatSql.ExecuteSQLQueryContext(ctx, scenario.query, nil, scenario.options)
		if err != nil {
			return scenarioResult{}, err
		}
		outputRows += len(result.Rows)
	}
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	result := scenarioResult{
		Name:           scenario.name,
		Rows:           outputRows,
		Iterations:     iterations,
		ElapsedNanos:   elapsed.Nanoseconds(),
		BytesAllocated: after.TotalAlloc - before.TotalAlloc,
		Allocations:    after.Mallocs - before.Mallocs,
		SpillBytes:     spillBytes,
		Plan:           plan,
		PlanSignature:  strings.Join(plan, " -> "),
	}
	if elapsed > 0 {
		result.RowsPerSecond = float64(outputRows) / elapsed.Seconds()
	}
	result.BytesPerRun = float64(result.BytesAllocated) / float64(iterations)
	result.AllocsPerRun = float64(result.Allocations) / float64(iterations)
	return result, nil
}

func summarizePlan(steps []hatSql.ExplainStep) ([]string, int64) {
	plan := make([]string, 0, len(steps))
	var spillBytes int64
	for _, step := range steps {
		if step.Node == "" {
			continue
		}
		plan = append(plan, step.Node+": "+step.Detail)
		for _, field := range strings.Fields(step.Detail) {
			if !strings.HasPrefix(field, "spill_bytes=") {
				continue
			}
			value, err := strconv.ParseInt(strings.TrimPrefix(field, "spill_bytes="), 10, 64)
			if err == nil {
				spillBytes += value
			}
		}
	}
	return plan, spillBytes
}
