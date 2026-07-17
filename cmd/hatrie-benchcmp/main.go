package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type benchmarkRun struct {
	Schema    string            `json:"schema"`
	RunID     string            `json:"run_id,omitempty"`
	CreatedAt string            `json:"created_at,omitempty"`
	Benchtime string            `json:"benchtime,omitempty"`
	Count     string            `json:"count,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Results   []benchmarkResult `json:"results"`
}

type benchmarkResult struct {
	Section  string  `json:"section"`
	Name     string  `json:"name"`
	NsOp     float64 `json:"ns_op"`
	BOp      float64 `json:"b_op"`
	AllocsOp float64 `json:"allocs_op"`
}

type compareOptions struct {
	MaxRegressionPct float64
	CompareMemory    bool
}

type compareFinding struct {
	Section  string
	Name     string
	Metric   string
	Baseline float64
	Current  float64
	Limit    float64
}

func main() {
	currentPath := flag.String("current", "", "current benchmark JSON artifact")
	baselinePath := flag.String("baseline", "", "baseline benchmark JSON artifact")
	maxRegressionPct := flag.Float64("max-regression-pct", 20, "maximum allowed regression percentage")
	compareMemory := flag.Bool("compare-memory", false, "also compare B/op and allocs/op")
	flag.Parse()

	if strings.TrimSpace(*currentPath) == "" || strings.TrimSpace(*baselinePath) == "" {
		fmt.Fprintln(os.Stderr, "hatrie-benchcmp: -current and -baseline are required")
		os.Exit(2)
	}
	current, err := readBenchmarkRun(*currentPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hatrie-benchcmp: read current: %v\n", err)
		os.Exit(2)
	}
	baseline, err := readBenchmarkRun(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hatrie-benchcmp: read baseline: %v\n", err)
		os.Exit(2)
	}
	findings, compared := compareRuns(current, baseline, compareOptions{
		MaxRegressionPct: *maxRegressionPct,
		CompareMemory:    *compareMemory,
	})
	if compared == 0 {
		fmt.Fprintln(os.Stderr, "hatrie-benchcmp: no matching benchmark rows between current and baseline")
		os.Exit(1)
	}
	writeComparisonSummary(os.Stdout, current, baseline, findings, compared, *maxRegressionPct, *compareMemory)
	if len(findings) > 0 {
		os.Exit(1)
	}
}

func readBenchmarkRun(path string) (benchmarkRun, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchmarkRun{}, err
	}
	var run benchmarkRun
	if err := json.Unmarshal(data, &run); err != nil {
		return benchmarkRun{}, err
	}
	return run, nil
}

func compareRuns(current benchmarkRun, baseline benchmarkRun, options compareOptions) ([]compareFinding, int) {
	baselineByKey := make(map[string]benchmarkResult, len(baseline.Results))
	for _, result := range baseline.Results {
		baselineByKey[benchmarkKey(result)] = result
	}
	var findings []compareFinding
	compared := 0
	for _, currentResult := range current.Results {
		baselineResult, ok := baselineByKey[benchmarkKey(currentResult)]
		if !ok {
			continue
		}
		compared++
		findings = append(findings, compareMetric(currentResult, baselineResult, "ns/op", currentResult.NsOp, baselineResult.NsOp, options.MaxRegressionPct)...)
		if options.CompareMemory {
			findings = append(findings, compareMetric(currentResult, baselineResult, "B/op", currentResult.BOp, baselineResult.BOp, options.MaxRegressionPct)...)
			findings = append(findings, compareMetric(currentResult, baselineResult, "allocs/op", currentResult.AllocsOp, baselineResult.AllocsOp, options.MaxRegressionPct)...)
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Section != findings[j].Section {
			return findings[i].Section < findings[j].Section
		}
		if findings[i].Name != findings[j].Name {
			return findings[i].Name < findings[j].Name
		}
		return findings[i].Metric < findings[j].Metric
	})
	return findings, compared
}

func benchmarkKey(result benchmarkResult) string {
	return result.Section + "\x00" + result.Name
}

func compareMetric(current benchmarkResult, baseline benchmarkResult, metric string, currentValue float64, baselineValue float64, maxRegressionPct float64) []compareFinding {
	if baselineValue <= 0 || currentValue <= 0 {
		return nil
	}
	limit := baselineValue * (1 + maxRegressionPct/100)
	if currentValue <= limit {
		return nil
	}
	return []compareFinding{{
		Section:  current.Section,
		Name:     current.Name,
		Metric:   metric,
		Baseline: baselineValue,
		Current:  currentValue,
		Limit:    limit,
	}}
}

func writeComparisonSummary(out interface{ Write([]byte) (int, error) }, current benchmarkRun, baseline benchmarkRun, findings []compareFinding, compared int, maxRegressionPct float64, compareMemory bool) {
	memoryMode := "cpu only"
	if compareMemory {
		memoryMode = "cpu and memory"
	}
	fmt.Fprintf(out, "Benchmark comparison: current=%s baseline=%s rows=%d max_regression=%.2f%% mode=%s\n", current.RunID, baseline.RunID, compared, maxRegressionPct, memoryMode)
	if len(findings) == 0 {
		fmt.Fprintln(out, "Benchmark comparison passed")
		return
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "| Section | Benchmark | Metric | Baseline | Current | Limit |")
	fmt.Fprintln(out, "| --- | --- | --- | ---: | ---: | ---: |")
	for _, finding := range findings {
		fmt.Fprintf(out, "| %s | `%s` | %s | %.2f | %.2f | %.2f |\n", finding.Section, finding.Name, finding.Metric, finding.Baseline, finding.Current, finding.Limit)
	}
}
