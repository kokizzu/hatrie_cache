package hatSql

import (
	"fmt"
	"sort"
)

// PivotAggregate selects the aggregation applied when more than one source row
// has the same group and pivot value.
type PivotAggregate string

const (
	// PivotSum sums numeric values and is the default aggregate.
	PivotSum PivotAggregate = "SUM"
	// PivotAverage averages numeric values.
	PivotAverage PivotAggregate = "AVG"
	// PivotMinimum selects the smallest numeric value.
	PivotMinimum PivotAggregate = "MIN"
	// PivotMaximum selects the largest numeric value.
	PivotMaximum PivotAggregate = "MAX"
	// PivotCount counts source rows, including rows whose value is null.
	PivotCount PivotAggregate = "COUNT"
)

// PivotSpec describes a wide analytics projection. GroupBy names the retained
// dimensions, PivotColumn supplies output column names, and ValueColumn
// supplies aggregated values. An empty Values list discovers pivot columns and
// orders them lexicographically; otherwise Values controls output shape/order.
type PivotSpec struct {
	GroupBy     []string
	PivotColumn string
	ValueColumn string
	Values      []string
	Aggregate   PivotAggregate
}

// UnpivotSpec describes the inverse long-form projection. When KeepColumns is
// empty, every source column other than Columns is retained. Null values are
// omitted unless IncludeNulls is set.
type UnpivotSpec struct {
	Columns      []string
	KeepColumns  []string
	NameColumn   string
	ValueColumn  string
	IncludeNulls bool
}

// PivotRows converts long-form rows into wide rows without mutating input.
func PivotRows(rows []Row, spec PivotSpec) ([]Row, error) {
	if spec.PivotColumn == "" {
		return nil, fmt.Errorf("pivot column is required")
	}
	if spec.ValueColumn == "" {
		return nil, fmt.Errorf("pivot value column is required")
	}
	aggregate := spec.Aggregate
	if aggregate == "" {
		aggregate = PivotSum
	}
	if !validPivotAggregate(aggregate) {
		return nil, fmt.Errorf("unsupported pivot aggregate %q", aggregate)
	}
	values := append([]string(nil), spec.Values...)
	fixedValues := len(values) != 0
	allowed := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return nil, fmt.Errorf("pivot values cannot be empty")
		}
		if _, exists := allowed[value]; exists {
			return nil, fmt.Errorf("pivot value %q appears more than once", value)
		}
		allowed[value] = struct{}{}
	}

	type group struct {
		row   Row
		cells map[string]pivotCell
	}
	groups := make(map[string]*group, len(rows))
	ordered := make([]*group, 0)
	for _, row := range rows {
		pivotValue, exists := row[spec.PivotColumn]
		if !exists || pivotValue == nil {
			continue
		}
		column := fmt.Sprint(pivotValue)
		if fixedValues {
			if _, exists := allowed[column]; !exists {
				continue
			}
		} else if _, exists := allowed[column]; !exists {
			allowed[column] = struct{}{}
			values = append(values, column)
		}
		key := pivotGroupKey(row, spec.GroupBy)
		current := groups[key]
		if current == nil {
			current = &group{row: pivotGroupRow(row, spec.GroupBy), cells: make(map[string]pivotCell)}
			groups[key] = current
			ordered = append(ordered, current)
		}
		cell := current.cells[column]
		if err := cell.add(row[spec.ValueColumn], aggregate); err != nil {
			return nil, fmt.Errorf("pivot %q value: %w", column, err)
		}
		current.cells[column] = cell
	}
	if len(spec.Values) == 0 {
		sort.Strings(values)
	}
	output := make([]Row, 0, len(ordered))
	for _, current := range ordered {
		row := pivotGroupRow(current.row, spec.GroupBy)
		for _, value := range values {
			row[value] = current.cells[value].value(aggregate)
		}
		output = append(output, row)
	}
	return output, nil
}

// UnpivotRows converts selected wide columns into long-form rows without
// mutating input.
func UnpivotRows(rows []Row, spec UnpivotSpec) ([]Row, error) {
	if len(spec.Columns) == 0 {
		return nil, fmt.Errorf("unpivot columns are required")
	}
	if spec.NameColumn == "" || spec.ValueColumn == "" {
		return nil, fmt.Errorf("unpivot name and value columns are required")
	}
	pivotColumns := make(map[string]struct{}, len(spec.Columns))
	for _, column := range spec.Columns {
		if column == "" {
			return nil, fmt.Errorf("unpivot columns cannot be empty")
		}
		if _, exists := pivotColumns[column]; exists {
			return nil, fmt.Errorf("unpivot column %q appears more than once", column)
		}
		pivotColumns[column] = struct{}{}
	}
	output := make([]Row, 0, len(rows)*len(spec.Columns))
	for _, source := range rows {
		keep := spec.KeepColumns
		if len(keep) == 0 {
			keep = make([]string, 0, len(source)-len(pivotColumns))
			for name := range source {
				if _, pivoted := pivotColumns[name]; !pivoted {
					keep = append(keep, name)
				}
			}
			sort.Strings(keep)
		}
		for _, column := range spec.Columns {
			value := source[column]
			if value == nil && !spec.IncludeNulls {
				continue
			}
			row := make(Row, len(keep)+2)
			for _, name := range keep {
				row[name] = source[name]
			}
			row[spec.NameColumn] = column
			row[spec.ValueColumn] = value
			output = append(output, row)
		}
	}
	return output, nil
}

type pivotCell struct {
	sum   float64
	count int
	min   float64
	max   float64
	seen  bool
}

func (cell *pivotCell) add(value interface{}, aggregate PivotAggregate) error {
	if aggregate == PivotCount {
		cell.count++
		return nil
	}
	if value == nil {
		return nil
	}
	number, ok := Number(value)
	if !ok {
		return fmt.Errorf("expected numeric value, got %T", value)
	}
	cell.sum += number
	cell.count++
	if !cell.seen || number < cell.min {
		cell.min = number
	}
	if !cell.seen || number > cell.max {
		cell.max = number
	}
	cell.seen = true
	return nil
}

func (cell pivotCell) value(aggregate PivotAggregate) interface{} {
	if aggregate == PivotCount {
		return cell.count
	}
	if !cell.seen {
		return nil
	}
	switch aggregate {
	case PivotAverage:
		return cell.sum / float64(cell.count)
	case PivotMinimum:
		return cell.min
	case PivotMaximum:
		return cell.max
	default:
		return cell.sum
	}
}

func validPivotAggregate(aggregate PivotAggregate) bool {
	switch aggregate {
	case PivotSum, PivotAverage, PivotMinimum, PivotMaximum, PivotCount:
		return true
	}
	return false
}

func pivotGroupKey(row Row, columns []string) string {
	key := ""
	for _, column := range columns {
		key += fmt.Sprintf("%T:%#v\x00", row[column], row[column])
	}
	return key
}

func pivotGroupRow(source Row, columns []string) Row {
	row := make(Row, len(columns))
	for _, column := range columns {
		row[column] = source[column]
	}
	return row
}
