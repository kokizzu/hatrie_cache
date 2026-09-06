package hatSql

import (
	"errors"
	"fmt"
)

var (
	// ErrColumnarMergeInvalid reports invalid part or field arguments.
	ErrColumnarMergeInvalid = errors.New("hatSql: invalid columnar merge")
	// ErrColumnarMergeFieldMissing reports a requested field absent from a part.
	ErrColumnarMergeFieldMissing = errors.New("hatSql: columnar merge field missing")
	// ErrColumnarMergeFieldMismatch reports a field with a different row count.
	ErrColumnarMergeFieldMismatch = errors.New("hatSql: columnar merge field row mismatch")
)

// ColumnarMergePart exposes one physical part to a vertical merge. LoadColumn
// should read only the named field and return false when that field is absent.
type ColumnarMergePart interface {
	RowCount() int
	LoadColumn(field string) ([]interface{}, bool, error)
}

// ColumnarBatchPart adapts an in-memory ColumnarBatch to ColumnarMergePart.
// It decodes dictionary columns only for fields requested by the merge.
type ColumnarBatchPart struct {
	Batch ColumnarBatch
}

// RowCount reports the number of rows in the part.
func (part ColumnarBatchPart) RowCount() int {
	return part.Batch.Rows
}

// LoadColumn returns one logical column with dictionary values decoded.
func (part ColumnarBatchPart) LoadColumn(field string) ([]interface{}, bool, error) {
	if part.Batch.Rows < 0 {
		return nil, false, ErrColumnarMergeInvalid
	}
	_, plain := part.Batch.Columns[field]
	_, dictionary := part.Batch.Dictionaries[field]
	if !plain && !dictionary {
		return nil, false, nil
	}
	values := make([]interface{}, part.Batch.Rows)
	for row := range values {
		value, ok := part.Batch.Value(field, row)
		if !ok {
			return nil, true, fmt.Errorf("%w: %q", ErrColumnarMergeFieldMismatch, field)
		}
		values[row] = value
	}
	return values, true, nil
}

// MergeColumnarParts performs a vertical merge of row-aligned parts. Only
// fields in fields are requested from each part, which lets a storage adapter
// avoid reading unchanged wide columns. The resulting rows retain part order.
func MergeColumnarParts(parts []ColumnarMergePart, fields []string) (ColumnarBatch, error) {
	if len(parts) == 0 || len(fields) == 0 {
		return ColumnarBatch{}, ErrColumnarMergeInvalid
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" {
			return ColumnarBatch{}, ErrColumnarMergeInvalid
		}
		if _, found := seen[field]; found {
			return ColumnarBatch{}, fmt.Errorf("%w: duplicate field %q", ErrColumnarMergeInvalid, field)
		}
		seen[field] = struct{}{}
	}

	rowCount := 0
	partRows := make([]int, len(parts))
	for index, part := range parts {
		if part == nil {
			return ColumnarBatch{}, fmt.Errorf("%w: nil part %d", ErrColumnarMergeInvalid, index+1)
		}
		rows := part.RowCount()
		if rows < 0 || rowCount > int(^uint(0)>>1)-rows {
			return ColumnarBatch{}, fmt.Errorf("%w: part %d row count", ErrColumnarMergeInvalid, index+1)
		}
		partRows[index] = rows
		rowCount += rows
	}

	merged := ColumnarBatch{Columns: make(map[string][]interface{}, len(fields)), Rows: rowCount}
	for _, field := range fields {
		values := make([]interface{}, 0, rowCount)
		for index, part := range parts {
			column, found, err := part.LoadColumn(field)
			if err != nil {
				return ColumnarBatch{}, err
			}
			if !found {
				return ColumnarBatch{}, fmt.Errorf("%w: %q in part %d", ErrColumnarMergeFieldMissing, field, index+1)
			}
			if len(column) != partRows[index] {
				return ColumnarBatch{}, fmt.Errorf("%w: %q in part %d", ErrColumnarMergeFieldMismatch, field, index+1)
			}
			values = append(values, column...)
		}
		merged.Columns[field] = values
	}
	merged.EncodeRepeatedStrings()
	return merged, nil
}
