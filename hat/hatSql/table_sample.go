package hatSql

import (
	"fmt"
	"sort"
)

type sqlTableSample struct {
	mode  string
	value int
	seed  uint64
}

func (p *sqlQueryParser) parseTableSample() (sqlTableSample, error) {
	token := p.current()
	p.next()
	sample := sqlTableSample{}
	switch {
	case p.keyword("BERNOULLI"):
		sample.mode = "BERNOULLI"
	case p.keyword("RESERVOIR"):
		sample.mode = "RESERVOIR"
	default:
		return sample, p.expected(p.current(), "BERNOULLI or RESERVOIR", []string{"BERNOULLI", "RESERVOIR"})
	}
	p.next()
	if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
		return sample, err
	}
	value, err := p.parseInteger("TABLESAMPLE " + sample.mode)
	if err != nil {
		return sample, err
	}
	if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
		return sample, err
	}
	sample.value = value
	if sample.mode == "BERNOULLI" && (sample.value < 0 || sample.value > 100) {
		return sample, p.diagnostic(token, "TABLESAMPLE BERNOULLI percentage must be between 0 and 100")
	}
	if sample.mode == "RESERVOIR" && (sample.value < 1 || sample.value > maxSQLQueryRows) {
		return sample, p.diagnostic(token, fmt.Sprintf("TABLESAMPLE RESERVOIR count must be between 1 and %d", maxSQLQueryRows))
	}
	if p.keyword("REPEATABLE") {
		p.next()
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return sample, err
		}
		seed, err := p.parseInteger("TABLESAMPLE REPEATABLE")
		if err != nil {
			return sample, err
		}
		if seed < 0 {
			return sample, p.diagnostic(token, "TABLESAMPLE REPEATABLE seed must not be negative")
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sample, err
		}
		sample.seed = uint64(seed)
	}
	return sample, nil
}

func (sample sqlTableSample) detail() string {
	return fmt.Sprintf("TABLESAMPLE %s (%d) REPEATABLE (%d)", sample.mode, sample.value, sample.seed)
}

func sqlSampleRows(rows []SQLRow, sample sqlTableSample) []SQLRow {
	if len(rows) == 0 {
		return nil
	}
	switch sample.mode {
	case "BERNOULLI":
		if sample.value == 0 {
			return []SQLRow{}
		}
		if sample.value == 100 {
			return append([]SQLRow(nil), rows...)
		}
		state := sample.seed
		out := make([]SQLRow, 0, len(rows)*sample.value/100)
		for _, row := range rows {
			if sqlSampleBounded(&state, 100) < uint64(sample.value) {
				out = append(out, row)
			}
		}
		return out
	case "RESERVOIR":
		count := sample.value
		if count >= len(rows) {
			return append([]SQLRow(nil), rows...)
		}
		type selectedRow struct {
			index int
			row   SQLRow
		}
		out := make([]selectedRow, count)
		for index := 0; index < count; index++ {
			out[index] = selectedRow{index: index, row: rows[index]}
		}
		state := sample.seed
		for index := count; index < len(rows); index++ {
			candidate := sqlSampleBounded(&state, uint64(index+1))
			if candidate < uint64(count) {
				out[candidate] = selectedRow{index: index, row: rows[index]}
			}
		}
		sort.Slice(out, func(left, right int) bool { return out[left].index < out[right].index })
		rows := make([]SQLRow, len(out))
		for index := range out {
			rows[index] = out[index].row
		}
		return rows
	}
	return nil
}

func sqlSampleBounded(state *uint64, bound uint64) uint64 {
	limit := ^uint64(0) - ^uint64(0)%bound
	for {
		value := sqlSampleNext(state)
		if value < limit {
			return value % bound
		}
	}
}

func sqlSampleNext(state *uint64) uint64 {
	*state += 0x9e3779b97f4a7c15
	value := *state
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return value ^ value>>31
}
