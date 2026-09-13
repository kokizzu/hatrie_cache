package hatSql

import "strconv"

func ch050BenchmarkRows() []Row {
	rows := make([]Row, 2048)
	for index := range rows {
		rows[index] = Row{
			"id":      int64(index),
			"name":    "name-" + strconv.Itoa(index%32),
			"enabled": index%3 == 0,
			"payload": []byte("payload-" + strconv.Itoa(index%16)),
		}
	}
	return rows
}
