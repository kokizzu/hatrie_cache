package hatSql

import "testing"

func TestSQLExecutionArenaReusesRowsAndClearsDroppedTail(t *testing.T) {
	arena := sqlExecutionArena{}
	first := arena.acquireColumnarRows(3)
	first[2].singleAlias = "retained"
	firstAddress := &first[0]
	second := arena.acquireColumnarRows(2)
	if &second[0] != firstAddress {
		t.Fatal("arena did not reuse the prior row backing")
	}
	if tail := arena.columnarRowsBuffer[:3][2]; tail.singleAlias != "" || tail.columnar != nil || tail.singleRow != nil {
		t.Fatalf("arena retained dropped row references: %#v", tail)
	}
}
