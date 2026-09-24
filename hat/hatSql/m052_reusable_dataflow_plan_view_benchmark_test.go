package hatSql

import "testing"

var benchmarkM052PlanViewSink int

func BenchmarkCompiledSQLQueryDataflowPlanView(b *testing.B) {
	query, err := CompileSQLQuery("FROM CACHE('items') SELECT id WHERE id >= 2 ORDER BY id LIMIT 4")
	if err != nil {
		b.Fatal(err)
	}
	view := query.DataflowPlanView()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		checksum := view.Root()
		for fragmentIndex := 0; fragmentIndex < view.FragmentCount(); fragmentIndex++ {
			fragment, ok := view.Fragment(fragmentIndex)
			if !ok {
				b.Fatal("fragment disappeared")
			}
			checksum += fragment.ID() + fragment.InputCount() + len(fragment.Kind())
		}
		benchmarkM052PlanViewSink = checksum
	}
}

func BenchmarkCompiledSQLQueryLowerDataflowInspect(b *testing.B) {
	query, err := CompileSQLQuery("FROM CACHE('items') SELECT id WHERE id >= 2 ORDER BY id LIMIT 4")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan := query.LowerDataflow()
		checksum := plan.Root
		for _, fragment := range plan.Fragments {
			checksum += fragment.ID + len(fragment.Inputs) + len(fragment.Kind)
		}
		benchmarkM052PlanViewSink = checksum
	}
}
