package hatSql

// SQLDataflowPlanView is a read-only view over a compiled query's memoized
// dataflow plan. It exposes scalar accessors instead of mutable slices, so
// callers can inspect reusable fragments without cloning the plan.
type SQLDataflowPlanView struct {
	plan *SQLDataflowPlan
}

// SQLDataflowFragmentView is a read-only view over one logical fragment.
type SQLDataflowFragmentView struct {
	plan  *SQLDataflowPlan
	index int
}

// DataflowPlanView returns a zero-allocation view after the compiled query's
// first lowering. The existing LowerDataflow method remains the safe mutable
// copy API for callers that need slices they can edit.
func (query *CompiledSQLQuery) DataflowPlanView() SQLDataflowPlanView {
	if query == nil || query.template == nil {
		return SQLDataflowPlanView{}
	}
	query.dataflowOnce.Do(func() {
		query.dataflowPlan = buildSQLDataflowPlan(query.source, query.template)
	})
	return SQLDataflowPlanView{plan: query.dataflowPlan}
}

// Valid reports whether the view references a compiled plan.
func (view SQLDataflowPlanView) Valid() bool {
	return view.plan != nil
}

// Format returns the plan format identifier.
func (view SQLDataflowPlanView) Format() string {
	if view.plan == nil {
		return ""
	}
	return view.plan.Format
}

// Source returns the original compiled SQL source.
func (view SQLDataflowPlanView) Source() string {
	if view.plan == nil {
		return ""
	}
	return view.plan.Source
}

// Root returns the root fragment ID, or -1 for an invalid view.
func (view SQLDataflowPlanView) Root() int {
	if view.plan == nil {
		return -1
	}
	return view.plan.Root
}

// FragmentCount returns the number of logical fragments.
func (view SQLDataflowPlanView) FragmentCount() int {
	if view.plan == nil {
		return 0
	}
	return len(view.plan.Fragments)
}

// Fragment returns a read-only fragment view for index.
func (view SQLDataflowPlanView) Fragment(index int) (SQLDataflowFragmentView, bool) {
	if view.plan == nil || index < 0 || index >= len(view.plan.Fragments) {
		return SQLDataflowFragmentView{}, false
	}
	return SQLDataflowFragmentView{plan: view.plan, index: index}, true
}

// Valid reports whether the fragment view references a fragment.
func (fragment SQLDataflowFragmentView) Valid() bool {
	return fragment.plan != nil && fragment.index >= 0 && fragment.index < len(fragment.plan.Fragments)
}

// ID returns the stable fragment ID, or -1 for an invalid view.
func (fragment SQLDataflowFragmentView) ID() int {
	if !fragment.Valid() {
		return -1
	}
	return fragment.plan.Fragments[fragment.index].ID
}

// Kind returns the logical operator kind, or an empty string for an invalid view.
func (fragment SQLDataflowFragmentView) Kind() string {
	if !fragment.Valid() {
		return ""
	}
	return fragment.plan.Fragments[fragment.index].Kind
}

// Detail returns the immutable operator detail, or an empty string for an invalid view.
func (fragment SQLDataflowFragmentView) Detail() string {
	if !fragment.Valid() {
		return ""
	}
	return fragment.plan.Fragments[fragment.index].Detail
}

// InputCount returns the number of upstream fragment IDs.
func (fragment SQLDataflowFragmentView) InputCount() int {
	if !fragment.Valid() {
		return 0
	}
	return len(fragment.plan.Fragments[fragment.index].Inputs)
}

// Input returns one upstream fragment ID without exposing the backing slice.
func (fragment SQLDataflowFragmentView) Input(index int) (int, bool) {
	if !fragment.Valid() || index < 0 || index >= len(fragment.plan.Fragments[fragment.index].Inputs) {
		return 0, false
	}
	return fragment.plan.Fragments[fragment.index].Inputs[index], true
}
