package hatSql

import "container/heap"

type sqlMutationReadyHeap []string

func (ready sqlMutationReadyHeap) Len() int { return len(ready) }

func (ready sqlMutationReadyHeap) Less(left, right int) bool {
	return ready[left] < ready[right]
}

func (ready sqlMutationReadyHeap) Swap(left, right int) {
	ready[left], ready[right] = ready[right], ready[left]
}

func (ready *sqlMutationReadyHeap) Push(value any) {
	*ready = append(*ready, value.(string))
}

func (ready *sqlMutationReadyHeap) Pop() any {
	values := *ready
	last := len(values) - 1
	value := values[last]
	*ready = values[:last]
	return value
}

func (graph *SQLMutationDependencyGraph) ensureReadyIndexesLocked() {
	if graph.tasks == nil {
		graph.tasks = make(map[string]*SQLMutationTaskRecord)
	}
	if graph.dependents == nil {
		graph.dependents = make(map[string][]string)
	}
	if graph.readySet == nil {
		graph.readySet = make(map[string]struct{})
	}
}

func (graph *SQLMutationDependencyGraph) enqueueReadyLocked(id string) {
	graph.ensureReadyIndexesLocked()
	task, ok := graph.tasks[id]
	if !ok || task.State != SQLMutationTaskPending || !graph.dependenciesCompletedLocked(task) {
		return
	}
	if _, exists := graph.readySet[id]; exists {
		return
	}
	graph.readySet[id] = struct{}{}
	heap.Push(&graph.ready, id)
}

func (graph *SQLMutationDependencyGraph) popReadyLocked() string {
	id := heap.Pop(&graph.ready).(string)
	delete(graph.readySet, id)
	return id
}

func (graph *SQLMutationDependencyGraph) rebuildReadyIndexesLocked() {
	graph.ensureReadyIndexesLocked()
	graph.dependents = make(map[string][]string, len(graph.tasks))
	graph.ready = nil
	graph.readySet = make(map[string]struct{})
	for id, task := range graph.tasks {
		for _, dependency := range task.DependsOn {
			graph.dependents[dependency] = append(graph.dependents[dependency], id)
		}
	}
	for id, task := range graph.tasks {
		if task.State == SQLMutationTaskPending && graph.dependenciesCompletedLocked(task) {
			graph.readySet[id] = struct{}{}
			graph.ready = append(graph.ready, id)
		}
	}
	heap.Init(&graph.ready)
}
