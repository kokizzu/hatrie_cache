# CH-014 Resumable Mutation Dependency Graph

`hatSql.SQLMutationDependencyGraph` is an opt-in coordinator for applications
that execute related SQL mutations outside one atomic command batch. It keeps a
bounded task graph, exposes only dependency-ready work, and persists lifecycle
state so a restarted worker can continue from the last completed task.

The graph does not parse SQL, execute mutations, or alter the default
`ExecuteSQLMutation` path. The caller supplies stable task IDs and invokes the
SQL executor after claiming a task.

## Basic workflow

Register dependencies before their dependents:

```go
graph, err := hatSql.NewSQLMutationDependencyGraph(4096)
if err != nil {
    return err
}
_ = graph.Add(hatSql.SQLMutationTask{ID: "orders:load"})
_ = graph.Add(hatSql.SQLMutationTask{
    ID:        "orders:rebuild-index",
    DependsOn: []string{"orders:load"},
})
sqlByTask := map[string]string{
    "orders:load":          "INSERT INTO cache (key, value) VALUES ('orders:loaded', '1')",
    "orders:rebuild-index": "INSERT INTO cache (key, value) VALUES ('orders:indexed', '1')",
}

for _, task := range graph.ClaimReady(32) {
    result, err := hatSql.ExecuteSQLMutation(ctx, trie, sqlByTask[task.ID], nil, options)
    if err != nil {
        _ = graph.Fail(task.ID, task.Attempt, err.Error())
        continue
    }
    _ = result
    _ = graph.Complete(task.ID, task.Attempt)
}
```

`ClaimReady` transitions tasks from `pending` to `running` atomically and sorts
by ID, so concurrent workers cannot claim the same attempt. A task becomes
ready only after every dependency is `completed`.

## Retry and recovery

`Fail` records a bounded diagnostic and marks the owned attempt failed. Call
`Retry` after deciding that it is safe to execute the task again. Every new
claim increments `Attempt`; stale workers cannot complete or fail a newer
attempt.

After loading a snapshot from a process that may have lost workers, call
`RequeueRunning` before claiming work. Completed tasks remain completed, while
in-flight tasks become pending and can be retried.

```go
file, err := os.Open("mutation-progress.json")
if err != nil {
    return err
}
defer file.Close()
if err := graph.Load(file); err != nil {
    return err
}
graph.RequeueRunning()
```

`Save` writes a versioned JSON snapshot. `Load` validates the complete graph,
including duplicate IDs, missing dependencies, invalid states, size limits,
and dependency cycles, before replacing any live state. A malformed snapshot
therefore cannot partially mutate the active coordinator.

## Limits and defaults

- A zero constructor limit uses `DefaultSQLMutationDependencyGraphMaxTasks`
  (`4096`).
- A positive limit bounds retained tasks; negative and excessively large limits
  are rejected.
- IDs are trimmed and limited to 256 bytes.
- Failure diagnostics are limited to 1024 bytes.
- Snapshot input is limited to 4 MiB.
- The graph is concurrency-safe, but it does not provide distributed consensus
  or cross-process locking. Persist the snapshot through the application's
  durable storage and ensure only one owner restores a given task attempt.

The dependency graph is deliberately separate from the SQL executor: callers
can choose task boundaries and transaction behavior without changing existing
SQL syntax, storage formats, or default mutation latency.

See [BENCHMARK.md](BENCHMARK.md#ch-014-resumable-mutation-dependency-graph) for
the measured CPU, memory, and allocation cost.
