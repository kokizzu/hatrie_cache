package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestRollingSchemaDeploymentRunOrdersNodesAndIsRetrySafe(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-b", "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	events := make([]string, 0, 4)
	install := func(_ context.Context, node string, schema Schema) error {
		users, ok := schema.Sources["users"]
		if !ok || len(users.Columns) != 2 {
			t.Fatalf("install(%q) received incomplete schema", node)
		}
		users.Columns = append(users.Columns, Column{Name: "callback_only", Type: TypeText})
		schema.Sources["users"] = users
		events = append(events, "install:"+node)
		return nil
	}
	activate := func(_ context.Context, node string, schema Schema) error {
		if len(schema.Sources["users"].Columns) != 2 {
			t.Fatalf("activate(%q) received callback-mutated schema", node)
		}
		events = append(events, "activate:"+node)
		return nil
	}
	if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
		t.Fatal(err)
	}
	want := []string{"install:node-a", "activate:node-a", "install:node-b", "activate:node-b"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if !deployment.Complete() {
		t.Fatal("deployment is incomplete after Run")
	}
	eventsBeforeRetry := append([]string(nil), events...)
	if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, eventsBeforeRetry) {
		t.Fatalf("retry events = %v, want unchanged %v", events, eventsBeforeRetry)
	}
}

func TestRollingSchemaDeploymentRunResumesAfterHookFailure(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	wantFailure := errors.New("node-b unavailable")
	failNodeB := true
	installCalls := make(map[string]int)
	install := func(_ context.Context, node string, _ Schema) error {
		installCalls[node]++
		if node == "node-b" && failNodeB {
			failNodeB = false
			return wantFailure
		}
		return nil
	}
	activate := func(_ context.Context, _ string, _ Schema) error { return nil }
	if err := plan.Run(context.Background(), deployment, install, activate); !errors.Is(err, wantFailure) {
		t.Fatalf("first Run() error = %v, want hook failure", err)
	}
	if phase, _ := deployment.Phase("node-a"); phase != RollingSchemaPhaseActive {
		t.Fatalf("node-a phase = %s, want active", phase)
	}
	if phase, _ := deployment.Phase("node-b"); phase != RollingSchemaPhasePending {
		t.Fatalf("node-b phase = %s, want pending", phase)
	}
	if err := plan.Run(context.Background(), deployment, install, activate); err != nil {
		t.Fatal(err)
	}
	if !deployment.Complete() || installCalls["node-a"] != 1 || installCalls["node-b"] != 2 {
		t.Fatalf("complete = %v, install calls = %v", deployment.Complete(), installCalls)
	}
}

func TestRollingSchemaDeploymentRunStopsAfterCancellationAndValidatesHooks(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a", "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	if err := plan.Run(context.Background(), deployment, nil, func(context.Context, string, Schema) error { return nil }); !errors.Is(err, ErrRollingSchemaCoordinatorInvalid) {
		t.Fatalf("nil install error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	activate := func(_ context.Context, node string, _ Schema) error {
		if node == "node-a" {
			cancel()
		}
		return nil
	}
	if err := plan.Run(ctx, deployment, func(context.Context, string, Schema) error { return nil }, activate); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run() error = %v, want context.Canceled", err)
	}
	if phase, _ := deployment.Phase("node-a"); phase != RollingSchemaPhaseActive {
		t.Fatalf("node-a phase = %s, want active", phase)
	}
	if phase, _ := deployment.Phase("node-b"); phase != RollingSchemaPhasePending {
		t.Fatalf("node-b phase = %s, want pending", phase)
	}
}

func TestRollingSchemaPlanRunRejectsConcurrentRunForSameNode(t *testing.T) {
	plan, err := NewRollingSchemaPlan(rollingSchemaFixture(1, false), rollingSchemaFixture(2, true), []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Begin()
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	var once sync.Once
	go func() {
		firstDone <- plan.Run(
			context.Background(),
			deployment,
			func(context.Context, string, Schema) error {
				once.Do(func() { close(started) })
				<-release
				return nil
			},
			func(context.Context, string, Schema) error { return nil },
		)
	}()
	<-started
	if err := plan.Run(
		context.Background(),
		deployment,
		func(context.Context, string, Schema) error { return nil },
		func(context.Context, string, Schema) error { return nil },
	); !errors.Is(err, ErrRollingSchemaTransition) {
		t.Fatalf("concurrent Run() error = %v, want transition error", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
