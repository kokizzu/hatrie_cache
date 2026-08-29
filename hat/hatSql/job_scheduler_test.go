package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestJobSchedulerRunsStoredQueryConditionAndDestination(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	rows := map[string][]hatSql.Row{
		"people":  {{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Lin"}},
		"invalid": {},
	}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	scheduler, err := hatSql.NewJobScheduler(resolver, hatSql.QueryOptions{}, hatSql.JobSchedulerOptions{
		Now:                func() time.Time { return now },
		RunHistoryCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	parameters := []interface{}{"people", int64(2)}
	if err := scheduler.Create(hatSql.JobDefinition{
		Name:        "eligible-people",
		Query:       "FROM CACHE($1) AS people WHERE people.id >= $2 SELECT people.name",
		Parameters:  parameters,
		Destination: hatSql.JobDestination{Name: "eligible_people"},
		Schedule:    hatSql.JobSchedule{Every: time.Minute},
		Condition: &hatSql.JobCondition{
			Query:       "FROM CACHE('invalid') SELECT id",
			RequireRows: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	parameters[1] = int64(3)

	runs, err := scheduler.RunDue(context.Background())
	if err != nil || len(runs) != 1 || runs[0].Status != hatSql.JobRunSkipped || runs[0].SkipReason != "condition returned no rows" {
		t.Fatalf("first RunDue() = %#v, %v", runs, err)
	}
	if _, exists := scheduler.Destination("eligible_people"); exists {
		t.Fatal("skipped job published a destination")
	}

	now = now.Add(time.Minute)
	rows["invalid"] = []hatSql.Row{{"id": int64(9)}}
	runs, err = scheduler.RunDue(context.Background())
	if err != nil || len(runs) != 1 || runs[0].Status != hatSql.JobRunSucceeded || runs[0].Output.Rows != 1 || len(runs[0].Plan) == 0 {
		t.Fatalf("second RunDue() = %#v, %v", runs, err)
	}
	result, exists := scheduler.Destination("eligible_people")
	if !exists || len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("Destination() = %#v, %v", result, exists)
	}
	history := scheduler.History()
	if len(history) != 2 || history[0].Status != hatSql.JobRunSkipped || history[1].Status != hatSql.JobRunSucceeded || history[1].Duration < 0 {
		t.Fatalf("History() = %#v", history)
	}
}

func TestJobSchedulerLeaseRejectsConcurrentRun(t *testing.T) {
	started := make(chan struct{})
	continueRun := make(chan struct{})
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if key == "slow" {
			close(started)
			<-continueRun
		}
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	scheduler, err := hatSql.NewJobScheduler(resolver, hatSql.QueryOptions{}, hatSql.JobSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Create(hatSql.JobDefinition{
		Name:     "slow-job",
		Query:    "FROM CACHE('slow') SELECT id",
		Schedule: hatSql.JobSchedule{Every: time.Hour},
	}); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := scheduler.Run(context.Background(), "slow-job")
		first <- err
	}()
	<-started
	duplicate, err := scheduler.Run(context.Background(), "slow-job")
	if !errors.Is(err, hatSql.ErrJobAlreadyRunning) || duplicate.Status != hatSql.JobRunAlreadyRunning {
		t.Fatalf("duplicate Run() = %#v, %v", duplicate, err)
	}
	close(continueRun)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if history := scheduler.History(); len(history) != 2 || history[0].Status != hatSql.JobRunAlreadyRunning || history[1].Status != hatSql.JobRunSucceeded {
		t.Fatalf("History() = %#v", history)
	}
}
