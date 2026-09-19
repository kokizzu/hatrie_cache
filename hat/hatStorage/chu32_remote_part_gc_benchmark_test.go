package hatStorage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

type chu32BaselineObject struct {
	uri          string
	lastModified time.Time
	sizeBytes    uint64
}

var chu32BenchmarkSink uint64

func chu32BaselineInput() ([]chu32BaselineObject, []string, time.Time) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	objects := make([]chu32BaselineObject, 10000)
	for index := range objects {
		objects[index] = chu32BaselineObject{
			uri:          fmt.Sprintf("s3://bucket/parts/%08d", index),
			lastModified: now.Add(-48 * time.Hour),
			sizeBytes:    uint64(index + 1),
		}
	}
	reachable := make([]string, 2000)
	for index := range reachable {
		reachable[index] = objects[index*3].uri
	}
	return objects, reachable, now
}

func BenchmarkCHU32NaiveReachabilitySweep(b *testing.B) {
	objects, reachable, now := chu32BaselineInput()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var reclaimable uint64
		for _, object := range objects {
			if now.Sub(object.lastModified) < 24*time.Hour {
				continue
			}
			live := false
			for _, uri := range reachable {
				if uri == object.uri {
					live = true
					break
				}
			}
			if !live {
				reclaimable += object.sizeBytes
			}
		}
		chu32BenchmarkSink = reclaimable
	}
}

func BenchmarkCHU32NaiveReachabilityPlan(b *testing.B) {
	objects, reachable, now := chu32BaselineInput()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		candidates := make([]chu32BaselineObject, 0, len(objects))
		for _, object := range objects {
			if now.Sub(object.lastModified) < 24*time.Hour {
				continue
			}
			live := false
			for _, uri := range reachable {
				if uri == object.uri {
					live = true
					break
				}
			}
			if !live {
				candidates = append(candidates, object)
			}
		}
		chu32BenchmarkSink = uint64(len(candidates))
	}
}

func chu32PlanBenchmarkInput(b testing.TB) ([]hatStorage.RemotePartReference, []hatStorage.RemotePartGCObject, hatStorage.RemotePartGCOptions) {
	b.Helper()
	objects, reachableURIs, now := chu32BaselineInput()
	reachable := make([]hatStorage.RemotePartReference, len(reachableURIs))
	for index, objectURI := range reachableURIs {
		var err error
		reachable[index], err = hatStorage.NewRemotePartReference(objectURI, fmt.Sprintf("parts/%d.json", index), fmt.Sprintf("sha256:live-%d", index), objects[index*3].sizeBytes)
		if err != nil {
			b.Fatal(err)
		}
	}
	listed := make([]hatStorage.RemotePartGCObject, len(objects))
	for index, object := range objects {
		listed[index] = hatStorage.RemotePartGCObject{
			ObjectURI:    object.uri,
			SizeBytes:    object.sizeBytes,
			LastModified: object.lastModified,
		}
	}
	return reachable, listed, hatStorage.RemotePartGCOptions{Now: now, MinAge: 24 * time.Hour}
}

func BenchmarkCHU32ReachabilityPlan(b *testing.B) {
	reachable, listed, options := chu32PlanBenchmarkInput(b)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		plan, err := hatStorage.PlanRemotePartGarbageCollection(reachable, listed, options)
		if err != nil {
			b.Fatal(err)
		}
		chu32BenchmarkSink = plan.ReclaimableBytes
	}
}

type chu32NoopDeleteStore struct{}

func (chu32NoopDeleteStore) DeleteRemotePart(_ context.Context, _ string) error {
	return nil
}

func BenchmarkCHU32GarbageCollectionExecute(b *testing.B) {
	reachable, listed, options := chu32PlanBenchmarkInput(b)
	plan, err := hatStorage.PlanRemotePartGarbageCollection(reachable, listed, options)
	if err != nil {
		b.Fatal(err)
	}
	store := chu32NoopDeleteStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := hatStorage.ExecuteRemotePartGarbageCollection(context.Background(), store, plan)
		if err != nil {
			b.Fatal(err)
		}
		chu32BenchmarkSink = uint64(result.DeletedObjects)
	}
}
