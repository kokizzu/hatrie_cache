package hatTopology

import "testing"

var tu14RouteSink VShardRoute
var tu14PlanSink VShardRebalancePlan

func BenchmarkTU14VShardRoute(b *testing.B) {
	mapping, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       256,
		ReplicationFactor: 2,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}, {ID: "node-d"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	keys := []string{"sg:user:1", "us:user:2", "eu:user:3", "jp:user:4"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		route, ok := mapping.RouteKey(keys[index%len(keys)])
		if !ok {
			b.Fatal("route unavailable")
		}
		tu14RouteSink = route
	}
}

func BenchmarkTU14VShardPlanRebalance(b *testing.B) {
	mapping, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       256,
		ReplicationFactor: 2,
		Nodes:             []VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}, {ID: "node-d"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	nodes := []VShardNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}, {ID: "node-e"}}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := mapping.PlanRebalance(nodes, uint64(index+2), uint64(index+2))
		if err != nil {
			b.Fatal(err)
		}
		tu14PlanSink = plan
	}
}
