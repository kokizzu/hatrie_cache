package hatPipeline

import (
	"errors"
	"sync"
	"testing"
)

func TestMZ012ConsumerGroupFenceLifecycle(t *testing.T) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 4})
	if err != nil {
		t.Fatalf("NewConsumerGroupFence() error = %v", err)
	}

	first, err := fence.Rebalance([]ConsumerGroupPartitionOwner{
		{Partition: 0, Member: "consumer-a"},
		{Partition: 1, Member: "consumer-b"},
	})
	if err != nil {
		t.Fatalf("first Rebalance() error = %v", err)
	}
	if first.Generation != 1 || len(first.Assignments) != 2 {
		t.Fatalf("first snapshot = %#v, want generation 1 with two assignments", first)
	}

	lease, err := fence.Lease("consumer-a", 0)
	if err != nil {
		t.Fatalf("Lease() error = %v", err)
	}
	if err := fence.Validate(lease); err != nil {
		t.Fatalf("Validate(current lease) error = %v", err)
	}

	first.Assignments[0].Member = "tampered"
	if got := fence.Snapshot().Assignments[0].Member; got == "tampered" {
		t.Fatal("Snapshot() exposed mutable internal assignment storage")
	}

	second, err := fence.Rebalance([]ConsumerGroupPartitionOwner{
		{Partition: 0, Member: "consumer-b"},
		{Partition: 1, Member: "consumer-a"},
	})
	if err != nil {
		t.Fatalf("second Rebalance() error = %v", err)
	}
	if second.Generation != 2 {
		t.Fatalf("second generation = %d, want 2", second.Generation)
	}
	if err := fence.Validate(lease); !errors.Is(err, ErrConsumerGroupFenceStaleGeneration) {
		t.Fatalf("Validate(stale lease) error = %v, want stale-generation error", err)
	}

	newLease, err := fence.Lease("consumer-b", 0)
	if err != nil {
		t.Fatalf("Lease(new owner) error = %v", err)
	}
	if err := fence.Validate(newLease); err != nil {
		t.Fatalf("Validate(new lease) error = %v", err)
	}

	wrongOwner := newLease
	wrongOwner.Member = "consumer-a"
	if err := fence.Validate(wrongOwner); !errors.Is(err, ErrConsumerGroupFenceWrongOwner) {
		t.Fatalf("Validate(wrong owner) error = %v, want wrong-owner error", err)
	}

	if _, err := fence.Lease("consumer-a", 9); !errors.Is(err, ErrConsumerGroupFencePartitionUnassigned) {
		t.Fatalf("Lease(unassigned partition) error = %v, want unassigned error", err)
	}
}

func TestMZ012ConsumerGroupFenceRejectsInvalidRebalances(t *testing.T) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: 2})
	if err != nil {
		t.Fatalf("NewConsumerGroupFence() error = %v", err)
	}
	if _, err := fence.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "consumer-a"}}); err != nil {
		t.Fatalf("initial Rebalance() error = %v", err)
	}

	tests := []struct {
		name        string
		assignments []ConsumerGroupPartitionOwner
		want        error
	}{
		{
			name: "duplicate partition",
			assignments: []ConsumerGroupPartitionOwner{
				{Partition: 0, Member: "consumer-a"},
				{Partition: 0, Member: "consumer-b"},
			},
			want: ErrConsumerGroupFenceDuplicatePartition,
		},
		{
			name:        "negative partition",
			assignments: []ConsumerGroupPartitionOwner{{Partition: -1, Member: "consumer-a"}},
			want:        ErrConsumerGroupFenceInvalidPartition,
		},
		{
			name:        "empty member",
			assignments: []ConsumerGroupPartitionOwner{{Partition: 0, Member: "  "}},
			want:        ErrConsumerGroupFenceInvalidMember,
		},
		{
			name: "over capacity",
			assignments: []ConsumerGroupPartitionOwner{
				{Partition: 0, Member: "consumer-a"},
				{Partition: 1, Member: "consumer-b"},
				{Partition: 2, Member: "consumer-c"},
			},
			want: ErrConsumerGroupFenceOptionsInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := fence.Rebalance(test.assignments); !errors.Is(err, test.want) {
				t.Fatalf("Rebalance() error = %v, want %v", err, test.want)
			}
		})
	}

	if snapshot := fence.Snapshot(); snapshot.Generation != 1 || len(snapshot.Assignments) != 1 {
		t.Fatalf("rejected rebalance changed state = %#v", snapshot)
	}
}

func TestMZ012ConsumerGroupFenceCopiesAssignmentInput(t *testing.T) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{})
	if err != nil {
		t.Fatalf("NewConsumerGroupFence() error = %v", err)
	}
	assignments := []ConsumerGroupPartitionOwner{{Partition: 0, Member: "consumer-a"}}
	if _, err := fence.Rebalance(assignments); err != nil {
		t.Fatalf("Rebalance() error = %v", err)
	}
	assignments[0].Member = "consumer-b"
	lease, err := fence.Lease("consumer-a", 0)
	if err != nil {
		t.Fatalf("Lease() after caller mutation error = %v", err)
	}
	if err := fence.Validate(lease); err != nil {
		t.Fatalf("Validate() after caller mutation error = %v", err)
	}
}

func TestMZ012ConsumerGroupFenceSupportsSparsePartitions(t *testing.T) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{})
	if err != nil {
		t.Fatalf("NewConsumerGroupFence() error = %v", err)
	}
	snapshot, err := fence.Rebalance([]ConsumerGroupPartitionOwner{
		{Partition: 7, Member: "consumer-b"},
		{Partition: 2, Member: "consumer-a"},
	})
	if err != nil {
		t.Fatalf("Rebalance(sparse) error = %v", err)
	}
	if len(snapshot.Assignments) != 2 || snapshot.Assignments[0].Partition != 2 || snapshot.Assignments[1].Partition != 7 {
		t.Fatalf("sparse snapshot = %#v, want sorted assignments", snapshot)
	}
	lease, err := fence.Lease("consumer-b", 7)
	if err != nil {
		t.Fatalf("Lease(sparse) error = %v", err)
	}
	if err := fence.Validate(lease); err != nil {
		t.Fatalf("Validate(sparse) error = %v", err)
	}
}

func TestMZ012ConsumerGroupFenceConcurrentRebalanceAndValidation(t *testing.T) {
	fence, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{})
	if err != nil {
		t.Fatalf("NewConsumerGroupFence() error = %v", err)
	}
	if _, err := fence.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: "consumer-a"}}); err != nil {
		t.Fatalf("initial Rebalance() error = %v", err)
	}
	lease, err := fence.Lease("consumer-a", 0)
	if err != nil {
		t.Fatalf("Lease() error = %v", err)
	}

	var rebalance sync.WaitGroup
	rebalance.Add(1)
	go func() {
		defer rebalance.Done()
		for index := 0; index < 128; index++ {
			member := "consumer-a"
			if index%2 == 1 {
				member = "consumer-b"
			}
			if _, err := fence.Rebalance([]ConsumerGroupPartitionOwner{{Partition: 0, Member: member}}); err != nil {
				t.Errorf("concurrent Rebalance() error = %v", err)
			}
		}
	}()
	for index := 0; index < 256; index++ {
		err := fence.Validate(lease)
		if err != nil && !errors.Is(err, ErrConsumerGroupFenceStaleGeneration) && !errors.Is(err, ErrConsumerGroupFenceWrongOwner) {
			t.Fatalf("concurrent Validate() error = %v", err)
		}
	}
	rebalance.Wait()
}

func TestMZ012ConsumerGroupFenceValidatesConfigurationAndNilReceiver(t *testing.T) {
	if _, err := NewConsumerGroupFence("", ConsumerGroupFenceOptions{}); !errors.Is(err, ErrConsumerGroupFenceInvalidGroup) {
		t.Fatalf("empty group error = %v, want invalid-group error", err)
	}
	if _, err := NewConsumerGroupFence("orders", ConsumerGroupFenceOptions{MaxPartitions: -1}); !errors.Is(err, ErrConsumerGroupFenceOptionsInvalid) {
		t.Fatalf("negative maximum error = %v, want options error", err)
	}

	var fence *ConsumerGroupFence
	if _, err := fence.Rebalance(nil); !errors.Is(err, ErrConsumerGroupFenceNil) {
		t.Fatalf("nil Rebalance() error = %v, want nil error", err)
	}
	if _, err := fence.Lease("consumer-a", 0); !errors.Is(err, ErrConsumerGroupFenceNil) {
		t.Fatalf("nil Lease() error = %v, want nil error", err)
	}
	if err := fence.Validate(ConsumerGroupLease{}); !errors.Is(err, ErrConsumerGroupFenceNil) {
		t.Fatalf("nil Validate() error = %v, want nil error", err)
	}
}
