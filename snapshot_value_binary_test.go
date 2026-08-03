package hatriecache

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSnapshotValueBinaryMapOrderIsDeterministic(t *testing.T) {
	ascending := make(Map, 16)
	descending := make(Map, 16)
	for index := 0; index < 16; index++ {
		ascending[fmt.Sprintf("field:%02d", index)] = index
	}
	for index := 15; index >= 0; index-- {
		descending[fmt.Sprintf("field:%02d", index)] = index
	}
	first, ok, err := marshalSnapshotCollectionValueBinary(ascending)
	if err != nil || !ok {
		t.Fatalf("marshalSnapshotCollectionValueBinary(ascending) = ok %t, err %v", ok, err)
	}
	second, ok, err := marshalSnapshotCollectionValueBinary(descending)
	if err != nil || !ok {
		t.Fatalf("marshalSnapshotCollectionValueBinary(descending) = ok %t, err %v", ok, err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("binary map bytes depend on insertion order:\nascending % x\ndescending % x", first, second)
	}
}

func BenchmarkSnapshotValueBinaryMapEncode(b *testing.B) {
	for _, fields := range []int{1, 2, 4, 8, 16, 64} {
		b.Run(fmt.Sprintf("fields-%d", fields), func(b *testing.B) {
			value := make(Map, fields)
			for index := 0; index < fields; index++ {
				value[fmt.Sprintf("field:%02d", index)] = "value"
			}
			payload, ok, err := marshalSnapshotCollectionValueBinary(value)
			if err != nil || !ok {
				b.Fatalf("marshalSnapshotCollectionValueBinary() = ok %t, err %v", ok, err)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(len(payload)), "payload_B/op")
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, ok, err := marshalSnapshotCollectionValueBinary(value); err != nil || !ok {
					b.Fatalf("marshalSnapshotCollectionValueBinary() = ok %t, err %v", ok, err)
				}
			}
		})
	}
}
