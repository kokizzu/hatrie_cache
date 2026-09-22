package hatCache

import "testing"

func BenchmarkT201CommandWriteQuorumResolution(b *testing.B) {
	request := CacheCommandRequest{Command: "SETSTR", Key: "critical:billing:invoice", Value: "value"}
	policy := &WriteQuorumPolicy{Rules: []WriteQuorumRule{
		{KeyPrefix: "critical:", Quorum: 2},
		{KeyPrefix: "critical:billing:", Quorum: 3},
		{KeyPrefix: "ordinary:", Quorum: 1},
	}}
	b.ReportAllocs()
	b.Run("Disabled", func(b *testing.B) {
		options := commandExecutionOptions{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := commandWriteQuorumForRequest(request, options); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ConfiguredMatch", func(b *testing.B) {
		options := commandExecutionOptions{WriteQuorumPolicy: policy}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := commandWriteQuorumForRequest(request, options); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ConfiguredMiss", func(b *testing.B) {
		options := commandExecutionOptions{WriteQuorumPolicy: policy}
		request.Key = "ordinary:profile"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := commandWriteQuorumForRequest(request, options); err != nil {
				b.Fatal(err)
			}
		}
	})
}
