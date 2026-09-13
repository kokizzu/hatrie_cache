package hatSql

import "testing"

func BenchmarkCH003DryRunNamespaceAdmission(b *testing.B) {
	governor, err := NewNamespaceQueryGovernorWithProfiles(NamespaceResourceProfile{
		Soft: NamespaceResourceLimits{MaxRows: 40, MaxJoinBytes: 400},
		Hard: NamespaceResourceLimits{MaxRows: 100, MaxJoinBytes: 1_000},
	}, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer governor.Close()
	requested := SQLQueryOptions{MaxRows: 500, MaxJoinBytes: 2_000}
	var admission NamespaceQueryAdmission
	var errAdmission error
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		admission, errAdmission = governor.DryRun("benchmark", requested)
	}
	b.StopTimer()
	if errAdmission != nil || admission.EffectiveOptions.MaxRows != 100 || admission.EffectiveOptions.MaxJoinBytes != 1_000 {
		b.Fatalf("admission = %#v, error %v", admission, errAdmission)
	}
}
