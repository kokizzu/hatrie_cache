package hatFiber

import "errors"

var (
	// ErrTenantNameInvalid reports an empty tenant name in configuration.
	ErrTenantNameInvalid = errors.New("hatFiber: tenant name is invalid")
	// ErrTenantQuotaInvalid reports a negative tenant quota.
	ErrTenantQuotaInvalid = errors.New("hatFiber: tenant quota is invalid")
	// ErrTenantQuotaExceeded reports that a tenant has reached its retained
	// fiber limit.
	ErrTenantQuotaExceeded = errors.New("hatFiber: tenant fiber quota exceeded")
)

// TenantQuota bounds one named tenant. MaxFibers counts spawned fibers until
// Reap, including terminal fibers retained for inspection. MaxStepsPerRun is
// a cooperative callback budget for one Scheduler.Run call; zero is unlimited.
type TenantQuota struct {
	MaxFibers      int
	MaxStepsPerRun uint64
}

func cloneTenantQuotas(input map[string]TenantQuota) (map[string]TenantQuota, bool, error) {
	if len(input) == 0 {
		return nil, false, nil
	}
	quotas := make(map[string]TenantQuota, len(input))
	hasStepQuota := false
	for tenant, quota := range input {
		if tenant == "" {
			return nil, false, ErrTenantNameInvalid
		}
		if quota.MaxFibers < 0 {
			return nil, false, ErrTenantQuotaInvalid
		}
		if quota.MaxStepsPerRun > 0 {
			hasStepQuota = true
		}
		quotas[tenant] = quota
	}
	return quotas, hasStepQuota, nil
}

func (scheduler *Scheduler) tenantStepQuotaExhausted(tenant string, steps map[string]uint64) bool {
	quota, ok := scheduler.tenantQuotas[tenant]
	return ok && quota.MaxStepsPerRun > 0 && steps[tenant] >= quota.MaxStepsPerRun
}

func recordTenantStep(scheduler *Scheduler, tenant string, steps map[string]uint64) {
	if steps == nil {
		return
	}
	if quota, ok := scheduler.tenantQuotas[tenant]; ok && quota.MaxStepsPerRun > 0 {
		steps[tenant]++
	}
}
