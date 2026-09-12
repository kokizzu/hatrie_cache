package hatCache

import "hatrie_cache/hat/hatSql"

type SQLQuotaLimits = hatSql.SQLQuotaLimits
type SQLQuotaRegistryOptions = hatSql.SQLQuotaRegistryOptions
type SQLQuotaRegistry = hatSql.SQLQuotaRegistry
type SQLQuotaReservation = hatSql.SQLQuotaReservation

var ErrSQLQuotaExceeded = hatSql.ErrSQLQuotaExceeded
var ErrSQLQuotaKeysExceeded = hatSql.ErrSQLQuotaKeysExceeded

func NewSQLQuotaRegistry(options SQLQuotaRegistryOptions) (*SQLQuotaRegistry, error) {
	return hatSql.NewSQLQuotaRegistry(options)
}
