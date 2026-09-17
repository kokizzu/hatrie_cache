package hatCache

import hatSql "hatrie_cache/hat/hatSql"

// ConfigureSQLJSONSubcolumnAutoMaterializer enables the bounded automatic
// JSON subcolumn cache. It is opt-in; a zero options value selects the sane
// limits documented by hatSql.JSONSubcolumnAutoMaterializerOptions.
func (ht *HatTrie) ConfigureSQLJSONSubcolumnAutoMaterializer(options hatSql.JSONSubcolumnAutoMaterializerOptions) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(options)
	if err != nil {
		return err
	}
	ht.sqlIndexMu.Lock()
	ht.sqlJSONSubcolumnAutoMaterializer = materializer
	ht.sqlIndexMu.Unlock()
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, partition := range partitions.tries {
			partition.sqlIndexMu.Lock()
			partition.sqlJSONSubcolumnAutoMaterializer = materializer
			partition.sqlIndexMu.Unlock()
		}
	}
	return nil
}

// DisableSQLJSONSubcolumnAutoMaterializer turns off automatic JSON subcolumn
// materialization and releases all retained typed columns.
func (ht *HatTrie) DisableSQLJSONSubcolumnAutoMaterializer() {
	if ht == nil {
		return
	}
	ht.sqlIndexMu.Lock()
	ht.sqlJSONSubcolumnAutoMaterializer = nil
	ht.sqlIndexMu.Unlock()
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, partition := range partitions.tries {
			partition.sqlIndexMu.Lock()
			partition.sqlJSONSubcolumnAutoMaterializer = nil
			partition.sqlIndexMu.Unlock()
		}
	}
}

// SQLJSONSubcolumnAutoMaterializerStats reports the bounded cache lifecycle
// counters without exposing source values or query text.
func (ht *HatTrie) SQLJSONSubcolumnAutoMaterializerStats() hatSql.JSONSubcolumnAutoMaterializerStats {
	if ht == nil {
		return hatSql.JSONSubcolumnAutoMaterializerStats{}
	}
	ht.sqlIndexMu.RLock()
	materializer := ht.sqlJSONSubcolumnAutoMaterializer
	ht.sqlIndexMu.RUnlock()
	if materializer != nil {
		return materializer.Stats()
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		var stats hatSql.JSONSubcolumnAutoMaterializerStats
		for _, partition := range partitions.tries {
			partition.sqlIndexMu.RLock()
			materializer = partition.sqlJSONSubcolumnAutoMaterializer
			partition.sqlIndexMu.RUnlock()
			if materializer == nil {
				continue
			}
			partStats := materializer.Stats()
			stats.Entries += partStats.Entries
			stats.Observations += partStats.Observations
			stats.Promotions += partStats.Promotions
			stats.Rejections += partStats.Rejections
			stats.Evictions += partStats.Evictions
			stats.Hits += partStats.Hits
			stats.RetainedBytes += partStats.RetainedBytes
		}
		return stats
	}
	return hatSql.JSONSubcolumnAutoMaterializerStats{}
}

// ResolveSQLColumnarJSONSubcolumns supplies the opt-in automatic typed JSON
// subcolumn layout to the SQL executor. Cold or disabled paths return
// available=false so the existing row-oriented executor keeps semantics.
func (ht *HatTrie) ResolveSQLColumnarJSONSubcolumns(name, key string, fields []string, paths []hatSql.ColumnarJSONSubcolumnRequest) (hatSql.ColumnarBatch, *hatSql.ColumnarNumericSegments, bool, error) {
	if ht == nil {
		return hatSql.ColumnarBatch{}, nil, false, ErrNilHatTrie
	}
	if name != "CACHE" || len(paths) == 0 {
		return hatSql.ColumnarBatch{}, nil, false, nil
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ResolveSQLColumnarJSONSubcolumns(name, key, fields, paths)
	}
	ht.sqlIndexMu.RLock()
	materializer := ht.sqlJSONSubcolumnAutoMaterializer
	ht.sqlIndexMu.RUnlock()
	if materializer == nil {
		return hatSql.ColumnarBatch{}, nil, false, nil
	}

	// Register before reading so every later write advances the generation used
	// to reject a promoted column from an older JSON snapshot.
	ht.registerSQLJSONIndexSource(key)
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return hatSql.ColumnarBatch{}, nil, false, err
	}
	ht.sqlIndexMu.RLock()
	admitted := ht.sqlJSONIndexSourceAdmittedLocked(source)
	ht.sqlIndexMu.RUnlock()
	if !admitted {
		return hatSql.ColumnarBatch{}, nil, false, nil
	}
	autoSource := hatSql.JSONSubcolumnAutoSource{
		SourceName: name,
		SourceKey:  key,
		Generation: source.generation,
	}
	promoted, available, err := materializer.ResolvePromotedBatch(autoSource, paths)
	if err != nil || !available {
		if err != nil {
			return hatSql.ColumnarBatch{}, nil, false, err
		}
		promoted = hatSql.ColumnarBatch{}
	}
	if available {
		if len(fields) == 0 {
			return promoted, nil, true, nil
		}
		base, err := sqlJSONColumnarBatchString(key, source.raw, fields)
		if err != nil {
			return hatSql.ColumnarBatch{}, nil, false, err
		}
		if base.Rows != promoted.Rows {
			return hatSql.ColumnarBatch{}, nil, false, nil
		}
		base.JSONSubcolumns = promoted.JSONSubcolumns
		return base, nil, true, nil
	}
	ready, err := materializer.ObserveRequests(autoSource, paths)
	if err != nil {
		return hatSql.ColumnarBatch{}, nil, false, err
	}
	if !ready {
		return hatSql.ColumnarBatch{}, nil, false, nil
	}

	rows, err := sqlJSONRowsString(key, source.raw)
	if err != nil {
		return hatSql.ColumnarBatch{}, nil, false, err
	}
	batch, available, err := materializer.ResolveBatch(autoSource, fields, paths, rows)
	return batch, nil, available, err
}
