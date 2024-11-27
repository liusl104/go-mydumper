package mydumper

func initialize_masquerade(o *OptionEntries) {
	o.global.file_hash = make(map[string]map[string][]string)
}

func finalize_masquerade(o *OptionEntries) {
	o.global.file_hash = nil
}

func get_estimated_remaining_chunks_on_dbt(dbt *db_table) uint64 {
	var total uint64
	for e := dbt.chunks.Front(); e != nil; e = e.Next() {
		total += e.Value.(*chunk_step).integer_step.estimated_remaining_steps
	}
	return total
}
