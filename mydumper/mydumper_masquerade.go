package mydumper

import . "go-mydumper/src"

var (
	file_hash map[string]map[string][]string
	pp        *Function_pointer
)

func initialize_masquerade() {
	file_hash = make(map[string]map[string][]string)
}
func identity_function(s string) {

}

func finalize_masquerade() {
	file_hash = nil
}

func get_estimated_remaining_chunks_on_dbt(dbt *DB_Table) uint64 {
	var total uint64
	for e := dbt.chunks.Front(); e != nil; e = e.Next() {
		total += e.Value.(*chunk_step).integer_step.estimated_remaining_steps
	}
	return total
}
