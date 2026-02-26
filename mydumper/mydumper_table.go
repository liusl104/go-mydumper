package mydumper

import (
	"container/list"
	"fmt"
	"strings"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

// initialize_table initializes all_dbts_mutex, character_set_hash_mutex, and character_set_hash.
func initialize_table() {
	all_dbts_mutex = G_mutex_new()
	character_set_hash_mutex = G_mutex_new()
	character_set_hash = make(map[string]string)
}

// finalize_table clears character_set_hash and related mutexes.
func finalize_table() {
	character_set_hash = nil
	all_dbts_mutex = nil
	character_set_hash_mutex = nil
}

// free_db_table resets dbt chunk-related state and clears insert_statement, select_fields, etc.
func free_db_table(dbt *db_table) {
	dbt.chunks_mutex.Lock()
	dbt.rows_lock = nil
	dbt.escaped_table = ""
	dbt.insert_statement = nil
	dbt.select_fields = ""
	dbt.min = ""
	dbt.max = ""
	dbt.data_checksum = ""
	dbt.chunks_completed = 0
	dbt.chunks_mutex.Unlock()
	dbt = nil
}

// get_character_set_from_collation returns the character set name for the given collation (cached in character_set_hash).
func get_character_set_from_collation(conn *DBConnection, collation string) string {
	character_set_hash_mutex.Lock()
	character_set, _ := character_set_hash[collation]
	if character_set == "" {
		query := fmt.Sprintf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE collation_name='%s'", collation)
		var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Failed to get CHARACTER_SET from collation %s", collation)
		if mr.Row != nil {
			character_set = string(mr.Row[0].AsString())
			character_set_hash[collation] = character_set
		}
		M_store_result_row_free(mr)
	}
	character_set_hash_mutex.Unlock()
	return character_set
}

// get_primary_key fills dbt.primary_key from SHOW INDEX (PRIMARY, or first non-subpart column, or use_any_index).
func get_primary_key(conn *DBConnection, dbt *db_table, conf *Configuration) {
	var indexes *M_ROW
	var row []FieldValue
	var query string = fmt.Sprintf("SHOW INDEX FROM %s%s%s.%s%s%s", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
	indexes = M_store_result_row(conn, query, M_warning, nil, "Failed to execute SHOW INDEX over %s", dbt.database.name)
	if indexes != nil {
		for _, row = range indexes.Res.FieldValues {
			if strings.EqualFold(row[2].String(), "PRIMARY") {
				dbt.primary_key = append(dbt.primary_key, string(row[4].AsString()))
			}
		}
		if dbt.primary_key != nil {
			return
		}
		for _, row = range indexes.Res.FieldValues {
			if strings.EqualFold(string(row[1].AsString()), "0") {
				dbt.primary_key = append(dbt.primary_key, string(row[4].AsString()))
			}
		}
		if dbt.primary_key != nil {
			return
		}
		if len(dbt.primary_key) == 0 && conf.use_any_index != "" {
			var max_cardinality uint64
			var cardinality uint64
			var field string
			for _, row = range indexes.Res.FieldValues {
				if row[3].AsUint64() == 1 {
					if row[6].Value() != nil {
						cardinality = row[6].AsUint64()
					}
					if cardinality > max_cardinality {
						field = string(row[4].AsString())
						max_cardinality = cardinality
					}
				}
			}
			if field != "" {
				dbt.primary_key = append(dbt.primary_key, field)
			}
		}
	}

}

// get_primary_key_separated_by_comma builds a comma-separated quoted list of primary key columns and stores it in dbt.primary_key_separated_by_comma.
func get_primary_key_separated_by_comma(dbt *db_table) {
	var field_list string
	var list = dbt.primary_key
	var first = true
	for _, row := range list {
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name = identifier_quote_character_protect(row)
		var tb = fmt.Sprintf("%s%s%s", Identifier_quote_character_str, field_name, Identifier_quote_character_str)
		field_list += tb
	}
	if field_list != "" {
		dbt.primary_key_separated_by_comma = field_list
	}
}

// get_selectable_fields returns a comma-separated list of column names (excluding VIRTUAL/STORED generated) for the table.
func get_selectable_fields(conn *DBConnection, database string, table string) string {
	var field_list string
	var query string = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%' ORDER BY ORDINAL_POSITION ASC", database, table)
	var res = M_store_result_critical(conn, query, "Failed to get Selectable Fields")
	var first = true
	for {
		row := Mysql_fetch_row(res)
		if row == nil {
			break
		}
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name = identifier_quote_character_protect(string(row[0].AsString()))
		var tb = fmt.Sprintf("%s%s%s", Identifier_quote_character_str, field_name, Identifier_quote_character_str)
		field_list += tb
	}
	Mysql_free_result(res)
	return field_list
}

// detect_generated_fields returns true if the table has generated columns (unless IgnoreGeneratedFields).
func detect_generated_fields(conn *DBConnection, database string, table string) bool {
	var result bool
	var query string
	if IgnoreGeneratedFields {
		return false
	}
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%' and extra not like '%%DEFAULT_GENERATED%%'", database, table)
	var mr *M_ROW = M_store_result_row(conn, query, M_warning, M_message, "Failed to detect Generated Fields")
	result = mr.Row != nil
	M_store_result_row_free(mr)
	return result
}

// has_json_fields returns true if the table has any JSON column.
func has_json_fields(conn *DBConnection, database string, table string) bool {
	var query string
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_TYPE ='json'", database, table)
	var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Failed to get JSON fields on %s.%s: %s", database, table, query)
	if mr.Row != nil {
		M_store_result_row_free(mr)
		return true
	}
	M_store_result_row_free(mr)
	return false
}

// new_db_table creates or reuses a db_table for the given database/table, registers it in all_dbts, and returns true if newly created.
func new_db_table(d **db_table, conn *DBConnection, conf *Configuration, database *database, table string, table_collation string, is_sequence bool) bool {
	var b bool
	var lkey = Build_dbt_key(database.name, table)
	all_dbts_mutex.Lock()
	var dbt *db_table
	dbt = all_dbts[lkey]
	if dbt != nil {
		b = false
		all_dbts_mutex.Unlock()
	} else {
		dbt = new(db_table)
		dbt.key = lkey
		dbt.object_to_export = new(Object_to_export)
		dbt.status = UNDEFINED
		all_dbts[lkey] = dbt
		all_dbts_mutex.Unlock()
		dbt.database = database
		dbt.table = identifier_quote_character_protect(table)
		dbt.table_filename = get_ref_table(dbt.table)
		dbt.is_sequence = is_sequence
		if table_collation == "" {
			dbt.character_set = ""
		} else {
			dbt.character_set = get_character_set_from_collation(conn, table_collation)
		}
		dbt.has_json_fields = has_json_fields(conn, dbt.database.name, dbt.table)
		dbt.rows_lock = G_mutex_new()
		dbt.rows_total = 0
		dbt.escaped_table = escape_string(dbt.table)
		dbt.where = conf_per_table.All_where_per_table[lkey]
		dbt.limit = conf_per_table.All_limit_per_table[lkey]
		Parse_object_to_export(dbt.object_to_export, conf_per_table.All_object_to_export[lkey])
		dbt.partition_regex = conf_per_table.All_partition_regex_per_table[lkey]
		dbt.max_threads_per_table = MaxThreadsPerTable
		dbt.current_threads_running = 0
		var rows_p_chunk = conf_per_table.All_rows_per_table[lkey]
		if rows_p_chunk != "" {
			dbt.split_integer_tables = parse_rows_per_chunk(rows_p_chunk, &(dbt.min_chunk_step_size), &(dbt.starting_chunk_step_size), &(dbt.max_chunk_step_size), "Invalid option on rows in Configuration file")
		} else {
			dbt.split_integer_tables = split_integer_tables
			dbt.min_chunk_step_size = min_chunk_step_size
			dbt.starting_chunk_step_size = starting_chunk_step_size
			dbt.max_chunk_step_size = max_chunk_step_size
		}
		if dbt.min_chunk_step_size == 1 && dbt.min_chunk_step_size == dbt.starting_chunk_step_size && dbt.starting_chunk_step_size != dbt.max_chunk_step_size {
			dbt.min_chunk_step_size = 2
			dbt.starting_chunk_step_size = 2
			log.Warnf("Setting min and start rows per file to 2 on %s", lkey)
		}
		dbt.is_fixed_length = dbt.min_chunk_step_size != 0 && dbt.min_chunk_step_size == dbt.starting_chunk_step_size && dbt.starting_chunk_step_size == dbt.max_chunk_step_size
		if dbt.is_fixed_length {
			dbt.chunk_filesize = 0
		} else {
			dbt.chunk_filesize = ChunkFilesize
		}
		if dbt.min_chunk_step_size == 0 {
			dbt.min_chunk_step_size = MIN_CHUNK_STEP_SIZE
		}
		n, ok := conf_per_table.All_num_threads_per_table[lkey]
		if ok {
			dbt.num_threads = n
		} else {
			dbt.num_threads = NumThreads
		}
		if max_integer_chunk_step_size != 0 && (dbt.max_chunk_step_size > max_integer_chunk_step_size || dbt.max_chunk_step_size == 0) {
			dbt.max_chunk_step_size = max_integer_chunk_step_size
		}
		if min_integer_chunk_step_size != 0 && (dbt.min_chunk_step_size < min_integer_chunk_step_size) {
			dbt.min_chunk_step_size = min_integer_chunk_step_size
		}
		if _, ok = conf_per_table.All_num_threads_per_table[lkey]; ok {
			dbt.num_threads = conf_per_table.All_num_threads_per_table[lkey]
		} else {
			dbt.num_threads = NumThreads
		}
		dbt.estimated_remaining_steps = 1
		dbt.min = ""
		dbt.max = ""
		dbt.chunks = new(list.List)
		dbt.load_data_header = nil
		dbt.load_data_suffix = nil
		dbt.insert_statement = nil
		dbt.chunks_mutex = G_mutex_new()
		dbt.chunks_queue = G_async_queue_new("dbt.chunks_queue")
		dbt.chunks_completed = 0
		get_primary_key(conn, dbt, conf)
		dbt.primary_key_separated_by_comma = ""
		if OrderByPrimaryKey {
			get_primary_key_separated_by_comma(dbt)
		}
		dbt.multicolumn = !UseSingleColumn && len(dbt.primary_key) > 1
		var columns_on_select = conf_per_table.All_columns_on_select_per_table[lkey]
		dbt.columns_on_insert = conf_per_table.All_columns_on_insert_per_table[lkey]
		dbt.select_fields = ""
		if columns_on_select != "" {
			dbt.select_fields = columns_on_select
		} else if dbt.columns_on_insert == "" {
			dbt.complete_insert = CompleteInsert || detect_generated_fields(conn, dbt.database.escaped, dbt.escaped_table)
			if dbt.complete_insert {
				dbt.select_fields = get_selectable_fields(conn, database.escaped, dbt.escaped_table)
			}
		}
		dbt.anonymized_function = nil
		dbt.indexes_checksum = ""
		dbt.data_checksum = ""
		dbt.schema_checksum = ""
		dbt.triggers_checksum = ""
		dbt.rows = 0
		b = true
	}
	*d = dbt
	return b
}
