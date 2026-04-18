package mydumper

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	ChunkFilesize            uint
	LoadData                 bool
	Csv                      bool
	OutputFormat             string
	IncludeHeader            bool
	FieldsTerminatedByLd     string
	FieldsEnclosedByLd       string
	FieldsEscapedBy          string
	LinesStartingByLd        string
	LinesTerminatedByLd      string
	StatementTerminatedByLd  string
	InsertIgnore             bool
	Replace                  bool
	CompleteInsert           bool
	HexBlob                  bool
	StatementSize            int = 1000000
	clickhouse               bool
	fields_enclosed_by       string
	fields_terminated_by     string
	lines_terminated_by      string
	lines_starting_by        string
	statement_terminated_by  string
	insert_statement         string = INSERT
	message_dumping_data     func(tj *table_job)
	max_statement_size_mutex *sync.Mutex
	row_delimiter            string
	max_statement_size       int
)

const LOAD_DATA_PREFIX = "LOAD DATA LOCAL INFILE '"

// update_files_on_table_job opens rows (and optionally sql) files for the table job if not yet open; returns true if new files were opened.
func update_files_on_table_job(tj *table_job) bool {
	var err error
	if tj.rows.file == nil {
		tj.rows.filename = build_rows_filename(tj.dbt.database.filename, tj.dbt.table_filename, tj.part, tj.sub_part)
		tj.rows.file, err = m_open(&tj.rows.filename, "w")
		log.Tracef("Thread %d: Filename assigned(%v): %s", tj.td.thread_id, err, tj.rows.filename)
		if tj.sql != nil {
			tj.sql.filename = build_sql_filename(tj.dbt.database.filename, tj.dbt.table_filename, tj.part, tj.sub_part)
			tj.sql.file, err = m_open(&tj.sql.filename, "w")
			log.Tracef("Thread %d: Filename assigned: %s", tj.td.thread_id, tj.sql.filename)
			if err != nil {
				log.Criticalf("open file %s fail: %v", tj.sql.filename, err)
				Errors++
				return false
			}
			return true
		}
	}
	return false
}

// message_dumping_data_short logs a short progress line (database.table and percentage).
func message_dumping_data_short(tj *table_job) {
	transactional_table.mutex.Lock()
	var transactional_table_size int = transactional_table.list.Len()
	transactional_table.mutex.Unlock()
	non_transactional_table.mutex.Lock()
	var non_transactional_table_size int = non_transactional_table.list.Len()
	non_transactional_table.mutex.Unlock()
	var db string
	var total uint64 = 0
	if tj.dbt.rows_total != 0 {
		total = 100 * tj.dbt.rows / tj.dbt.rows_total
	}
	if masquerade_filename {
		db = tj.dbt.database.filename
	} else {
		db = tj.dbt.database.name
	}
	var tb string
	if masquerade_filename {
		tb = tj.dbt.table_filename
	} else {
		tb = tj.dbt.table
	}
	log.Infof("Thread %d: %s%s%s.%s%s%s [ %d%% ] | Tables: %d/%d",
		tj.td.thread_id,
		Identifier_quote_character_str, db, Identifier_quote_character_str, Identifier_quote_character_str,
		tb, Identifier_quote_character_str,
		total,
		transactional_table_size+non_transactional_table_size, len(all_dbts))
}

// message_dumping_data_long logs a detailed progress line including partition, WHERE, ORDER BY and output filename.
func message_dumping_data_long(tj *table_job) {
	transactional_table.mutex.Lock()
	var transactional_table_size int = transactional_table.list.Len()
	transactional_table.mutex.Unlock()
	non_transactional_table.mutex.Lock()
	var non_transactional_table_size int = non_transactional_table.list.Len()
	non_transactional_table.mutex.Unlock()
	var total uint64 = 0
	if tj.dbt.rows_total != 0 {
		total = 100 * tj.dbt.rows / uint64(tj.dbt.rows_total)
	}
	var partition_opt, partition_val string
	var where_opt, where_val string
	var where_and_opt, where_and_val string
	var where_and_opt_1, where_and_val_1 string
	var order_by, order_by_val string
	if tj.partition != "" {
		partition_opt = " "
		partition_val = tj.partition
	}
	if tj.where != nil {
		if tj.where.Len > 0 || WhereOption != "" || tj.dbt.where != "" {
			where_opt = " WHERE "
		}
		if tj.where.Len > 0 {
			where_val = tj.where.Str.String()
		}
		if tj.where.Len > 0 && WhereOption != "" {
			where_and_opt = " AND "
		}
		if (tj.where.Len > 0 || WhereOption != "") && tj.dbt.where != "" {
			where_and_opt_1 = " AND "
		}
		if tj.dbt.where != "" {
			where_and_val_1 = tj.dbt.where
		}
	}

	if WhereOption != "" {
		where_and_val = WhereOption
	}

	if OrderByPrimaryKey && tj.dbt.primary_key_separated_by_comma != "" {
		order_by = " ORDER BY "
		order_by_val = tj.dbt.primary_key_separated_by_comma
	}
	var db, tb string
	if masquerade_filename {
		db = tj.dbt.database.filename
		tb = tj.dbt.table_filename
	} else {
		db = tj.dbt.database.name
		tb = tj.dbt.table
	}
	log.Infof("Thread %d: dumping data from %s%s%s.%s%s%s%s%s%s%s%s%s%s%s%s%s into %s | Completed: %d%% | Remaining tables: %d / %d",
		tj.td.thread_id,
		Identifier_quote_character_str, db, Identifier_quote_character_str, Identifier_quote_character_str,
		tb, Identifier_quote_character_str,
		partition_opt, partition_val,
		where_opt, where_val,
		where_and_opt, where_and_val,
		where_and_opt_1, where_and_val_1,
		order_by, order_by_val,
		tj.rows.filename, total,
		non_transactional_table_size+transactional_table_size, len(all_dbts))
}

// initialize_write sets message_dumping_data, validates options, and initializes format-specific fields (enclosed/terminated by, row_delimiter, insert_statement).
func initialize_write() {
	if Verbose > 3 {
		message_dumping_data = message_dumping_data_long
	} else {
		message_dumping_data = message_dumping_data_short
	}

	if starting_chunk_step_size > 0 && ChunkFilesize > 0 {
		log.Warnf("We are going to chunk by row and by filesize when possible")
	}
	G_assert(fields_enclosed_by != "")
	if FieldsEnclosedByLd == "" && len(FieldsEnclosedByLd) > 1 {
		M_critical("--fields-enclosed-by must be a single character")
	}
	if FieldsEscapedBy == "" && len(FieldsEscapedBy) > 1 {
		M_critical("--fields-enclosed-by must be a single character")
	}
	max_statement_size_mutex = G_mutex_new()
	switch output_format {
	case CLICKHOUSE, SQL_INSERT:
		if FieldsEnclosedByLd != "" {
			fields_enclosed_by = FieldsEnclosedByLd
		}
		if FieldsTerminatedByLd == "" {
			fields_terminated_by = ","
		} else if strings.Compare(FieldsTerminatedByLd, "\\t") == 0 {
			fields_terminated_by = "\t"
		} else {
			fields_terminated_by = Replace_escaped_strings(FieldsTerminatedByLd)
		}
		if LinesStartingByLd == "" {
			lines_starting_by = "("
		} else {
			lines_starting_by = Replace_escaped_strings(LinesStartingByLd)
		}
		if LinesTerminatedByLd == "" {
			lines_terminated_by = ")\n"
		} else {
			lines_terminated_by = Replace_escaped_strings(LinesTerminatedByLd)
		}
		if StatementTerminatedByLd == "" {
			statement_terminated_by = ";\n"
		} else {
			statement_terminated_by = Replace_escaped_strings(StatementTerminatedByLd)
		}
		row_delimiter = ","
		break
	case LOAD_DATA:
		if FieldsEnclosedByLd == "" {
			fields_enclosed_by = ""
			FieldsEnclosedByLd = fields_enclosed_by
		} else {
			fields_enclosed_by = FieldsEnclosedByLd
		}
		if FieldsEscapedBy != "" {
			if strings.Compare(FieldsEscapedBy, "\\") == 0 {
				FieldsEscapedBy = "\\\\"
			}
		} else {
			FieldsEscapedBy = "\\\\"
		}
		if FieldsTerminatedByLd == "" {
			fields_terminated_by = "\t"
			FieldsTerminatedByLd = "\\t"
		} else if strings.Compare(FieldsTerminatedByLd, "\\t") == 0 {
			fields_terminated_by = "\t"
			FieldsTerminatedByLd = "\\t"
		} else {
			fields_terminated_by = Replace_escaped_strings(FieldsTerminatedByLd)
		}
		if LinesStartingByLd == "" {
			LinesStartingByLd = ""
			LinesStartingByLd = lines_starting_by
		} else {
			lines_starting_by = Replace_escaped_strings(LinesStartingByLd)
		}
		if LinesTerminatedByLd == "" {
			lines_terminated_by = "\n"
			LinesTerminatedByLd = "\\n"
		} else {
			lines_terminated_by = Replace_escaped_strings(LinesTerminatedByLd)
		}
		if StatementTerminatedByLd == "" {
			statement_terminated_by = ""
			StatementTerminatedByLd = statement_terminated_by
		} else {
			statement_terminated_by = Replace_escaped_strings(StatementTerminatedByLd)
		}
		row_delimiter = ""
		break
	case CSV:
		if FieldsEnclosedByLd == "" {
			fields_enclosed_by = "\""
			FieldsEnclosedByLd = fields_enclosed_by
		} else {
			fields_enclosed_by = FieldsEnclosedByLd
		}
		if FieldsEscapedBy != "" {
			if strings.Compare(FieldsEscapedBy, "\\") == 0 {
				FieldsEscapedBy = "\\\\"
			}
		} else {
			FieldsEscapedBy = "\\\\"
		}
		if FieldsTerminatedByLd == "" {
			fields_terminated_by = ","
			FieldsTerminatedByLd = fields_terminated_by
		} else if strings.Compare(FieldsTerminatedByLd, "\\t") == 0 {
			fields_terminated_by = "\t"
			FieldsTerminatedByLd = "\\t"
		} else {
			fields_terminated_by = Replace_escaped_strings(FieldsTerminatedByLd)
		}
		if LinesStartingByLd == "" {
			lines_starting_by = ""
			LinesStartingByLd = lines_starting_by
		} else {
			lines_starting_by = Replace_escaped_strings(LinesStartingByLd)
		}
		if LinesTerminatedByLd == "" {
			lines_terminated_by = "\n"
			LinesTerminatedByLd = "\\n"
		} else {
			lines_terminated_by = Replace_escaped_strings(LinesTerminatedByLd)
		}
		if StatementTerminatedByLd == "" {
			statement_terminated_by = ""
			StatementTerminatedByLd = statement_terminated_by
		} else {
			statement_terminated_by = Replace_escaped_strings(StatementTerminatedByLd)
		}
		row_delimiter = ""
		break
	}

	if InsertIgnore && Replace {
		log.Errorf("You can't use --insert-ignore_engines and --replace at the same time")
	}

	if InsertIgnore {
		insert_statement = INSERT_IGNORE
	}
	if Replace {
		insert_statement = REPLACE
	}
}

// finalize_write clears format-specific string variables.
func finalize_write() {
	fields_terminated_by = ""
	lines_starting_by = ""
	lines_terminated_by = ""
	statement_terminated_by = ""
}

// is_hex_blob returns true if HexBlob is set and the column type is BLOB, VAR_STRING, or STRING.
func is_hex_blob(field *sql.ColumnType) bool {
	return HexBlob && (IsMySQLType(GetStandardType(field.DatabaseTypeName()), "MYSQL_TYPE_BLOB") || (IsMySQLType(GetStandardType(field.DatabaseTypeName()), "MYSQL_TYPE_VAR_STRING") || IsMySQLType(GetStandardType(field.DatabaseTypeName()), "MYSQL_TYPE_STRING")))
}

// append_load_data_columns appends quoted column names to statement and returns a SET clause for JSON/hex columns if needed.
func append_load_data_columns(statement *GString, fields []*sql.ColumnType, num_fields uint) *GString {
	var i uint
	var str = G_string_new("SET ")
	var appendable bool
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			G_string_append_c(statement, ',')
		}
		if IsMySQLType(GetStandardType(fields[i].Name()), "MYSQL_TYPE_JSON") {
			G_string_append_c(statement, '@')
			G_string_append(statement, fields[i].Name())
			if str.Len > 4 {
				G_string_append(str, ",")
			}
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(str, fields[i].Name())
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(str, "=CONVERT(@")
			G_string_append(str, fields[i].Name())
			G_string_append(str, " USING UTF8MB4)")
			appendable = true
		} else if is_hex_blob(fields[i]) {
			G_string_append_c(statement, '@')
			G_string_append(statement, fields[i].Name())
			if str.Len > 4 {
				G_string_append_c(str, ',')
			}
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(statement, fields[i].Name())
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(str, "=UNHEX(@")
			G_string_append(statement, fields[i].Name())
			G_string_append(statement, ")")
			appendable = true
		} else {
			G_string_append(statement, Identifier_quote_character_str)
			G_string_append(statement, fields[i].Name())
			G_string_append(statement, Identifier_quote_character_str)
		}
	}
	if appendable {
		return str
	} else {
		return nil
	}
}

// append_columns appends quoted column names to statement, comma-separated.
func append_columns(statement *GString, fields []*sql.ColumnType, num_fields uint) {
	var i uint
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			G_string_append(statement, ",")
		}
		G_string_append(statement, Identifier_quote_character)
		G_string_append(statement, fields[i].Name())
		G_string_append(statement, Identifier_quote_character)
	}

}

// set_anonymized_function_list fills dbt.anonymized_function from conf_per_table for each column (or identity).
func set_anonymized_function_list(dbt *db_table, fields []*sql.ColumnType, num_fields uint) {
	var db string = dbt.database.name
	var table string = dbt.table
	var k string = fmt.Sprintf("`%s`.`%s`", db, table)
	var ht map[string]*Function_pointer = conf_per_table.All_anonymized_function[k]
	var anonymized_function_list []*Function_pointer
	if ht != nil {
		anonymized_function_list = make([]*Function_pointer, num_fields)
		var i uint = 0
		var fp *Function_pointer
		for i = 0; i < num_fields; i++ {
			fp = ht[fields[i].Name()]
			if fp != nil {
				log.Infof("Masquerade function found on `%s`.`%s`.`%s`", db, table, fields[i].Name())
				anonymized_function_list[i] = fp
			} else {
				anonymized_function_list[i] = identity_function_pointer
			}
		}
		dbt.anonymized_function = anonymized_function_list
	}
}

// build_insert_statement builds the INSERT/INSERT IGNORE/REPLACE INTO ... (columns) VALUES prefix and stores it in dbt.insert_statement.
func build_insert_statement(dbt *db_table, fields []*sql.ColumnType, num_fields uint) {
	var i_s = G_string_new(insert_statement)
	G_string_append(i_s, " INTO ")
	G_string_append(i_s, Identifier_quote_character)
	G_string_append(i_s, dbt.table)
	G_string_append(i_s, Identifier_quote_character)
	set_anonymized_function_list(dbt, fields, num_fields)
	if dbt.columns_on_insert != "" {
		G_string_append(i_s, " (")
		G_string_append(i_s, dbt.columns_on_insert)
		G_string_append(i_s, ")")
	} else {
		if dbt.complete_insert {
			G_string_append(i_s, " (")
			append_columns(i_s, fields, num_fields)
			G_string_append(i_s, ")")
		}
	}
	G_string_append(i_s, " VALUES ")
	dbt.insert_statement = i_s
}

// real_write_data writes data to file, updates filesize, and returns false on write error.
func real_write_data(file *file_write, filesize *float64, data *GString) bool {
	var written int
	var r int
	var err error
	var second_write_zero bool
	for written < data.Len {
		r, err = file.write([]byte(data.Str.String()))
		if err != nil {
			log.Criticalf("Couldn't write data to a file: %v", err)
			Errors++
			return false
		}
		if r == 0 {
			if second_write_zero {
				log.Criticalf("Couldn't write data to a file: %v", err)
				Errors++
				return false
			}
			second_write_zero = true
		} else {
			second_write_zero = false
		}
		written += r
	}

	*filesize += float64(written)
	return true
}

// write_data writes data to file (ignores size); returns false on error.
func write_data(file *file_write, data *GString) bool {
	var f float64
	return real_write_data(file, &f, data)
}

// initialize_load_data_statement_suffix builds the LOAD DATA LOCAL INFILE ... INTO TABLE ... FIELDS/LINES clause and stores it in dbt.load_data_suffix.
func initialize_load_data_statement_suffix(dbt *db_table, fields []*sql.ColumnType, num_fields uint) {
	var character_set string
	if Set_names_in_conn_by_default != "" {
		character_set = Set_names_in_conn_by_default
	} else {
		character_set = dbt.character_set
	}
	var load_data_suffix = G_string_sized_new(StatementSize)
	G_string_append_printf(load_data_suffix, "%s' INTO TABLE %s%s%s ", ExecPerThreadExtension, Identifier_quote_character_str, dbt.table,
		Identifier_quote_character_str)
	if character_set != "" && len(character_set) != 0 {
		G_string_append_printf(load_data_suffix, "CHARACTER SET %s ", character_set)
	}
	if FieldsTerminatedByLd != "" {
		G_string_append_printf(load_data_suffix, "FIELDS TERMINATED BY '%s' ", FieldsTerminatedByLd)
	}
	if FieldsEnclosedByLd != "" {
		G_string_append_printf(load_data_suffix, "ENCLOSED BY '%s' ", FieldsEnclosedByLd)
	}
	if FieldsEscapedBy != "" {
		G_string_append_printf(load_data_suffix, "ESCAPED BY '%s' ", FieldsEscapedBy)
	}
	G_string_append(load_data_suffix, "LINES ")
	if LinesStartingByLd != "" {
		G_string_append_printf(load_data_suffix, "STARTING BY '%s' ", LinesStartingByLd)
	}
	G_string_append_printf(load_data_suffix, "TERMINATED BY '%s' ", LinesTerminatedByLd)
	if IncludeHeader {
		G_string_append(load_data_suffix, "IGNORE 1 LINES ")
	}
	G_string_append(load_data_suffix, "(")
	if dbt.columns_on_insert != "" {
		G_string_append(load_data_suffix, dbt.columns_on_insert)
		G_string_append(load_data_suffix, ")")
	} else {
		var set_statement = append_load_data_columns(load_data_suffix, fields, num_fields)
		G_string_append(load_data_suffix, ")")
		if set_statement != nil {
			G_string_append(load_data_suffix, set_statement.Str.String())
		}
	}
	G_string_append(load_data_suffix, ";\n")
	dbt.load_data_suffix = load_data_suffix
}

// initialize_clickhouse_statement_suffix builds the ClickHouse LOAD DATA INTO TABLE ... clause and stores it in dbt.load_data_suffix.
func initialize_clickhouse_statement_suffix(dbt *db_table, fields []*sql.ColumnType, num_fields uint) {
	var character_set string
	if Set_names_in_conn_by_default != "" {
		character_set = Set_names_in_conn_by_default
	} else {
		character_set = dbt.character_set
	}
	dbt.insert_statement = G_string_sized_new(StatementSize)
	G_string_append_printf(dbt.load_data_suffix, "%s' INTO TABLE %s%s%s ", ExecPerThreadExtension, Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
	if character_set != "" && len(character_set) != 0 {
		G_string_append_printf(dbt.load_data_suffix, "CHARACTER SET %s ", character_set)
	}
	if FieldsTerminatedByLd != "" {
		G_string_append_printf(dbt.load_data_suffix, "FIELDS TERMINATED BY '%s' ", FieldsTerminatedByLd)
	}
	if FieldsEnclosedByLd != "" {
		G_string_append_printf(dbt.load_data_suffix, "ENCLOSED BY '%s' ", FieldsEnclosedByLd)
	}
	if FieldsEscapedBy != "" {
		G_string_append_printf(dbt.load_data_suffix, "ESCAPED BY '%s' ", FieldsEscapedBy)
	}
	G_string_append(dbt.load_data_suffix, "LINES ")
	if LinesStartingByLd != "" {
		G_string_append_printf(dbt.load_data_suffix, "STARTING BY '%s' ", LinesStartingByLd)
	}
	G_string_append_printf(dbt.load_data_suffix, "TERMINATED BY '%s' ", LinesTerminatedByLd)
	if IncludeHeader {
		G_string_append(dbt.load_data_suffix, "IGNORE 1 LINES ")
	}
	G_string_append(dbt.load_data_suffix, "(")
	if dbt.columns_on_insert != "" {
		G_string_append(dbt.load_data_suffix, dbt.columns_on_insert)
		G_string_append(dbt.load_data_suffix, ")")
	} else {
		var set_statement = append_load_data_columns(dbt.load_data_suffix, fields, num_fields)
		G_string_append(dbt.load_data_suffix, ")")
		if set_statement != nil {
			G_string_append(dbt.load_data_suffix, set_statement.Str.String())
		}
	}
	G_string_append(dbt.load_data_suffix, ";\n")
}

// initialize_load_data_header builds the CSV/LOAD DATA header line (column names) and stores it in dbt.load_data_header.
func initialize_load_data_header(dbt *db_table, fields []*sql.ColumnType, num_fields uint) {
	dbt.load_data_header = G_string_sized_new(StatementSize)
	var i uint
	for i = 0; i < num_fields-1; i++ {
		G_string_append(dbt.load_data_header, fields_enclosed_by)
		G_string_append(dbt.load_data_header, fields[i].Name())
		G_string_append(dbt.load_data_header, fields_enclosed_by)
		G_string_append(dbt.load_data_header, fields_terminated_by)
	}
	G_string_append(dbt.load_data_header, fields_enclosed_by)
	G_string_append(dbt.load_data_header, fields[i].Name())
	G_string_append(dbt.load_data_header, fields_enclosed_by)
	G_string_append(dbt.load_data_header, fields_terminated_by)
}

// write_statement writes statement to load_data_file, updates max_statement_size, and clears statement buffer; returns false on error.
func write_statement(load_data_file *file_write, filessize *float64, statement *GString, dbt *db_table) bool {
	if !real_write_data(load_data_file, filessize, statement) {
		log.Criticalf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
		return false
	}
	max_statement_size_mutex.Lock()
	if statement.Len > max_statement_size {
		max_statement_size = statement.Len
	}
	max_statement_size_mutex.Unlock()
	G_string_set_size(statement, 0)
	return true
}

// initialize_config_on_string appends [config] and max-statement-size to output.
func initialize_config_on_string(output *GString) {
	max_statement_size_mutex.Lock()
	G_string_append_printf(output, "[config]\nmax-statement-size = %d\n", max_statement_size)
	max_statement_size_mutex.Unlock()
}

// write_load_data_statement writes the LOAD DATA LOCAL INFILE statement for the table job's rows file into the SQL file.
func write_load_data_statement(tj *table_job) {
	var statement = G_string_sized_new(StatementSize)
	var basename = path.Base(tj.rows.filename)
	initialize_sql_statement(statement)
	G_string_append_printf(statement, "%s%s%s", LOAD_DATA_PREFIX, basename, tj.dbt.load_data_suffix.Str.String())
	if !write_data(tj.sql.file, statement) {
		log.Criticalf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
	}
}

// write_clickhouse_statement writes the ClickHouse INSERT ... FROM INFILE statement for the table job's rows file.
func write_clickhouse_statement(tj *table_job) {
	var statement = G_string_sized_new(StatementSize)
	var basename = path.Base(tj.rows.filename)
	initialize_sql_statement(statement)
	G_string_append_printf(statement, "%s INTO %s%s%s FROM INFILE '%s' FORMAT MySQLDump;", insert_statement, Identifier_quote_character_str,
		tj.dbt.table, Identifier_quote_character_str, basename)
	if !write_data(tj.sql.file, statement) {
		log.Criticalf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
	}
}

// write_header writes the load_data_header (if any) to the rows file; returns false on error.
func write_header(tj *table_job) bool {
	if tj.dbt.load_data_header != nil && !write_data(tj.rows.file, tj.dbt.load_data_header) {
		log.Criticalf("Could not write header for %s.%s", tj.dbt.database.name, tj.dbt.table)
		return false
	}
	return true
}

// StringToByte returns the first byte of s (helper for field delimiter/escape).
func StringToByte(s string) byte {
	b := []byte(s)[0]
	return b
}

// write_load_data_column_into_string appends the column value to buffers.column in LOAD DATA format (\\N, hex, or escaped).
func write_load_data_column_into_string(conn *DBConnection, column FieldValue, field *sql.ColumnType, length uint64, buffers *thread_data_buffers) {
	_ = conn
	if column.Value() == nil {
		G_string_append(buffers.column, "\\N")
	} else if is_hex_blob(field) {
		G_string_set_size(buffers.escaped, int(length*2+1))
		G_string_append(buffers.escaped, buffers.escaped.Str.String())
	} else if GetStandardType(field.DatabaseTypeName()) != "MYSQL_TYPE_LONG" && GetStandardType(field.DatabaseTypeName()) != "MYSQL_TYPE_LONGLONG" && GetStandardType(field.DatabaseTypeName()) != "MYSQL_TYPE_INT24" && GetStandardType(field.DatabaseTypeName()) != "MYSQL_TYPE_SHORT" {
		G_string_append(buffers.column, fields_enclosed_by)
		G_string_set_size(buffers.escaped, int(length*2+1))
		var new_length = m_replace_char_with_char('\\', StringToByte(FieldsEscapedBy), []byte(Escape(string(column.AsString()))))
		tmp := m_escape_char_with_char(StringToByte(fields_terminated_by), StringToByte(FieldsEscapedBy), []byte(new_length))
		G_string_append_b(buffers.column, tmp)
		G_string_append(buffers.column, fields_enclosed_by)
	} else {
		G_string_append_b(buffers.column, column.AsString())
	}
}

// write_sql_column_into_string appends the column value to buffers.column in SQL format (NULL, number, 0x..., or quoted escaped string).
func write_sql_column_into_string(conn *DBConnection, column FieldValue, field *sql.ColumnType, length uint64, buffers *thread_data_buffers) {
	_ = conn
	if column.Value() == nil {
		G_string_append(buffers.column, "NULL")
	} else if IsNumber(field.DatabaseTypeName()) && IsUnsigned(field.DatabaseTypeName()) {
		// G_string_append(buffers.column, strconv.FormatUint(column.AsUint64(), 10))
		G_string_append_b(buffers.column, column.AsString())
	} else if IsNumber(field.DatabaseTypeName()) && !IsUnsigned(field.DatabaseTypeName()) {
		// G_string_append(buffers.column, strconv.FormatInt(column.AsInt64(), 10))
		G_string_append_b(buffers.column, column.AsString())
	} else if IsFloat(field.DatabaseTypeName()) {
		G_string_append_b(buffers.column, column.AsString())
		// G_string_append(buffers.column, strconv.FormatFloat(column.AsFloat64(), 'f', -1, 64))
	} else if length == 0 {
		G_string_append(buffers.column, fields_enclosed_by)
		G_string_append(buffers.column, fields_enclosed_by)
	} else if is_hex_blob(field) {
		G_string_set_size(buffers.escaped, int(length*2+1))
		G_string_append(buffers.column, "0x")
		G_string_append(buffers.escaped, hex.EncodeToString(column.AsString()))
		G_string_append(buffers.column, buffers.escaped.Str.String())
	} else {
		G_string_set_size(buffers.escaped, int(length*2+1))
		columnStr := string(column.AsString())
		G_string_append(buffers.escaped, Escape(columnStr))
		if IsMySQLType(GetStandardType(field.DatabaseTypeName()), "MYSQL_TYPE_JSON") {
			G_string_append(buffers.column, "CONVERT(")
		}
		G_string_append(buffers.column, fields_enclosed_by)
		G_string_append(buffers.column, buffers.escaped.Str.String())
		G_string_append(buffers.column, fields_enclosed_by)
		// statement_row.WriteString(fmt.Sprintf("%s%s%s", fields_enclosed_by, *escaped, fields_enclosed_by))
		if IsMySQLType(GetStandardType(field.DatabaseTypeName()), "MYSQL_TYPE_JSON") {
			G_string_append(buffers.column, " USING UTF8MB4)")
		}
	}
}

// write_column_into_string_with_terminated_by writes one column (with optional anonymization f) into buffers.column and appends it plus terminated_by to buffers.row.
func write_column_into_string_with_terminated_by(conn *DBConnection, row FieldValue, fields *sql.ColumnType, lengths uint64, buffers *thread_data_buffers, write_column_into_string func(conn *DBConnection, column FieldValue, field *sql.ColumnType, length uint64, buffers *thread_data_buffers), f *Function_pointer, terminated_by string) {
	var column FieldValue
	var rlength = lengths
	G_string_set_size(buffers.column, 0)
	if row.Value() != nil {
		column = row
	}
	if f != nil {
		if f.Is_pre {
			write_column_into_string(conn, column, fields, rlength, buffers)
			column = f.Fun_ptr(buffers.column.Str.String())
			G_string_printf(buffers.column, "%s", column.AsString())
		} else {
			column = f.Fun_ptr(string(column.AsString()))
			write_column_into_string(conn, column, fields, rlength, buffers)
		}
	} else {
		write_column_into_string(conn, column, fields, rlength, buffers)
	}
	G_string_append(buffers.row, buffers.column.Str.String())
	G_string_append(buffers.row, terminated_by)
}

// write_row_into_string formats one row (with anonymization) into buffers.row using the given write_column_into_string callback.
func write_row_into_string(conn *DBConnection, dbt *db_table, row []FieldValue, fields []*sql.ColumnType, lengths uint64, num_fields uint, buffers *thread_data_buffers, write_column_into_string func(conn *DBConnection, column FieldValue, field *sql.ColumnType, length uint64, buffers *thread_data_buffers)) {
	var i uint
	G_string_append(buffers.row, lines_starting_by)
	var f = dbt.anonymized_function
	for i = 0; i < num_fields-1; i++ {
		lengths = uint64(row[i].Length())
		if f == nil {
			write_column_into_string_with_terminated_by(conn, row[i], fields[i], lengths, buffers, write_column_into_string, nil, fields_terminated_by)
		} else {
			write_column_into_string_with_terminated_by(conn, row[i], fields[i], lengths, buffers, write_column_into_string, f[i], fields_terminated_by)
		}
	}
	if f == nil {
		write_column_into_string_with_terminated_by(conn, row[i], fields[i], uint64(len(row[i].AsString())), buffers, write_column_into_string, nil, lines_terminated_by)
	} else {
		write_column_into_string_with_terminated_by(conn, row[i], fields[i], uint64(len(row[i].AsString())), buffers, write_column_into_string, f[i], lines_terminated_by)
	}
}

// update_dbt_rows adds num_rows to dbt.rows (thread-safe).
func update_dbt_rows(dbt *db_table, num_rows *uint64) {
	dbt.rows_lock.Lock()
	dbt.rows += *num_rows
	dbt.rows_lock.Unlock()
}

// close_file closes the table job file tjf and clears its file/filename; no-op if tjf.file is nil.
func close_file(tj *table_job, tjf *table_job_file) {
	if tjf.file != nil {
		m_close(tj.td.thread_id, tjf.file, tjf.filename, 1, tj.dbt)
		tjf.file = nil
		tjf.filename = ""
	}
}

// close_files closes SQL and rows files for the table job according to output_format.
func close_files(tj *table_job) {
	switch output_format {
	case LOAD_DATA, CSV, CLICKHOUSE:
		close_file(tj, tj.sql)
		break
	case SQL_INSERT:
		break
	}
	close_file(tj, tj.rows)
}

// reopen_files closes current files and opens new ones (incremented sub_part), rewriting LOAD DATA/ClickHouse/header as needed.
func reopen_files(tj *table_job) {
	close_files(tj)
	switch output_format {
	case LOAD_DATA, CSV:
		if update_files_on_table_job(tj) {
			write_load_data_statement(tj)
			write_header(tj)
		}
		break
	case CLICKHOUSE:
		if update_files_on_table_job(tj) {
			write_clickhouse_statement(tj)
			write_header(tj)
		}
		break
	case SQL_INSERT:
		update_files_on_table_job(tj)
		break
	}
}

// write_result_into_file streams the query result to the dump file(s), batching rows into INSERT statements up to StatementSize, and rotating files when chunk_filesize is exceeded.
func write_result_into_file(conn *DBConnection, result *MYSQL_RES, tj *table_job) {
	var dbt *db_table = tj.dbt
	var num_fields uint = Mysql_num_fields(result)
	var row []FieldValue
	var fields []*sql.ColumnType = Mysql_fetch_fields(result)
	G_string_set_size(tj.td.thread_data_buffers.statement, 0)
	G_string_set_size(tj.td.thread_data_buffers.row, 0)
	G_string_set_size(tj.td.thread_data_buffers.escaped, 0)
	var lengths uint64
	var num_rows uint64
	var num_rows_st uint64
	var write_column_into_string func(conn *DBConnection, column FieldValue, field *sql.ColumnType, length uint64, buffers *thread_data_buffers) = write_sql_column_into_string
	switch output_format {
	case LOAD_DATA, CSV:
		write_column_into_string = write_load_data_column_into_string
		if dbt.load_data_suffix == nil {
			dbt.chunks_mutex.Lock()
			if dbt.load_data_suffix == nil {
				initialize_load_data_statement_suffix(tj.dbt, fields, num_fields)
			}
			if IncludeHeader {
				initialize_load_data_header(tj.dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if update_files_on_table_job(tj) {
			write_load_data_statement(tj)
			write_header(tj)
		}
		break
	case CLICKHOUSE:
		if tj.rows.file == nil {
			update_files_on_table_job(tj)
		}
		if dbt.load_data_suffix == nil {
			dbt.chunks_mutex.Lock()
			if dbt.load_data_suffix == nil {
				initialize_clickhouse_statement_suffix(tj.dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if dbt.insert_statement == nil {
			dbt.chunks_mutex.Lock()
			if dbt.insert_statement == nil {
				build_insert_statement(dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if tj.st_in_file == 0 {
			initialize_sql_statement(tj.td.thread_data_buffers.statement)
			write_clickhouse_statement(tj)
		}
		G_string_append(tj.td.thread_data_buffers.statement, dbt.insert_statement.Str.String())
		break
	case SQL_INSERT:
		if tj.rows.file == nil {
			update_files_on_table_job(tj)
		}
		if dbt.insert_statement == nil {
			dbt.chunks_mutex.Lock()
			if dbt.insert_statement == nil {
				build_insert_statement(dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if tj.st_in_file == 0 {
			initialize_sql_statement(tj.td.thread_data_buffers.statement)
		}
		G_string_append(tj.td.thread_data_buffers.statement, dbt.insert_statement.Str.String())
		break
	}
	message_dumping_data(tj)
	var from = time.Now()
	var to time.Time
	var diff float64
	for row = Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		num_rows++
		write_row_into_string(conn, dbt, row, fields, lengths, num_fields, tj.td.thread_data_buffers, write_column_into_string)
		if tj.td.thread_data_buffers.statement.Len+tj.td.thread_data_buffers.row.Len+1 > StatementSize {
			if num_rows_st == 0 {
				G_string_append(tj.td.thread_data_buffers.statement, tj.td.thread_data_buffers.row.Str.String())
				G_string_set_size(tj.td.thread_data_buffers.row, 0)
				log.Warnf("Row bigger than statement_size for %s.%s", dbt.database.name, dbt.table)
			}
			G_string_append(tj.td.thread_data_buffers.statement, statement_terminated_by)
			if !write_statement(tj.rows.file, &(tj.filesize), tj.td.thread_data_buffers.statement, dbt) {
				log.Criticalf("Fail to write on %s", tj.rows.filename)
				return
			}
			update_dbt_rows(dbt, &num_rows)
			tj.num_rows_of_last_run += num_rows
			num_rows = 0
			tj.st_in_file++
			if output_format == SQL_INSERT || output_format == CLICKHOUSE {
				G_string_append(tj.td.thread_data_buffers.statement, dbt.insert_statement.Str.String())
			}
			to = time.Now()
			diff = to.Sub(from).Seconds()
			if diff > 4 {
				from = to
				message_dumping_data(tj)
			}
			check_pause_resume(tj.td)
			if shutdown_triggered {
				return
			}
		}
		if dbt.chunk_filesize != 0 && math.Ceil(tj.filesize/1024/1024) > float64(dbt.chunk_filesize) {
			tj.sub_part++
			reopen_files(tj)
			if output_format == SQL_INSERT {
				initialize_sql_statement(tj.td.thread_data_buffers.statement)
				G_string_append(tj.td.thread_data_buffers.statement, dbt.insert_statement.Str.String())
			}
			tj.st_in_file = 0
			tj.filesize = 0
		}
		if num_rows_st != 0 && (output_format == SQL_INSERT || output_format == CLICKHOUSE) {
			G_string_append(tj.td.thread_data_buffers.statement, row_delimiter)
		}
		G_string_append(tj.td.thread_data_buffers.statement, tj.td.thread_data_buffers.row.Str.String())
		if tj.td.thread_data_buffers.row.Len > 0 {
			num_rows_st++
		}
		G_string_set_size(tj.td.thread_data_buffers.row, 0)
	}
	update_dbt_rows(dbt, &num_rows)
	tj.num_rows_of_last_run += num_rows
	if num_rows_st > 0 && tj.td.thread_data_buffers.statement.Len > 0 {
		if output_format == SQL_INSERT || output_format == CLICKHOUSE {
			G_string_append(tj.td.thread_data_buffers.statement, statement_terminated_by)
		}
		if !write_statement(tj.rows.file, &(tj.filesize), tj.td.thread_data_buffers.statement, dbt) {
			log.Criticalf("Fail to write on %s", tj.rows.filename)
			return
		}
		tj.st_in_file++
	}

	return
}

// write_table_job_into_file runs the SELECT for the table job, streams the result into file(s) via write_result_into_file, and performs cleanup/retry on error.
func write_table_job_into_file(tj *table_job) {
	var conn = tj.td.thrconn
	var query string
	time.Sleep(time.Millisecond * time.Duration(Throttle))
	tj.num_rows_of_last_run = 0
	var cache, fields, where1, where_option1, where2, where_option2, where3, where_option3, order, order_option, limit, limit_opt string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if tj.dbt.select_fields != "" {
		fields = tj.dbt.select_fields
	} else {
		fields = "*"
	}
	if tj.where.Len > 0 || WhereOption != "" || tj.dbt.where != "" {
		where1 = "WHERE"
	}
	if tj.where.Len > 0 {
		where_option1 = tj.where.Str.String()
	}
	if tj.where.Len > 0 && WhereOption != "" {
		where2 = "AND"
	}
	if WhereOption != "" {
		where_option2 = WhereOption
	}
	if (tj.where.Len > 0 || WhereOption != "") && tj.dbt.where != "" {
		where3 = "AND"
	}
	if tj.dbt.where != "" {
		where_option3 = tj.dbt.where
	}
	if OrderByPrimaryKey && tj.dbt.primary_key_separated_by_comma != "" {
		order = "ORDER BY"
		order_option = tj.dbt.primary_key_separated_by_comma
	}
	if tj.dbt.limit != "" {
		limit = "LIMIT"
		limit_opt = tj.dbt.limit
	}
	query = fmt.Sprintf("SELECT %s %s FROM %s%s%s.%s%s%s %s %s %s %s %s %s %s %s %s %s %s",
		cache,
		fields,
		Identifier_quote_character_str, tj.dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, tj.dbt.table, Identifier_quote_character_str,
		tj.partition, where1, where_option1, where2, where_option2, where3, where_option3, order, order_option, limit, limit_opt)
	var result = M_use_result(conn, query, M_warning, "Failed to execute query")
	if result == nil && conn.Err != nil {
		if !it_is_a_consistent_backup {
			log.Warnf("Thread %d: Error dumping table (%s.%s) data: %s\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
				Mysql_error(conn), query)
			if !Mysql_ping(tj.td.thrconn) {
				M_connect(tj.td.thrconn)
				Execute_gstring(tj.td.thrconn, Set_session)
			}
			log.Warnf("Thread %d: Retrying last failed executed statement", tj.td.thread_id)
			result = M_use_result(conn, query, nil, "Failed to execute query on second try")
			if result == nil {
				goto cleanup
			}
		} else {
			goto cleanup
		}
	}
	/* Poor man's data dump code */
	write_result_into_file(conn, result, tj)
	if Mysql_errno(conn) != 0 {
		log.Criticalf("Thread %d: Could not read data from %s.%s to write on %s at byte %.0f: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, tj.rows.filename, tj.filesize, Mysql_error(conn))
		Errors++
		if Mysql_ping(tj.td.thrconn) {
			if !it_is_a_consistent_backup {
				log.Warnf("Thread %d: Reconnecting due Errors", tj.td.thread_id)
				M_connect(tj.td.thrconn)
				Execute_gstring(tj.td.thrconn, Set_session)
			}
		}
	}
cleanup:
	if result != nil {
		Mysql_free_result(result)
	}
}
