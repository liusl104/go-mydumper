package mydumper

import (
	"container/list"
	"encoding/hex"
	Error "errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

var (
	ChunkFilesize           uint
	LoadData                bool
	Csv                     bool
	OutputFormat            string
	IncludeHeader           bool
	FieldsTerminatedByLd    string
	FieldsEnclosedByLd      string
	FieldsEscapedBy         string
	LinesStartingByLd       string
	LinesTerminatedByLd     string
	StatementTerminatedByLd string
	InsertIgnore            bool
	Replace                 bool
	CompleteInsert          bool
	HexBlob                 bool
	StatementSize           int = 100000
	clickhouse              bool
	fields_enclosed_by      string
	fields_terminated_by    string
	lines_terminated_by     string
	lines_starting_by       string
	statement_terminated_by string
	insert_statement        string = INSERT
	message_dumping_data    func(tj *table_job)
)

const LOAD_DATA_PREFIX = "LOAD DATA LOCAL INFILE '"

// var m_write func(file *file_write, buff string) (int, error)

func message_dumping_data_short(tj *table_job) {
	var total uint64 = 0
	if tj.dbt.rows_total != 0 {
		total = 100 * (tj.dbt.rows / tj.dbt.rows_total)
	}
	log.Infof("Thread %d: %s%s%s.%s%s%s [ %d%% ] | Tables: %d/%d",
		tj.td.thread_id,
		Identifier_quote_character_str, tj.dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str,
		tj.dbt.table, Identifier_quote_character_str,
		total,
		innodb_table.list.Len()+non_innodb_table.list.Len(), len(all_dbts))
}

func message_dumping_data_long(tj *table_job) {
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
	if len(tj.where) > 0 || WhereOption != "" || tj.dbt.where != "" {
		where_opt = " WHERE "
	}
	if len(tj.where) > 0 {
		where_val = tj.where
	}
	if len(tj.where) > 0 && WhereOption != "" {
		where_and_opt = " AND "
	}
	if WhereOption != "" {
		where_and_val = WhereOption
	}
	if (len(tj.where) > 0 || WhereOption != "") && tj.dbt.where != "" {
		where_and_opt_1 = " AND "
	}
	if tj.dbt.where != "" {
		where_and_val_1 = tj.dbt.where
	}
	if tj.order_by != "" {
		order_by = " ORDER BY "
		order_by_val = tj.order_by
	}

	log.Infof("Thread %d: dumping data from %s%s%s.%s%s%s%s%s%s%s%s%s%s%s%s%s into %s | Completed: %d%% | Remaining tables: %d / %d",
		tj.td.thread_id,
		Identifier_quote_character_str, tj.dbt.database.name, Identifier_quote_character_str, Identifier_quote_character_str,
		tj.dbt.table, Identifier_quote_character_str,
		partition_opt, partition_val,
		where_opt, where_val,
		where_and_opt, where_and_val,
		where_and_opt_1, where_and_val_1,
		order_by, order_by_val,
		tj.rows.filename, total,
		innodb_table.list.Len()+non_innodb_table.list.Len(), len(all_dbts))
}

func initialize_write() {
	if Verbose > 3 {
		message_dumping_data = message_dumping_data_long
	} else {
		message_dumping_data = message_dumping_data_short
	}

	if starting_chunk_step_size > 0 && ChunkFilesize > 0 {
		log.Warnf("We are going to chunk by row and by filesize when possible")
	}
	if fields_enclosed_by == "" {
		log.Fatalf("detect_quote_character not exectue")
	}
	if FieldsEnclosedByLd == "" && len(FieldsEnclosedByLd) > 1 {
		log.Fatalf("--fields-enclosed-by must be a single character")
	}
	if FieldsEscapedBy == "" && len(FieldsEscapedBy) > 1 {
		log.Fatalf("--fields-enclosed-by must be a single character")
	}

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
			statement_terminated_by = ";"
		} else {
			statement_terminated_by = Replace_escaped_strings(StatementTerminatedByLd)
		}
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
	}

	if InsertIgnore && Replace {
		log.Errorf("You can't use --insert-ignore and --replace at the same time")
	}

	if InsertIgnore {
		insert_statement = INSERT_IGNORE
	}
	if Replace {
		insert_statement = REPLACE
	}
}

func finalize_write() {
	fields_terminated_by = ""
	lines_starting_by = ""
	lines_terminated_by = ""
	statement_terminated_by = ""
}

func append_load_data_columns(statement *GString, fields []*mysql.Field, num_fields uint) *GString {
	var i uint
	var str = G_string_new("SET ")
	var appendable bool
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			G_string_append(statement, ",")
		}
		if fields[i].Type == mysql.MYSQL_TYPE_JSON {
			G_string_append(statement, "@")
			G_string_append_c(statement, fields[i].Name)
			if str.Len > 4 {
				G_string_append(str, ",")
			}
			G_string_append(str, Identifier_quote_character_str)
			G_string_append_c(str, fields[i].Name)
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(str, "=CONVERT(@")
			G_string_append_c(str, fields[i].Name)
			G_string_append(str, " USING UTF8MB4)")
			appendable = true
		} else if HexBlob && fields[i].Type == mysql.MYSQL_TYPE_BLOB {
			G_string_append(statement, "@")
			G_string_append_c(statement, fields[i].Name)
			if str.Len > 4 {
				G_string_append(str, ",")
			}
			G_string_append(str, Identifier_quote_character_str)
			G_string_append_c(statement, fields[i].Name)
			G_string_append(str, Identifier_quote_character_str)
			G_string_append(str, "=UNHEX(@")
			G_string_append_c(statement, fields[i].Name)
			G_string_append(statement, ")")
			appendable = true
		} else {
			G_string_append(str, Identifier_quote_character_str)
			G_string_append_c(statement, fields[i].Name)
			G_string_append(str, Identifier_quote_character_str)
		}
	}
	if appendable {
		return str
	} else {
		return nil
	}
}

func append_columns(statement *GString, fields []*mysql.Field, num_fields uint) {
	var i uint
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			G_string_append(statement, ",")
		}
		G_string_append(statement, Identifier_quote_character)
		G_string_append(statement, string(fields[i].Name))
		G_string_append(statement, Identifier_quote_character)
	}

}

func build_insert_statement(dbt *DB_Table, fields []*mysql.Field, num_fields uint) {
	var i_s = G_string_new(insert_statement)
	G_string_append(i_s, insert_statement)
	G_string_append(i_s, " INTO ")
	G_string_append(i_s, Identifier_quote_character)
	G_string_append(i_s, dbt.table)
	G_string_append(i_s, Identifier_quote_character)
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
	dbt.insert_statement = nil
}

func real_write_data(file *file_write, filesize *float64, data *GString) bool {
	var written int
	var r int
	var err error
	var second_write_zero bool
	for written < data.Len {
		r, err = file.write([]byte(data.Str.String()))
		if err != nil {
			log.Criticalf("Couldn't write data to a file: %v", err)
			return false
		}
		if r == 0 {
			if second_write_zero {
				log.Criticalf("Couldn't write data to a file: %v", err)
				return false
			}
			second_write_zero = true
		} else {
			second_write_zero = true
		}
		written += r
	}

	*filesize += float64(written)
	return true
}

func write_data(file *file_write, data *GString) bool {
	var f float64
	return real_write_data(file, &f, data)
}

func initialize_load_data_statement_suffix(dbt *DB_Table, fields []*mysql.Field, num_fields uint) {
	var character_set string
	if SetNamesStr != "" {
		character_set = SetNamesStr
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

func initialize_clickhouse_statement_suffix(dbt *DB_Table, fields []*mysql.Field, num_fields uint) {
	var character_set string
	if SetNamesStr != "" {
		character_set = SetNamesStr
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

func initialize_load_data_header(dbt *DB_Table, fields []*mysql.Field, num_fields uint) {
	dbt.load_data_header = G_string_sized_new(StatementSize)
	var i uint
	for i = 0; i < num_fields-1; i++ {
		G_string_append(dbt.load_data_header, fields_enclosed_by)
		G_string_append_c(dbt.load_data_header, fields[i].Name)
		G_string_append(dbt.load_data_header, fields_enclosed_by)
		G_string_append(dbt.load_data_header, fields_terminated_by)
	}
	G_string_append(dbt.load_data_header, fields_enclosed_by)
	G_string_append_c(dbt.load_data_header, fields[i].Name)
	G_string_append(dbt.load_data_header, fields_enclosed_by)
	G_string_append(dbt.load_data_header, fields_terminated_by)
}

func write_statement(load_data_file *file_write, filessize *float64, statement *GString, dbt *DB_Table) bool {
	if !real_write_data(load_data_file, filessize, statement) {
		log.Criticalf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
		return false
	}
	G_string_set_size(statement, 0)
	return true
}

func write_load_data_statement(tj *table_job) {
	var statement = G_string_sized_new(StatementSize)
	var basename = path.Base(tj.rows.filename)
	initialize_sql_statement(statement)
	G_string_append_printf(statement, "%s%s%s", LOAD_DATA_PREFIX, basename, tj.dbt.load_data_suffix.Str.String())
	if !write_data(tj.sql.file, statement) {
		log.Criticalf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
	}
}

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

func write_header(tj *table_job) bool {
	if tj.dbt.load_data_header != nil && !write_data(tj.rows.file, tj.dbt.load_data_header) {
		log.Criticalf("Could not write header for %s.%s", tj.dbt.database.name, tj.dbt.table)
		return false
	}
	return true
}

func get_estimated_remaining_of(mlist *MList) uint64 {
	mlist.mutex.Lock()
	var tl *list.List = mlist.list
	var total uint64
	for e := tl.Front(); e != nil; e = e.Next() {
		total += e.Value.(*DB_Table).estimated_remaining_steps
	}
	mlist.mutex.Unlock()
	return total
}

func get_estimated_remaining_of_all_chunks() uint64 {
	return get_estimated_remaining_of(non_innodb_table) + get_estimated_remaining_of(innodb_table)
}

func write_load_data_column_into_string(conn *DBConnection, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *GString, statement_row *GString, fun_ptr_i *Function_pointer) {
	_ = conn
	if column.Value() != nil {
		fun_ptr_i = nil
		// TODO
	}
	if column.Value() == nil {
		G_string_append(statement_row, "\\N")
	} else if field.Type == mysql.MYSQL_TYPE_BLOB && HexBlob {
		G_string_printf(escaped, hex.EncodeToString([]byte(escaped.Str.String())))
		G_string_append(statement_row, escaped.Str.String())
	} else if field.Type != mysql.MYSQL_TYPE_LONG && field.Type != mysql.MYSQL_TYPE_LONGLONG && field.Type != mysql.MYSQL_TYPE_INT24 && field.Type != mysql.MYSQL_TYPE_SHORT {
		G_string_append(statement_row, fields_enclosed_by)
		// *escaped = mysql.Escape(string(column.AsString()))
		// *escaped = strings.ReplaceAll(*escaped, "\\", Fields_escaped_by)
		m_replace_char_with_char('\\', []rune(FieldsEscapedBy)[0], []rune(escaped.Str.String()))
		m_escape_char_with_char([]byte(fields_terminated_by), []byte(FieldsEscapedBy), []byte(escaped.Str.String()))
		G_string_append(statement_row, escaped.Str.String())
		G_string_append(statement_row, fields_enclosed_by)
	} else {
		// statement_row.WriteString(strconv.FormatInt(column.AsInt64(), 10))
		G_string_append_c(statement_row, column.AsString())
	}
}

func write_sql_column_into_string(conn *DBConnection, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *GString, statement_row *GString, fun_ptr_i *Function_pointer) {
	if &column != nil {
		fun_ptr_i = nil
	}
	_ = conn
	if column.Value() == nil {
		G_string_append(statement_row, "NULL")
	} else if field.Type <= mysql.MYSQL_TYPE_INT24 {
		G_string_append(statement_row, strconv.FormatInt(column.AsInt64(), 10))
	} else if length.ColumnLength == 0 {
		G_string_append(statement_row, fields_enclosed_by)
		G_string_append(statement_row, fields_enclosed_by)
	} else if field.Type == mysql.MYSQL_TYPE_BLOB && HexBlob {
		G_string_set_size(escaped, int(length.ColumnLength*2+1))
		G_string_append(statement_row, "0x")
		G_string_printf(escaped, hex.EncodeToString(column.AsString()))
		G_string_append(statement_row, escaped.Str.String())
	} else {
		G_string_set_size(escaped, int(length.ColumnLength*2+1))
		G_string_printf(escaped, mysql.Escape(string(column.AsString())))
		if field.Type == mysql.MYSQL_TYPE_JSON {
			G_string_append(statement_row, "CONVERT(")
		}
		G_string_append(statement_row, fields_enclosed_by)
		G_string_append(statement_row, escaped.Str.String())
		G_string_append(statement_row, fields_enclosed_by)
		// statement_row.WriteString(fmt.Sprintf("%s%s%s", fields_enclosed_by, *escaped, fields_enclosed_by))
		if field.Type == mysql.MYSQL_TYPE_JSON {
			G_string_append(statement_row, " USING UTF8MB4)")
		}
	}
}

func write_row_into_string(conn *DBConnection, dbt *DB_Table, row []mysql.FieldValue, fields []*mysql.Field, lengths []*mysql.Field, num_fields uint, escaped *GString, statement_row *GString, write_column_into_string func(conn *DBConnection, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *GString, statement_row *GString, fun_ptr_i *Function_pointer)) {
	var i uint
	G_string_append(statement_row, lines_starting_by)
	_ = dbt
	// var f = dbt.anonymized_function
	var p *Function_pointer

	for i = 0; i < num_fields-1; i++ {
		/*if f == nil {
			p = new(function_pointer)
		} else {
			p = f[i]
		}*/
		write_column_into_string(conn, row[i], fields[i], lengths[i], escaped, statement_row, p)
		G_string_append(statement_row, fields_terminated_by)

	}
	write_column_into_string(conn, row[i], fields[i], lengths[i], escaped, statement_row, p)
	G_string_append(statement_row, lines_terminated_by)
}

func update_dbt_rows(dbt *DB_Table, num_rows uint64) {
	dbt.rows_lock.Lock()
	dbt.rows += num_rows
	dbt.rows_lock.Unlock()
}

func initiliaze_load_data_files(tj *table_job, dbt *DB_Table) {
	m_close(tj.td.thread_id, tj.sql.file, tj.sql.filename, 1, dbt)
	m_close(tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
	tj.sql.file = nil
	tj.sql.file.status = 0
	tj.rows.file = nil
	tj.rows.file.status = 0
	tj.sql.filename = ""
	tj.rows.filename = ""
	if update_files_on_table_job(tj) {
		write_load_data_statement(tj)
		write_header(tj)
	}

}

func initiliaze_clickhouse_files(tj *table_job, dbt *DB_Table) {
	m_close(tj.td.thread_id, tj.sql.file, tj.sql.filename, 1, dbt)
	m_close(tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
	tj.sql.file = nil
	tj.sql.file.status = 0
	tj.rows.file = nil
	tj.rows.file.status = 0

	tj.sql.filename = ""
	tj.rows.filename = ""

	if update_files_on_table_job(tj) {
		write_clickhouse_statement(tj)
		write_header(tj)
	}
}

func write_result_into_file(conn *DBConnection, tj *table_job, query string) error {
	var result mysql.Result
	var dbt *DB_Table = tj.dbt
	var num_fields uint
	var escaped *GString = G_string_sized_new(3000)
	var err error
	var fields []*mysql.Field
	var statement = G_string_sized_new(2 * StatementSize)
	var statement_row = G_string_sized_new(0)
	// var lengths []*mysql.Field
	var num_rows uint64
	var num_row_st uint64
	var write_column_into_string func(conn *DBConnection, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *GString, statement_row *GString, fun_ptr_i *Function_pointer)
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
	case CLICKHOUSE:
		write_column_into_string = write_sql_column_into_string
		if tj.rows.file.status == 0 {
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
			initialize_sql_statement(statement)
			write_clickhouse_statement(tj)
		}
		G_string_append(statement, dbt.insert_statement.Str.String())
	case SQL_INSERT:
		write_column_into_string = write_sql_column_into_string
		if tj.rows.file.status == 0 {
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
			initialize_sql_statement(statement)
		}
		G_string_append(statement, dbt.insert_statement.Str.String())
	}
	message_dumping_data(tj)
	err = execute_select_streaming(conn, query, tj, dbt, write_column_into_string)
	if err != nil {
		if !it_is_a_consistent_backup {
			log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
				err, query)
			if err = conn.Ping(); err != nil {
				M_connect(tj.td.thrconn)
				Execute_gstring(tj.td.thrconn, Set_session)
			}
			err = execute_select_streaming(conn, query, tj, dbt, write_column_into_string)
			log.Warnf("Thread %d: Retrying last failed executed statement", tj.td.thread_id)
			if err != nil || &result == nil {
				if SuccessOn1146 && conn.Code == 1146 {
					log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
						err, query)
				} else {
					log.Criticalf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
						err, query)
					errors++
				}
			}
		} else {
			if SuccessOn1146 && conn.Code == 1146 {
				log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
					err, query)
			} else {
				log.Criticalf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table,
					err, query)
				errors++
			}

		}
	}
	update_dbt_rows(dbt, num_rows)
	if num_row_st > 0 && statement.Len > 0 {
		if output_format == SQL_INSERT || output_format == CLICKHOUSE {
			G_string_append(statement, statement_terminated_by)
		}
		if !write_statement(tj.rows.file, &tj.filesize, statement, dbt) {
			return err
		}
		tj.st_in_file++
	}
	G_string_free(statement, true)
	G_string_free(escaped, true)
	G_string_free(statement_row, true)
	return err
}
func execute_select_streaming(conn *DBConnection, query string, tj *table_job, dbt *DB_Table, write_column_into_string func(conn *DBConnection, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *GString, statement_row *GString, fun_ptr_i *Function_pointer)) error {
	var err error
	var result mysql.Result
	var num_fields uint
	var escaped *GString = G_string_sized_new(3000)
	var fields []*mysql.Field
	var statement = G_string_sized_new(2 * StatementSize)
	var statement_row = G_string_sized_new(0)
	var lengths []*mysql.Field
	var num_rows uint64
	var num_row_st uint64
	var from = time.Now()
	err = conn.Conn.ExecuteSelectStreaming(query, &result, func(row []mysql.FieldValue) error {
		num_rows++
		write_row_into_string(conn, dbt, row, fields, lengths, num_fields, escaped, statement_row, write_column_into_string)
		if statement.Len+statement_row.Len+1 > StatementSize {
			if num_row_st == 0 {
				G_string_append(statement, statement_row.Str.String())
				G_string_set_size(statement_row, 0)
				log.Warnf("Row bigger than statement_size for %s.%s", dbt.database.name, dbt.table)
			}
			G_string_append(statement, statement_terminated_by)
			if !write_statement(tj.rows.file, &tj.filesize, statement, dbt) {
				return nil
			}
			update_dbt_rows(dbt, num_rows)
			num_rows = 0
			num_row_st = 0
			tj.st_in_file++
			if output_format == SQL_INSERT || output_format == CLICKHOUSE {
				G_string_append(statement, dbt.insert_statement.Str.String())
			}
			to := time.Now()
			diff := to.Sub(from).Seconds()
			if diff > 4 {
				from = to
				message_dumping_data(tj)
			}
			check_pause_resume(tj.td)
			if shutdown_triggered {
				return nil
			}
		}
		if dbt.chunk_filesize != 0 && uint(math.Ceil(tj.filesize/1024/1024)) > dbt.chunk_filesize {
			tj.sub_part++
			switch output_format {
			case LOAD_DATA, CSV:
				initiliaze_load_data_files(tj, dbt)
				break
			case CLICKHOUSE:
				initiliaze_clickhouse_files(tj, dbt)
				break
			case SQL_INSERT:
				m_close(tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
				tj.rows.file = nil
				tj.rows.file.status = 0
				update_files_on_table_job(tj)
				initiliaze_clickhouse_files(tj, dbt)
				G_string_append(statement, dbt.insert_statement.Str.String())
				break
			}
			tj.st_in_file = 0
			tj.filesize = 0
		}

		if num_row_st != 0 && (output_format == SQL_INSERT || output_format == CLICKHOUSE) {
			G_string_append(statement, ",")
		}
		G_string_append(statement, statement_row.Str.String())
		if statement_row.Len > 0 {
			num_row_st++
		}
		G_string_set_size(statement_row, 0)
		return nil
	}, func(result *mysql.Result) error {
		lengths = result.Fields
		num_fields = uint(len(result.Fields))
		fields = result.Fields
		return nil
	})
	if err != nil {
		var myError *mysql.MyError
		Error.As(err, &myError)
		conn.Code = myError.Code
		return err
	}
	return nil
}

func write_table_job_into_file(tj *table_job) {
	var conn = tj.td.thrconn
	var query string
	var cache, fields, where1, where_option1, where2, where_option2, where3, where_option3, order, limit string
	if Is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if tj.dbt.columns_on_select != "" {
		fields = tj.dbt.columns_on_select
	} else {
		fields = tj.dbt.select_fields
	}
	if tj.where != "" || WhereOption != "" || tj.dbt.where != "" {
		where1 = "WHERE"
	}
	if tj.where != "" {
		where_option1 = tj.where
	}
	if tj.where != "" && WhereOption != "" {
		where2 = "AND"
	}
	if WhereOption != "" {
		where_option2 = WhereOption
	}
	if (tj.where != "" || WhereOption != "") && tj.dbt.where != "" {
		where3 = "AND"
	}
	if tj.dbt.where != "" {
		where_option3 = tj.dbt.where
	}
	if tj.order_by != "" {
		order = "ORDER BY"
	}
	if tj.dbt.limit != "" {
		limit = "LIMIT"
	}
	query = fmt.Sprintf("SELECT %s %s FROM %s%s%s.%s%s%s %s %s %s %s %s %s %s %s %s %s %s",
		cache,
		fields,
		Identifier_quote_character_str, tj.dbt.database.name, Identifier_quote_character_str,
		Identifier_quote_character_str, tj.dbt.table, Identifier_quote_character_str,
		tj.partition, where1, where_option1, where2, where_option2, where3, where_option3, order, tj.order_by, limit, tj.dbt.limit)

	err := write_result_into_file(conn, tj, query)
	if err != nil {
		log.Criticalf("Thread %d: Could not read data from %s.%s to write on %s at byte %.0f: %v", tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, tj.rows.filename, tj.filesize,
			err)
		errors++
		if err = tj.td.thrconn.Ping(); err != nil {
			if !it_is_a_consistent_backup {
				log.Warnf("Thread %d: Reconnecting due errors", tj.td.thread_id)
				M_connect(tj.td.thrconn)
				Execute_gstring(tj.td.thrconn, Set_session)
			}
		}
	}
}
