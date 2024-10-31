package mydumper

import (
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

const LOAD_DATA_PREFIX = "LOAD DATA LOCAL INFILE '"

// var m_write func(file *file_write, buff string) (int, error)

func message_dumping_data_short(o *OptionEntries, tj *table_job) {
	var total uint64 = 0
	if tj.dbt.rows_total != 0 {
		total = 100 * (tj.dbt.rows / tj.dbt.rows_total)
	}
	log.Infof("Thread %d: %s%s%s.%s%s%s [ %d%% ] | Tables: %d/%d",
		tj.td.thread_id,
		o.global.identifier_quote_character_str, tj.dbt.database.name, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str,
		tj.dbt.table, o.global.identifier_quote_character_str,
		total,
		len(o.global.innodb_table.list)+len(o.global.non_innodb_table.list), len(o.global.all_dbts))
}

func message_dumping_data_long(o *OptionEntries, tj *table_job) {
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
	if len(tj.where) > 0 || o.Filter.WhereOption != "" || tj.dbt.where != "" {
		where_opt = " WHERE "
	}
	if len(tj.where) > 0 {
		where_val = tj.where
	}
	if len(tj.where) > 0 && o.Filter.WhereOption != "" {
		where_and_opt = " AND "
	}
	if o.Filter.WhereOption != "" {
		where_and_val = o.Filter.WhereOption
	}
	if (len(tj.where) > 0 || o.Filter.WhereOption != "") && tj.dbt.where != "" {
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
		o.global.identifier_quote_character_str, tj.dbt.database.name, o.global.identifier_quote_character_str, o.global.identifier_quote_character_str,
		tj.dbt.table, o.global.identifier_quote_character_str,
		partition_opt, partition_val,
		where_opt, where_val,
		where_and_opt, where_and_val,
		where_and_opt_1, where_and_val_1,
		order_by, order_by_val,
		tj.rows.filename, total,
		len(o.global.innodb_table.list)+len(o.global.non_innodb_table.list), len(o.global.all_dbts))
}

func initialize_write(o *OptionEntries) {
	if o.Common.Verbose > 3 {
		o.global.message_dumping_data = message_dumping_data_long
	} else {
		o.global.message_dumping_data = message_dumping_data_short
	}

	if o.global.starting_chunk_step_size > 0 && o.Extra.ChunkFilesize > 0 {
		log.Warnf("We are going to chunk by row and by filesize when possible")
	}
	if o.global.fields_enclosed_by == "" {
		log.Fatalf("detect_quote_character not exectue")
	}
	if o.Statement.FieldsEnclosedByLd == "" && len(o.Statement.FieldsEnclosedByLd) > 1 {
		log.Fatalf("--fields-enclosed-by must be a single character")
	}
	if o.global.fields_escaped_by == "" && len(o.global.fields_escaped_by) > 1 {
		log.Fatalf("--fields-enclosed-by must be a single character")
	}

	switch o.global.output_format {
	case CLICKHOUSE, SQL_INSERT:
		if o.Statement.FieldsEnclosedByLd != "" {
			o.global.fields_enclosed_by = o.Statement.FieldsEnclosedByLd
		}
		if o.Statement.FieldsTerminatedByLd == "" {
			o.global.fields_terminated_by = ","
		} else if strings.Compare(o.Statement.FieldsTerminatedByLd, "\\t") == 0 {
			o.global.fields_terminated_by = "\t"
		} else {
			o.global.fields_terminated_by = replace_escaped_strings(o.Statement.FieldsTerminatedByLd)
		}
		if o.Statement.LinesStartingByLd == "" {
			o.global.lines_starting_by = "("
		} else {
			o.global.lines_starting_by = replace_escaped_strings(o.Statement.LinesStartingByLd)
		}
		if o.Statement.LinesTerminatedByLd == "" {
			o.global.lines_terminated_by = ")\n"
		} else {
			o.global.lines_terminated_by = replace_escaped_strings(o.Statement.LinesTerminatedByLd)
		}
		if o.Statement.StatementTerminatedByLd == "" {
			o.global.statement_terminated_by = ";"
		} else {
			o.global.statement_terminated_by = replace_escaped_strings(o.Statement.StatementTerminatedByLd)
		}
	case LOAD_DATA:
		if o.Statement.FieldsEnclosedByLd == "" {
			o.global.fields_enclosed_by = ""
			o.Statement.FieldsEnclosedByLd = o.global.fields_enclosed_by
		} else {
			o.global.fields_enclosed_by = o.Statement.FieldsEnclosedByLd
		}
		if o.global.fields_escaped_by != "" {
			if strings.Compare(o.global.fields_escaped_by, "\\") == 0 {
				o.global.fields_escaped_by = "\\\\"
			}
		} else {
			o.global.fields_escaped_by = "\\\\"
		}
		if o.Statement.FieldsTerminatedByLd == "" {
			o.global.fields_terminated_by = "\t"
			o.Statement.FieldsTerminatedByLd = "\\t"
		} else if strings.Compare(o.Statement.FieldsTerminatedByLd, "\\t") == 0 {
			o.global.fields_terminated_by = "\t"
			o.Statement.FieldsTerminatedByLd = "\\t"
		} else {
			o.global.fields_terminated_by = replace_escaped_strings(o.Statement.FieldsTerminatedByLd)
		}
		if o.Statement.LinesStartingByLd == "" {
			o.Statement.LinesStartingByLd = ""
			o.Statement.LinesStartingByLd = o.global.lines_starting_by
		} else {
			o.global.lines_starting_by = replace_escaped_strings(o.Statement.LinesStartingByLd)
		}
		if o.Statement.LinesTerminatedByLd == "" {
			o.global.lines_terminated_by = "\n"
			o.Statement.LinesTerminatedByLd = "\\n"
		} else {
			o.global.lines_terminated_by = replace_escaped_strings(o.Statement.LinesTerminatedByLd)
		}
		if o.Statement.StatementTerminatedByLd == "" {
			o.global.statement_terminated_by = ""
			o.Statement.StatementTerminatedByLd = o.global.statement_terminated_by
		} else {
			o.global.statement_terminated_by = replace_escaped_strings(o.Statement.StatementTerminatedByLd)
		}

	case CSV:
		if o.Statement.FieldsEnclosedByLd == "" {
			o.global.fields_enclosed_by = "\""
			o.Statement.FieldsEnclosedByLd = o.global.fields_enclosed_by
		} else {
			o.global.fields_enclosed_by = o.Statement.FieldsEnclosedByLd
		}
		if o.global.fields_escaped_by != "" {
			if strings.Compare(o.global.fields_escaped_by, "\\") == 0 {
				o.global.fields_escaped_by = "\\\\"
			}
		} else {
			o.global.fields_escaped_by = "\\\\"
		}
		if o.Statement.FieldsTerminatedByLd == "" {
			o.global.fields_terminated_by = ","
			o.Statement.FieldsTerminatedByLd = o.global.fields_terminated_by
		} else if strings.Compare(o.Statement.FieldsTerminatedByLd, "\\t") == 0 {
			o.global.fields_terminated_by = "\t"
			o.Statement.FieldsTerminatedByLd = "\\t"
		} else {
			o.global.fields_terminated_by = replace_escaped_strings(o.Statement.FieldsTerminatedByLd)
		}
		if o.Statement.LinesStartingByLd == "" {
			o.global.lines_starting_by = ""
			o.Statement.LinesStartingByLd = o.global.lines_starting_by
		} else {
			o.global.lines_starting_by = replace_escaped_strings(o.Statement.LinesStartingByLd)
		}
		if o.Statement.LinesTerminatedByLd == "" {
			o.global.lines_terminated_by = "\n"
			o.Statement.LinesTerminatedByLd = "\\n"
		} else {
			o.global.lines_terminated_by = replace_escaped_strings(o.Statement.LinesTerminatedByLd)
		}
		if o.Statement.StatementTerminatedByLd == "" {
			o.global.statement_terminated_by = ""
			o.Statement.StatementTerminatedByLd = o.global.statement_terminated_by
		} else {
			o.global.statement_terminated_by = replace_escaped_strings(o.Statement.StatementTerminatedByLd)

		}
	}

	if o.Statement.InsertIgnore && o.Statement.Replace {
		log.Errorf("You can't use --insert-ignore and --replace at the same time")
	}

	if o.Statement.InsertIgnore {
		o.global.insert_statement = INSERT_IGNORE
	}
	if o.Statement.Replace {
		o.global.insert_statement = REPLACE
	}
}

func finalize_write(o *OptionEntries) {
	o.global.fields_terminated_by = ""
	o.global.lines_starting_by = ""
	o.global.lines_terminated_by = ""
	o.global.statement_terminated_by = ""
}

func append_load_data_columns(o *OptionEntries, statement *strings.Builder, fields []*mysql.Field, num_fields uint) *strings.Builder {
	var i uint
	var str *strings.Builder = new(strings.Builder)
	var appendable bool
	str.WriteString("SET ")
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			statement.WriteString(",")
		}
		if fields[i].Type == mysql.MYSQL_TYPE_JSON {
			statement.WriteString("@")
			statement.WriteString(string(fields[i].Name))
			if str.Len() > 4 {
				str.WriteString(",")
			}
			str.WriteString(o.global.identifier_quote_character_str)
			str.Write(fields[i].Name)
			str.WriteString(o.global.identifier_quote_character_str)
			str.WriteString("=CONVERT(@")
			str.Write(fields[i].Name)
			str.WriteString(" USING UTF8MB4)")
			appendable = true
		} else if o.Statement.HexBlob && fields[i].Type == mysql.MYSQL_TYPE_BLOB {
			statement.WriteString("@")
			statement.Write(fields[i].Name)
			if str.Len() > 4 {
				str.WriteString(",")
			}
			str.WriteString(o.global.identifier_quote_character_str)
			str.Write(fields[i].Name)
			str.WriteString(o.global.identifier_quote_character_str)
			str.WriteString("=UNHEX(@")
			str.Write(fields[i].Name)
			str.WriteString(")")
			appendable = true
		} else {
			str.WriteString(o.global.identifier_quote_character_str)
			str.Write(fields[i].Name)
			str.WriteString(o.global.identifier_quote_character_str)
		}
	}
	if appendable {
		return str
	} else {
		return nil
	}
}

func append_columns(o *OptionEntries, statement *strings.Builder, fields []*mysql.Field, num_fields uint) {
	var i uint
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			statement.WriteString(",")
		}
		statement.WriteString(o.Common.IdentifierQuoteCharacter)
		statement.Write(fields[i].Name)
		statement.WriteString(o.Common.IdentifierQuoteCharacter)
	}

}

func build_insert_statement(o *OptionEntries, dbt *db_table, fields []*mysql.Field, num_fields uint) {
	var i_s *strings.Builder = new(strings.Builder)
	i_s.WriteString(o.global.insert_statement)
	i_s.WriteString(" INTO ")
	i_s.WriteString(o.Common.IdentifierQuoteCharacter)
	i_s.WriteString(dbt.table)
	i_s.WriteString(o.Common.IdentifierQuoteCharacter)
	if dbt.columns_on_insert != "" {
		i_s.WriteString(" (")
		i_s.WriteString(dbt.columns_on_insert)
		i_s.WriteString(")")
	} else {
		if dbt.complete_insert {
			i_s.WriteString(" (")
			append_columns(o, i_s, fields, num_fields)
			i_s.WriteString(")")
		}
	}
	i_s.WriteString(" VALUES ")
	dbt.insert_statement = i_s
}

func real_write_data(file *file_write, filesize *float64, data *strings.Builder) bool {
	var written int
	var r int
	var err error
	var second_write_zero bool
	for written < data.Len() {
		r, err = file.write([]byte(data.String()))
		if err != nil {
			log.Errorf("Couldn't write data to a file: %v", err)
			return false
		}
		if r == 0 {
			if second_write_zero {
				log.Errorf("Couldn't write data to a file: %v", err)
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

func write_data(file *file_write, data *strings.Builder) bool {
	var f float64
	return real_write_data(file, &f, data)
}

func initialize_load_data_statement_suffix(o *OptionEntries, dbt *db_table, fields []*mysql.Field, num_fields uint) {
	var character_set string
	if o.Statement.SetNamesStr != "" {
		character_set = o.Statement.SetNamesStr
	} else {
		character_set = dbt.character_set
	}
	var load_data_suffix *strings.Builder = new(strings.Builder)
	load_data_suffix.WriteString(fmt.Sprintf("%s' INTO TABLE %s%s%s ", o.Exec.ExecPerThreadExtension, o.global.identifier_quote_character_str, dbt.table,
		o.global.identifier_quote_character_str))
	if character_set != "" && len(character_set) != 0 {
		load_data_suffix.WriteString(fmt.Sprintf("CHARACTER SET %s ", character_set))
	}
	if o.Statement.FieldsTerminatedByLd != "" {
		load_data_suffix.WriteString(fmt.Sprintf("FIELDS TERMINATED BY '%s' ", o.Statement.FieldsTerminatedByLd))
	}
	if o.Statement.FieldsEnclosedByLd != "" {
		load_data_suffix.WriteString(fmt.Sprintf("ENCLOSED BY '%s' ", o.Statement.FieldsEnclosedByLd))
	}
	if o.global.fields_escaped_by != "" {
		load_data_suffix.WriteString(fmt.Sprintf("ESCAPED BY '%s' ", o.global.fields_escaped_by))
	}
	load_data_suffix.WriteString("LINES ")
	if o.Statement.LinesStartingByLd != "" {
		load_data_suffix.WriteString(fmt.Sprintf("STARTING BY '%s' ", o.Statement.LinesStartingByLd))
	}
	load_data_suffix.WriteString(fmt.Sprintf("TERMINATED BY '%s' ", o.Statement.LinesTerminatedByLd))
	if o.Statement.IncludeHeader {
		load_data_suffix.WriteString("IGNORE 1 LINES ")
	}
	load_data_suffix.WriteString("(")
	if dbt.columns_on_insert != "" {
		load_data_suffix.WriteString(dbt.columns_on_insert)
		load_data_suffix.WriteString(")")
	} else {
		var set_statement = append_load_data_columns(o, load_data_suffix, fields, num_fields)
		load_data_suffix.WriteString(")")
		if set_statement != nil {
			load_data_suffix.WriteString(set_statement.String())
		}
	}
	load_data_suffix.WriteString(";\n")
	dbt.load_data_suffix = load_data_suffix
}

func initialize_clickhouse_statement_suffix(o *OptionEntries, dbt *db_table, fields []*mysql.Field, num_fields uint) {
	var character_set string
	if o.Statement.SetNamesStr != "" {
		character_set = o.Statement.SetNamesStr
	} else {
		character_set = dbt.character_set
	}
	dbt.insert_statement = new(strings.Builder)
	dbt.load_data_suffix.WriteString(fmt.Sprintf("%s' INTO TABLE %s%s%s ", o.Exec.ExecPerThreadExtension, o.global.identifier_quote_character_str, dbt.table,
		o.global.identifier_quote_character_str))
	if character_set != "" && len(character_set) != 0 {
		dbt.load_data_suffix.WriteString(fmt.Sprintf("CHARACTER SET %s ", character_set))
	}
	if o.Statement.FieldsTerminatedByLd != "" {
		dbt.load_data_suffix.WriteString(fmt.Sprintf("FIELDS TERMINATED BY '%s' ", o.Statement.FieldsTerminatedByLd))
	}
	if o.Statement.FieldsEnclosedByLd != "" {
		dbt.load_data_suffix.WriteString(fmt.Sprintf("ENCLOSED BY '%s' ", o.Statement.FieldsEnclosedByLd))
	}
	if o.global.fields_escaped_by != "" {
		dbt.load_data_suffix.WriteString(fmt.Sprintf("ESCAPED BY '%s' ", o.global.fields_escaped_by))
	}
	dbt.load_data_suffix.WriteString("LINES ")
	if o.Statement.LinesStartingByLd != "" {
		dbt.load_data_suffix.WriteString(fmt.Sprintf("STARTING BY '%s' ", o.Statement.LinesStartingByLd))
	}
	dbt.load_data_suffix.WriteString(fmt.Sprintf("TERMINATED BY '%s' ", o.Statement.LinesTerminatedByLd))
	if o.Statement.IncludeHeader {
		dbt.load_data_suffix.WriteString("IGNORE 1 LINES ")
	}
	dbt.load_data_suffix.WriteString("(")
	if dbt.columns_on_insert != "" {
		dbt.load_data_suffix.WriteString(dbt.columns_on_insert)
		dbt.load_data_suffix.WriteString(")")
	} else {
		var set_statement = append_load_data_columns(o, dbt.load_data_suffix, fields, num_fields)
		dbt.load_data_suffix.WriteString(")")
		if set_statement != nil {
			dbt.load_data_suffix.WriteString(set_statement.String())
		}
	}
	dbt.load_data_suffix.WriteString(";\n")
}

func initialize_load_data_header(o *OptionEntries, dbt *db_table, fields []*mysql.Field, num_fields uint) {
	dbt.load_data_header = new(strings.Builder)
	var i uint
	for i = 0; i < num_fields-1; i++ {
		dbt.load_data_header.WriteString(o.global.fields_enclosed_by)
		dbt.load_data_header.Write(fields[i].Name)
		dbt.load_data_header.WriteString(o.global.fields_enclosed_by)
		dbt.load_data_header.WriteString(o.global.fields_terminated_by)
	}
	dbt.load_data_header.WriteString(o.global.fields_enclosed_by)
	dbt.load_data_header.Write(fields[i].Name)
	dbt.load_data_header.WriteString(o.global.fields_enclosed_by)
	dbt.load_data_header.WriteString(o.global.fields_terminated_by)
}

func write_statement(load_data_file *file_write, filessize *float64, statement *strings.Builder, dbt *db_table) bool {
	if !real_write_data(load_data_file, filessize, statement) {
		log.Errorf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
		return false
	}
	statement.Reset()
	return true
}

func write_load_data_statement(o *OptionEntries, tj *table_job) {
	var statement *strings.Builder = new(strings.Builder)
	var basename = path.Base(tj.rows.filename)
	initialize_sql_statement(o, statement)
	statement.WriteString(fmt.Sprintf("%s%s%s", LOAD_DATA_PREFIX, basename, tj.dbt.load_data_suffix.String()))
	if !write_data(tj.sql.file, statement) {
		log.Fatalf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
	}
}

func write_clickhouse_statement(o *OptionEntries, tj *table_job) {
	var statement *strings.Builder = new(strings.Builder)
	var basename = path.Base(tj.rows.filename)
	initialize_sql_statement(o, statement)
	statement.WriteString(fmt.Sprintf("%s INTO %s%s%s FROM INFILE '%s' FORMAT MySQLDump;", o.global.insert_statement, o.global.identifier_quote_character_str,
		tj.dbt.table, o.global.identifier_quote_character_str, basename))
	if !write_data(tj.sql.file, statement) {
		log.Fatalf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
	}
}

func write_header(tj *table_job) bool {
	if tj.dbt.load_data_header != nil && !write_data(tj.rows.file, tj.dbt.load_data_header) {
		log.Errorf("Could not write header for %s.%s", tj.dbt.database.name, tj.dbt.table)
		return false
	}
	return true
}

func get_estimated_remaining_of(mlist *MList) uint64 {
	mlist.mutex.Lock()
	var total uint64
	for _, tl := range mlist.list {
		total += tl.estimated_remaining_steps
	}
	mlist.mutex.Unlock()
	return total
}

func get_estimated_remaining_of_all_chunks(o *OptionEntries) uint64 {
	return get_estimated_remaining_of(o.global.non_innodb_table) + get_estimated_remaining_of(o.global.innodb_table)
}

func write_load_data_column_into_string(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer) {
	_ = conn
	if column.Value() != nil {
		fun_ptr_i = nil
		// TODO
	}
	if column.Value() == nil {
		statement_row.WriteString("\\N")
	} else if field.Type == mysql.MYSQL_TYPE_BLOB && o.Statement.HexBlob {
		*escaped = hex.EncodeToString([]byte(*escaped))
		statement_row.WriteString(*escaped)
	} else if field.Type != mysql.MYSQL_TYPE_LONG && field.Type != mysql.MYSQL_TYPE_LONGLONG && field.Type != mysql.MYSQL_TYPE_INT24 && field.Type != mysql.MYSQL_TYPE_SHORT {
		statement_row.WriteString(o.global.fields_enclosed_by)
		*escaped = mysql.Escape(string(column.AsString()))
		// *escaped = strings.ReplaceAll(*escaped, "\\", o.Statement.Fields_escaped_by)
		*escaped = m_replace_char_with_char('\\', []rune(o.global.fields_escaped_by)[0], []rune(*escaped))
		*escaped = string(m_escape_char_with_char([]byte(o.global.fields_terminated_by), []byte(o.global.fields_escaped_by), []byte(*escaped)))
		statement_row.WriteString(*escaped)
		statement_row.WriteString(o.global.fields_enclosed_by)
	} else {
		// statement_row.WriteString(strconv.FormatInt(column.AsInt64(), 10))
		statement_row.WriteString(string(column.AsString()))
	}
}

func write_sql_column_into_string(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer) {
	if &column != nil {
		fun_ptr_i = nil
	}
	_ = conn
	if column.Value() == nil {
		statement_row.WriteString("NULL")
	} else if field.Type <= mysql.MYSQL_TYPE_INT24 {
		statement_row.WriteString(strconv.FormatInt(column.AsInt64(), 10))
	} else if length.ColumnLength == 0 {
		statement_row.WriteString(o.global.fields_enclosed_by)
		statement_row.WriteString(o.global.fields_enclosed_by)
	} else if field.Type == mysql.MYSQL_TYPE_BLOB || o.Statement.HexBlob {
		statement_row.WriteString("0x")
		*escaped = hex.EncodeToString(column.AsString())
		statement_row.WriteString(*escaped)
	} else {
		*escaped = mysql.Escape(string(column.AsString()))
		if field.Type == mysql.MYSQL_TYPE_JSON {
			statement_row.WriteString("CONVERT(")
		}
		statement_row.WriteString(o.global.fields_enclosed_by)
		statement_row.WriteString(*escaped)
		statement_row.WriteString(o.global.fields_enclosed_by)
		// statement_row.WriteString(fmt.Sprintf("%s%s%s", o.global.fields_enclosed_by, *escaped, o.global.fields_enclosed_by))
		if field.Type == mysql.MYSQL_TYPE_JSON {
			statement_row.WriteString(" USING UTF8MB4)")
		}
	}
}

func write_row_into_string(o *OptionEntries, conn *client.Conn, dbt *db_table, row []mysql.FieldValue, fields []*mysql.Field, lengths []*mysql.Field, num_fields uint, escaped *string, statement_row *strings.Builder, write_column_into_string func(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer)) {
	var i uint
	statement_row.WriteString(o.global.lines_starting_by)
	_ = dbt
	// var f = dbt.anonymized_function
	var p *function_pointer

	for i = 0; i < num_fields-1; i++ {
		/*if f == nil {
			p = new(function_pointer)
		} else {
			p = f[i]
		}*/
		write_column_into_string(o, conn, row[i], fields[i], lengths[i], escaped, statement_row, p)
		statement_row.WriteString(o.global.fields_terminated_by)

	}
	write_column_into_string(o, conn, row[i], fields[i], lengths[i], escaped, statement_row, p)
	statement_row.WriteString(o.global.lines_terminated_by)
}

func update_dbt_rows(dbt *db_table, num_rows uint64) {
	dbt.rows_lock.Lock()
	dbt.rows += num_rows
	dbt.rows_lock.Unlock()
}

func initiliaze_load_data_files(o *OptionEntries, tj *table_job, dbt *db_table) {
	o.global.m_close(o, tj.td.thread_id, tj.sql.file, tj.sql.filename, 1, dbt)
	o.global.m_close(o, tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
	tj.sql.file = nil
	tj.sql.file.status = 0
	tj.rows.file = nil
	tj.rows.file.status = 0
	tj.sql.filename = ""
	tj.rows.filename = ""
	if update_files_on_table_job(o, tj) {
		write_load_data_statement(o, tj)
		write_header(tj)
	}

}

func initiliaze_clickhouse_files(o *OptionEntries, tj *table_job, dbt *db_table) {
	o.global.m_close(o, tj.td.thread_id, tj.sql.file, tj.sql.filename, 1, dbt)
	o.global.m_close(o, tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
	tj.sql.file = nil
	tj.sql.file.status = 0
	tj.rows.file = nil
	tj.rows.file.status = 0

	tj.sql.filename = ""
	tj.rows.filename = ""

	if update_files_on_table_job(o, tj) {
		write_clickhouse_statement(o, tj)
		write_header(tj)
	}
}

func write_result_into_file(o *OptionEntries, conn *client.Conn, query string, result mysql.Result, tj *table_job) error {
	var dbt *db_table = tj.dbt
	var num_fields uint
	var escaped string
	var err error
	var fields []*mysql.Field
	var statement *strings.Builder = new(strings.Builder)
	var statement_row *strings.Builder = new(strings.Builder)
	var lengths []*mysql.Field
	var num_rows uint64
	var num_row_st uint64
	var write_column_into_string func(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer)
	switch o.global.output_format {
	case LOAD_DATA, CSV:
		write_column_into_string = write_load_data_column_into_string
		if dbt.load_data_suffix == nil {
			dbt.chunks_mutex.Lock()
			if dbt.load_data_suffix == nil {
				initialize_load_data_statement_suffix(o, tj.dbt, fields, num_fields)
			}
			if o.Statement.IncludeHeader {
				initialize_load_data_header(o, tj.dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if update_files_on_table_job(o, tj) {
			write_load_data_statement(o, tj)
			write_header(tj)
		}
	case CLICKHOUSE:
		write_column_into_string = write_sql_column_into_string
		if tj.rows.file.status == 0 {
			update_files_on_table_job(o, tj)
		}
		if dbt.load_data_suffix == nil {
			dbt.chunks_mutex.Lock()
			if dbt.load_data_suffix == nil {
				initialize_clickhouse_statement_suffix(o, tj.dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if dbt.insert_statement == nil {
			dbt.chunks_mutex.Lock()
			if dbt.insert_statement == nil {
				build_insert_statement(o, dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if tj.st_in_file == 0 {
			initialize_sql_statement(o, statement)
			write_clickhouse_statement(o, tj)
		}
		statement.WriteString(dbt.insert_statement.String())
	case SQL_INSERT:
		write_column_into_string = write_sql_column_into_string
		if tj.rows.file.status == 0 {
			update_files_on_table_job(o, tj)
		}
		if dbt.insert_statement == nil {
			dbt.chunks_mutex.Lock()
			if dbt.insert_statement == nil {
				build_insert_statement(o, dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		if tj.st_in_file == 0 {
			initialize_sql_statement(o, statement)
		}
		statement.WriteString(dbt.insert_statement.String())
	}
	o.global.message_dumping_data(o, tj)
	var from = time.Now()

	err = conn.ExecuteSelectStreaming(query, &result, func(row []mysql.FieldValue) error {
		num_rows++
		write_row_into_string(o, conn, dbt, row, fields, lengths, num_fields, &escaped, statement_row, write_column_into_string)
		if statement.Len()*statement_row.Len()+1 > o.Statement.StatementSize {
			if num_row_st == 0 {
				statement.WriteString(statement_row.String())
				statement_row.Reset()
				log.Warnf("Row bigger than statement_size for %s.%s", dbt.database.name, dbt.table)
			}
			statement.WriteString(o.global.statement_terminated_by)
			if !write_statement(tj.rows.file, &tj.filesize, statement, dbt) {
				return nil
			}
			update_dbt_rows(dbt, num_rows)
			num_rows = 0
			num_row_st = 0
			tj.st_in_file++
			if o.global.output_format == SQL_INSERT || o.global.output_format == CLICKHOUSE {
				statement.WriteString(dbt.insert_statement.String())
			}
			to := time.Now()
			diff := to.Sub(from).Seconds()
			if diff > 4 {
				from = to
				o.global.message_dumping_data(o, tj)
			}
			check_pause_resume(tj.td)
			if o.global.shutdown_triggered {
				return nil
			}
		}
		if dbt.chunk_filesize != 0 && uint(math.Ceil(tj.filesize/1024/1024)) > dbt.chunk_filesize {
			tj.sub_part++
			switch o.global.output_format {
			case LOAD_DATA, CSV:
				initiliaze_load_data_files(o, tj, dbt)
			case CLICKHOUSE:
				initiliaze_clickhouse_files(o, tj, dbt)
			case SQL_INSERT:
				o.global.m_close(o, tj.td.thread_id, tj.rows.file, tj.rows.filename, 1, dbt)
				tj.rows.file = nil
				tj.rows.file.status = 0
				update_files_on_table_job(o, tj)
				initiliaze_clickhouse_files(o, tj, dbt)
			}
		}
		tj.st_in_file = 0
		tj.filesize = 0
		if num_row_st != 0 && (o.global.output_format == SQL_INSERT || o.global.output_format == CLICKHOUSE) {
			statement.WriteString(",")
		}
		statement.WriteString(statement_row.String())
		if statement_row.Len() > 0 {
			num_row_st++
		}
		statement_row.Reset()
		return nil
	}, func(result *mysql.Result) error {
		lengths = result.Fields
		num_fields = uint(len(result.Fields))
		fields = result.Fields
		return nil
	})
	if err != nil {
		if !o.global.it_is_a_consistent_backup {
			log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s",
				tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, err, query)
			// try connection
			// TODO
			err = conn.Ping()
			if err != nil {
				conn, err = m_connect(o)
				if err != nil {
					return err
				} else {
					execute_gstring(conn, o.global.set_session)
				}
			}
			log.Warnf("Thread %d: Retrying last failed executed statement", tj.td.thread_id)
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s",
					tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, err, query)
			} else {
				log.Errorf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s",
					tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, err, query)
				return err
			}
		} else {
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s",
					tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, err, query)
			} else {
				log.Errorf("Thread %d: Error dumping table (%s.%s) data: %v\nQuery: %s",
					tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, err, query)
				return err
			}
		}
	}
	update_dbt_rows(dbt, num_rows)
	if num_row_st > 0 && statement.Len() > 0 {
		if o.global.output_format == SQL_INSERT || o.global.output_format == CLICKHOUSE {
			statement.WriteString(o.global.statement_terminated_by)
		}
		if !write_statement(tj.rows.file, &tj.filesize, statement, dbt) {
			return err
		}
		tj.st_in_file++
	}
	statement.Reset()
	escaped = ""
	statement_row.Reset()
	return err
}

func write_table_job_into_file(o *OptionEntries, tj *table_job) {
	var conn = tj.td.thrconn
	var err error
	var result mysql.Result
	var query string
	var cache, fields, where1, where_option1, where2, where_option2, where3, where_option3, order, limit string
	if o.is_mysql_like() {
		cache = "/*!40001 SQL_NO_CACHE */"
	}
	if tj.dbt.columns_on_select != "" {
		fields = tj.dbt.columns_on_select
	} else {
		fields = tj.dbt.select_fields
	}
	if tj.where != "" || o.Filter.WhereOption != "" || tj.dbt.where != "" {
		where1 = "WHERE"
	}
	if tj.where != "" {
		where_option1 = tj.where
	}
	if tj.where != "" && o.Filter.WhereOption != "" {
		where2 = "AND"
	}
	if o.Filter.WhereOption != "" {
		where_option2 = o.Filter.WhereOption
	}
	if (tj.where != "" || o.Filter.WhereOption != "") && tj.dbt.where != "" {
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
		o.global.identifier_quote_character_str, tj.dbt.database.name, o.global.identifier_quote_character_str,
		o.global.identifier_quote_character_str, tj.dbt.table, o.global.identifier_quote_character_str,
		tj.partition, where1, where_option1, where2, where_option2, where3, where_option3, order, tj.order_by, limit, tj.dbt.limit)
	err = write_result_into_file(o, conn, query, result, tj)
	if err != nil {
		log.Errorf("Thread %d: Could not read data from %s.%s to write on %s at byte %.0f: %v",
			tj.td.thread_id, tj.dbt.database.name, tj.dbt.table, tj.rows.filename, tj.filesize, err)
		err = conn.Ping()
		if err != nil {
			if !o.global.it_is_a_consistent_backup {
				log.Warnf("Thread %d: Reconnecting due errors", tj.td.thread_id)
				tj.td.thrconn, err = m_connect(o)
				if err != nil {
					log.Fatalf("Reconnect file: %v", err)
				}
				execute_gstring(tj.td.thrconn, o.global.set_session)
			}
		}
	}
}
