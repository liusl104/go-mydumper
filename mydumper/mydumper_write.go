package mydumper

import (
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"path"
	"strings"
)

var m_open func(*OptionEntries, *string, string) (*file_write, error)
var m_write func(file *file_write, buff string) (int, error)

func initialize_write(o *OptionEntries) {
	if o.global.rows_per_file > 0 && o.Extra.ChunkFilesize > 0 {
		log.Warnf("We are going to chunk by row and by filesize when possible")
	}
	o.global.fields_enclosed_by = "\""
	if o.Statement.Csv {
		o.Statement.LoadData = true
		if o.Statement.FieldsTerminatedByLd == "" {
			o.Statement.FieldsTerminatedByLd = ","
		}
		if o.Statement.FieldsEnclosedByLd == "" {
			o.Statement.FieldsEnclosedByLd = "\""
		}
		if o.Statement.FieldsEscapedBy == "" {
			o.Statement.FieldsEscapedBy = "\\"
		}

		if o.Statement.LinesTerminatedByLd == "" {
			o.Statement.LinesTerminatedByLd = "\\n"
		}
	}
	//  if (load_data){
	if o.Statement.FieldsEnclosedByLd == "" {
		if o.Statement.LoadData {
			o.global.fields_enclosed_by = ""
		}
		o.Statement.FieldsEnclosedByLd = o.global.fields_enclosed_by
	} else if len(o.Statement.FieldsEnclosedByLd) > 1 {
		log.Fatalf("--fields-enclosed-by must be a single character")
	} else {
		o.global.fields_enclosed_by = o.Statement.FieldsEnclosedByLd
	}

	if o.Statement.LoadData {
		if o.Statement.FieldsEscapedBy != "" {
			if len(o.Statement.FieldsEscapedBy) > 1 {
				log.Fatalf("--fields-escaped-by must be a single character")
			} else if strings.Compare(o.Statement.FieldsEscapedBy, "\\") == 0 {
				o.Statement.FieldsEscapedBy = "\\\\"
			}
		} else {
			o.Statement.FieldsEscapedBy = "\\\\"
		}
	}

	if o.Statement.FieldsTerminatedByLd == "" {
		if o.Statement.LoadData {
			o.global.fields_terminated_by = "\t"
			o.Statement.FieldsTerminatedByLd = "\\t"
		} else {
			o.global.fields_terminated_by = ","
		}

	} else if strings.Compare(o.Statement.FieldsTerminatedByLd, "\\t") == 0 {
		o.global.fields_terminated_by = "\t"
		o.Statement.FieldsTerminatedByLd = "\\t"
	} else {
		o.global.fields_terminated_by = replace_escaped_strings(o.Statement.FieldsTerminatedByLd)
	}

	if o.Statement.LinesStartingByLd == "" {
		if o.Statement.LoadData {
			o.global.lines_starting_by = ""
			o.Statement.LinesStartingByLd = o.global.lines_starting_by
		} else {
			o.global.lines_starting_by = "("
		}

	} else {
		o.global.lines_starting_by = replace_escaped_strings(o.Statement.LinesStartingByLd)
	}

	if o.Statement.LinesTerminatedByLd == "" {
		if o.Statement.LoadData {
			o.global.lines_terminated_by = "\n"
			o.Statement.LinesTerminatedByLd = "\\n"
		} else {
			o.global.lines_terminated_by = ")\n"
		}

	} else {
		o.global.lines_terminated_by = replace_escaped_strings(o.Statement.LinesTerminatedByLd)
	}

	if o.Statement.StatementTerminatedByLd == "" {
		if o.Statement.LoadData {
			o.global.statement_terminated_by = ""
			o.Statement.StatementTerminatedByLd = o.global.statement_terminated_by
		} else {
			o.global.statement_terminated_by = ";\n"
		}
	} else {
		o.global.statement_terminated_by = replace_escaped_strings(o.Statement.StatementTerminatedByLd)
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
	o.global.fields_enclosed_by = ""
	o.global.fields_terminated_by = ""
	o.global.lines_starting_by = ""
	o.global.lines_terminated_by = ""
}

func append_load_data_columns(statement *strings.Builder, fields []*mysql.Field, num_fields uint) string {
	var i uint
	var str = "SET "
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			statement.WriteString(",")
		}
		if fields[i].Type == mysql.MYSQL_TYPE_JSON {
			statement.WriteString("@")
			statement.WriteString(string(fields[i].Name))
			if len(str) > 4 {
				str += ","
			}
			str += fmt.Sprintf("`%s`=CONVERT(@%s USING UTF8MB4)", fields[i].Name, fields[i].Name)
		} else {
			statement.WriteString(fmt.Sprintf("`%s`", fields[i].Name))
		}
	}
	if len(str) > 4 {
		return str
	}
	return ""
}

func append_columns(o *OptionEntries, statement *string, fields []*mysql.Field, num_fields uint) {
	var i uint
	for i = 0; i < num_fields; i++ {
		if i > 0 {
			*statement += ","
		}
		*statement += fmt.Sprintf("%s%s%s", o.Common.IdentifierQuoteCharacter, fields[i].Name, o.Common.IdentifierQuoteCharacter)
	}

}

func build_insert_statement(o *OptionEntries, dbt *db_table, fields []*mysql.Field, num_fields uint) {
	dbt.insert_statement = o.global.insert_statement
	dbt.insert_statement += fmt.Sprintf(" INTO %s%s%s", o.Common.IdentifierQuoteCharacter, dbt.table, o.Common.IdentifierQuoteCharacter)
	if dbt.columns_on_insert != "" {
		dbt.insert_statement += fmt.Sprintf(" (%s)", dbt.columns_on_insert)
	} else {
		if dbt.complete_insert {
			dbt.insert_statement += fmt.Sprintf(" (")
			append_columns(o, &dbt.insert_statement, fields, num_fields)
			dbt.insert_statement += fmt.Sprintf(")")
		}
	}
	dbt.insert_statement += " VALUES "
}

func real_write_data(file *file_write, filesize *float64, data string) bool {
	n, err := m_write(file, data)
	if err != nil {
		log.Errorf("Couldn't write data to a file: %v", err)
		return false
	}
	*filesize += float64(n)
	return true
}

func write_data(file *file_write, data string) bool {
	var f float64
	return real_write_data(file, &f, data)
}

func initialize_load_data_statement(o *OptionEntries, statement *strings.Builder, dbt *db_table, basename string, fields []*mysql.Field, num_fields uint) {
	var character_set string
	if o.Statement.SetNamesStr != "" {
		character_set = o.Statement.SetNamesStr
	} else {
		character_set = dbt.character_set
	}
	statement.WriteString(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s%s' REPLACE INTO TABLE `%s` ", basename, o.Exec.ExecPerThreadExtension, dbt.table))
	if character_set != "" {
		statement.WriteString(fmt.Sprintf("CHARACTER SET %s ", character_set))
	}
	if o.Statement.FieldsTerminatedByLd != "" {
		statement.WriteString(fmt.Sprintf("FIELDS TERMINATED BY '%s' ", o.Statement.FieldsTerminatedByLd))
	}
	if o.Statement.FieldsEnclosedByLd != "" {
		statement.WriteString(fmt.Sprintf("ENCLOSED BY '%s' ", o.Statement.FieldsEnclosedByLd))
	}
	if o.Statement.FieldsEscapedBy != "" {
		statement.WriteString(fmt.Sprintf("ESCAPED BY '%s' ", o.Statement.FieldsEscapedBy))
	}
	statement.WriteString("LINES ")
	//if o.Statement.Lines_starting_by_ld != "" {
	statement.WriteString(fmt.Sprintf("STARTING BY '%s' ", o.Statement.LinesStartingByLd))
	//}
	statement.WriteString(fmt.Sprintf("TERMINATED BY '%s' (", o.Statement.LinesTerminatedByLd))
	if dbt.columns_on_insert != "" {
		statement.WriteString(dbt.columns_on_insert)
		statement.WriteString(")")
	} else {
		var set_statement = append_load_data_columns(statement, fields, num_fields)
		statement.WriteString(")")
		if set_statement != "" {
			statement.WriteString(set_statement)
		}
	}
	statement.WriteString(";\n")

}

func write_statement(load_data_file *file_write, filessize *float64, statement *strings.Builder, dbt *db_table) bool {
	if !real_write_data(load_data_file, filessize, statement.String()) {
		log.Errorf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
		return false
	}
	statement.Reset()
	return true
}

func write_load_data_statement(o *OptionEntries, tj *table_job, fields []*mysql.Field, num_fields uint) bool {
	var statement *strings.Builder
	var basename = path.Base(tj.dat_filename)
	initialize_sql_statement(o, statement)
	initialize_load_data_statement(o, statement, tj.dbt, basename, fields, num_fields)
	if !write_data(tj.sql_file, statement.String()) {
		log.Errorf("Could not write out data for %s.%s", tj.dbt.database.name, tj.dbt.table)
		return false
	}
	return true
}

func get_estimated_remaining_chunks_on_dbt(dbt *db_table) uint64 {
	var total uint64
	for _, l := range dbt.chunks {
		total += l.(*chunk_step).integer_step.estimated_remaining_steps
	}
	return total
}

func get_estimated_remaining_of(list []*db_table) uint64 {
	var total uint64
	for _, tl := range list {
		total += tl.estimated_remaining_steps
	}
	return total
}

func get_estimated_remaining_of_all_chunks(o *OptionEntries) uint64 {
	return get_estimated_remaining_of(o.global.non_innodb_table) + get_estimated_remaining_of(o.global.innodb_table)
}

func message_dumping_data(o *OptionEntries, td *thread_data, tj *table_job) {
	var where1, where_option1 string
	var where2, where_option2 string
	var where3, where_option3 string
	var order_by string
	if tj.order_by != "" {
		order_by = "ORDER BY"
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

	log.Infof("Thread %d: dumping data for `%s`.`%s` %s %s %s %s %s %s %s %s %s into %s| Remaining jobs in this table: %d All remaining jobs: %d",
		td.thread_id, tj.dbt.database.name, tj.dbt.table, tj.partition, where1, where_option1, where2, where_option2, where3, where_option3,
		order_by, tj.order_by, tj.sql_filename, tj.dbt.estimated_remaining_steps, get_estimated_remaining_of_all_chunks(o))
}

func write_load_data_column_into_string(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer) {
	_ = conn
	if &column != nil {
		fun_ptr_i = nil
		// TODO
	}
	if column.Value() == nil {
		statement_row.WriteString("\\N")
	} else if field.Type != mysql.MYSQL_TYPE_LONG && field.Type != mysql.MYSQL_TYPE_LONGLONG && field.Type != mysql.MYSQL_TYPE_INT24 && field.Type != mysql.MYSQL_TYPE_SHORT {
		statement_row.WriteString(o.global.fields_enclosed_by)
		*escaped = mysql.Escape(string(column.AsString()))
		// *escaped = strings.ReplaceAll(*escaped, "\\", o.Statement.Fields_escaped_by)
		*escaped = m_replace_char_with_char('\\', []rune(o.Statement.FieldsEscapedBy)[0], []rune(*escaped))
		*escaped = string(m_escape_char_with_char([]byte(o.global.fields_terminated_by), []byte(o.Statement.FieldsEscapedBy), []byte(*escaped)))
		statement_row.WriteString(*escaped)
		statement_row.WriteString(o.global.fields_enclosed_by)
	} else {
		statement_row.WriteString(fmt.Sprintf("%d", column.AsInt64()))
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
		statement_row.WriteString(fmt.Sprintf("%d", column.AsInt64()))
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
		statement_row.WriteString(fmt.Sprintf("%s%s%s", o.global.fields_enclosed_by, *escaped, o.global.fields_enclosed_by))
		if field.Type == mysql.MYSQL_TYPE_JSON {
			statement_row.WriteString(" USING UTF8MB4)")
		}
	}
}

func write_row_into_string(o *OptionEntries, conn *client.Conn, dbt *db_table, row []mysql.FieldValue, fields []*mysql.Field, lengths []*mysql.Field, num_fields uint, escaped *string, statement_row *strings.Builder, write_column_into_string func(o *OptionEntries, conn *client.Conn, column mysql.FieldValue, field *mysql.Field, length *mysql.Field, escaped *string, statement_row *strings.Builder, fun_ptr_i *function_pointer)) {
	var i uint
	statement_row.WriteString(o.global.lines_starting_by)
	_ = dbt
	var f = dbt.anonymized_function
	var p *function_pointer

	for i = 0; i < num_fields-1; i++ {
		if f == nil {
			p = new(function_pointer)
		} else {
			p = f[i]
		}
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

func write_row_into_file_in_load_data_mode(o *OptionEntries, conn *client.Conn, query string, tj *table_job) {
	var err error
	var result mysql.Result
	var dbt = tj.dbt
	var num_rows uint64
	var statement strings.Builder
	var statement_row strings.Builder
	var fields []*mysql.Field
	var lengths []*mysql.Field
	var escaped string
	var num_fields uint

	err = conn.ExecuteSelectStreaming(query, &result, func(row []mysql.FieldValue) error {
		// 后执行
		num_rows++
		if intToBool(int(dbt.chunk_filesize)) && uint(math.Ceil(tj.filesize/1024/1024)) > dbt.chunk_filesize {
			update_dbt_rows(dbt, num_rows)
			if !write_statement(tj.dat_file, &tj.filesize, &statement_row, dbt) {
				return nil
			}

			m_close(o, tj.td.thread_id, tj.sql_file, tj.sql_filename, 1, dbt)
			m_close(o, tj.td.thread_id, tj.dat_file, tj.dat_filename, 1, dbt)
			tj.sql_file = nil
			tj.dat_file = nil

			tj.sql_filename = ""
			tj.dat_filename = ""
			tj.sub_part++

			if update_files_on_table_job(o, tj) {
				write_load_data_statement(o, tj, fields, num_fields)
			}
			tj.st_in_file = 0
			tj.filesize = 0
			num_rows = 0
		}
		statement_row.Reset()
		write_row_into_string(o, conn, dbt, row, fields, lengths, num_fields, &escaped, &statement_row, write_load_data_column_into_string)
		tj.filesize += float64(statement_row.Len() + 1)
		statement.WriteString(statement_row.String())
		/* INSERT statement is closed before over limit but this is load data, so we only need to flush the data to disk*/
		if statement.Len() > o.Statement.StatementSize {
			if !write_statement(tj.dat_file, &(tj.filesize), &statement, dbt) {
				update_dbt_rows(dbt, num_rows)
				num_rows = 0
				return nil
			}
			check_pause_resume(tj.td)
			if o.global.shutdown_triggered {
				update_dbt_rows(dbt, num_rows)
				return nil
			}
		}
		return nil
	}, func(result *mysql.Result) error {
		// 先执行
		num_fields = uint(result.ColumnNumber())
		fields = result.Fields
		lengths = result.Fields
		if update_files_on_table_job(o, tj) {
			write_load_data_statement(o, tj, fields, num_fields)
		}
		message_dumping_data(o, tj.td, tj)
		return nil
	})
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping table (%s.%s) data: %v\nQuery: %s", tj.dbt.database.name, tj.dbt.table, err, query)
		} else {
			log.Fatalf("Error dumping table (%s.%s) data: %v\nQuery: %s", tj.dbt.database.name, tj.dbt.table, err, query)
		}
	}
	update_dbt_rows(dbt, num_rows)
	if statement.Len() > 0 {
		if !real_write_data(tj.dat_file, &(tj.filesize), statement.String()) {
			log.Fatalf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
			return
		}
	}
	return
}

func write_row_into_file_in_sql_mode(o *OptionEntries, conn *client.Conn, query string, tj *table_job) {
	var err error
	var result mysql.Result
	var dbt = tj.dbt
	var num_rows uint64
	var num_rows_st uint64
	var statement strings.Builder
	var statement_row strings.Builder
	var fields []*mysql.Field
	var lengths []*mysql.Field
	var escaped string
	var num_fields uint
	err = conn.ExecuteSelectStreaming(query, &result, func(row []mysql.FieldValue) error {
		num_rows++
		// 后执行
		if statement.Len() == 0 {
			if !intToBool(int(tj.st_in_file)) {
				initialize_sql_statement(o, &statement)
				if !real_write_data(tj.sql_file, &tj.filesize, statement.String()) {
					log.Fatalf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
					return err
				}
				statement.Reset()
			}
			dbt.chunks_mutex.Lock()
			statement.WriteString(dbt.insert_statement)
			dbt.chunks_mutex.Unlock()
			num_rows_st = 0
		}
		if statement_row.Len() != 0 {
			statement.WriteString(statement_row.String())
			statement_row.Reset()
			num_rows_st++
		}
		write_row_into_string(o, conn, dbt, row, fields, lengths, num_fields, &escaped, &statement_row, write_sql_column_into_string)
		if statement.Len()+statement_row.Len()+1 > o.Statement.StatementSize || (dbt.chunk_filesize > 0 && uint(math.Ceil(tj.filesize)/1024/1024) > dbt.chunk_filesize) {
			update_dbt_rows(dbt, num_rows)
			if num_rows_st == 0 {
				statement.WriteString(statement_row.String())
				statement_row.Reset()
				log.Warnf("Row bigger than statement_size for %s.%s", dbt.database.name, dbt.table)
			}
			statement.WriteString(o.global.statement_terminated_by)
			if !real_write_data(tj.sql_file, &tj.filesize, statement.String()) {
				log.Fatalf("Could not write out data for %s.%s", dbt.database.name, dbt.table)
			}
			tj.st_in_file++
			if intToBool(int(dbt.chunk_filesize)) && uint(math.Ceil(tj.filesize)/1024/1024) > dbt.chunk_filesize {
				tj.sub_part++
				m_close(o, tj.td.thread_id, tj.sql_file, tj.sql_filename, 1, dbt)
				tj.sql_file = nil
				update_files_on_table_job(o, tj)
				tj.st_in_file = 0
				tj.filesize = 0
			}
			num_rows = 0
			check_pause_resume(tj.td)
			if o.global.shutdown_triggered {
				return nil
			}
			statement.Reset()

		} else {
			if intToBool(int(num_rows_st)) {
				statement.WriteString(",")
			}
			statement.WriteString(statement_row.String())
			num_rows_st++
			statement_row.Reset()
		}
		return nil
	}, func(result *mysql.Result) error {
		// 先执行
		num_fields = uint(result.ColumnNumber())
		lengths = result.Fields
		fields = result.Fields
		update_files_on_table_job(o, tj)
		message_dumping_data(o, tj.td, tj)
		if dbt.insert_statement == "" {
			dbt.chunks_mutex.Lock()
			if dbt.insert_statement == "" {
				build_insert_statement(o, dbt, fields, num_fields)
			}
			dbt.chunks_mutex.Unlock()
		}
		return nil
	})
	if statement_row.Len() != 0 {
		if statement.Len() == 0 {
			statement.WriteString(dbt.insert_statement)
		}
		statement.WriteString(statement_row.String())
	}
	update_dbt_rows(dbt, num_rows)
	if statement.Len() != 0 {
		statement.WriteString(o.global.statement_terminated_by)
		if !real_write_data(tj.sql_file, &tj.filesize, statement.String()) {
			log.Fatalf("Could not write out closing newline for %s.%s, now this is sad!", dbt.database.name, dbt.table)
			return
		}
		tj.st_in_file++
	}
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping table (%s.%s) data: %v\nQuery: %s", tj.dbt.database.name, tj.dbt.table, err, query)
		} else {
			log.Fatalf("Error dumping table (%s.%s) data: %v\nQuery: %s", tj.dbt.database.name, tj.dbt.table, err, query)
		}
	}
}

func write_table_job_into_file(o *OptionEntries, conn *client.Conn, tj *table_job) {
	var cache, fields, where1, where_option1, where2, where_option2, where3, where_option3, order, limit string
	var query string
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
	err := conn.UseDB(tj.dbt.database.name)
	if err != nil {
		log.Errorf("Could not use DB '%s': %v", tj.dbt.database.name, err)
		return
	}
	query = fmt.Sprintf("SELECT %s %s FROM `%s` %s %s %s %s %s %s %s %s %s %s %s", cache, fields, tj.dbt.table, tj.partition, where1, where_option1, where2, where_option2, where3, where_option3, order, tj.order_by, limit, tj.dbt.limit)
	if o.Statement.LoadData {
		write_row_into_file_in_load_data_mode(o, conn, query, tj)
	} else {
		write_row_into_file_in_sql_mode(o, conn, query, tj)
	}
}
