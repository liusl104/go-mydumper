package mydumper

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"strings"
	"sync"
)

var (
	IgnoreGeneratedFields            bool
	OrderByPrimaryKey                bool
	Exec_per_thread                  string
	ExecPerThreadExtension           string
	case_sensitive_prefix            string
	case_sensitive_suffix            string
	table_engine_for_view_dependency string
	DumpTriggers                     bool
	ignore_generated_fields          bool
	SkipDefiner                      bool
	exec_per_thread_cmd              []string
	m_open                           func(filename *string, mode string) (*file_write, error)
	m_close                          func(thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error
)

type checksum_fun func(conn *DBConnection, database, table string) string
type build_filename_fun func(dump_directory, database, table string, part uint64, sub_part uint) string

type schema_metadata_job struct {
	metadata_file        *os.File
	release_binlog_mutex *sync.Mutex
}

type schema_job struct {
	dbt                     *db_table
	filename                string
	checksum_filename       bool
	checksum_index_filename bool
}

type sequence_job struct {
	dbt               *db_table
	filename          string
	checksum_filename bool
}

type table_checksum_job struct {
	dbt      *db_table
	filename string
}

type create_tablespace_job struct {
	filename string
}

type database_job struct {
	database          *database
	filename          string
	checksum_filename bool
}

type view_job struct {
	dbt                *db_table
	tmp_table_filename string
	view_filename      string
	checksum_filename  bool
}

// initialize_jobs calls initialize_database and logs a warning if IgnoreGeneratedFields is set.
func initialize_jobs() {
	initialize_database()
	if IgnoreGeneratedFields {
		log.Warnf("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	}
}

// write_checksum_into_file runs the checksum function for the table and returns the checksum string (or "0" if empty).
func write_checksum_into_file(conn *DBConnection, database *database, table string, fun checksum_fun) string {
	checksum := fun(conn, database.name, table)
	if checksum == "" {
		checksum = "0"
	}
	return checksum
}

// get_tablespace_query returns the SQL to list tablespaces for the current server version, or empty string if unsupported.
func get_tablespace_query() string {
	if Server_support_tablespaces() {
		if Get_major() == 5 && Get_secondary() == 7 {
			return "select NAME, PATH, FS_BLOCK_SIZE from information_schema.INNODB_SYS_TABLESPACES join information_schema.INNODB_SYS_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';"
		}
		if Get_major() == 8 {
			return "select NAME,PATH,FS_BLOCK_SIZE,ENCRYPTION from information_schema.INNODB_TABLESPACES join information_schema.INNODB_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';"
		}
	}
	return ""
}

// write_tablespace_definition_into_file dumps CREATE TABLESPACE statements to the given filename.
func write_tablespace_definition_into_file(conn *DBConnection, filename string) {
	var query string
	var outfile *file_write
	var err error
	var row []FieldValue
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: Could not create output file %s (%v)", filename, err)
		Errors++
		return
	}
	query = get_tablespace_query()
	if query == "" {
		log.Warnf("Tablespace resquested, but not possible due to server version not supported")
		return
	}
	var result *MYSQL_RES
	result = M_store_result_critical(conn, query, "Error dumping create tablespace")
	if result == nil {
		return
	}
	var statement = G_string_sized_new(StatementSize)
	initialize_sql_statement(statement)
	for row = Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		G_string_append_printf(statement, "CREATE TABLESPACE %s%s%s ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", Identifier_quote_character, row[0].AsString(), Identifier_quote_character,
			row[1].AsString(), row[2].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write tablespace data for %s", row[0].AsString())
			Errors++
			return
		}
		G_string_set_size(statement, 0)
	}
}

// write_schema_definition_into_file writes SHOW CREATE DATABASE to the given file.
func write_schema_definition_into_file(conn *DBConnection, database *database, filename string) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		Errors++
		return
	}
	var statement = G_string_sized_new(StatementSize)
	initialize_sql_statement(statement)
	query = fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS %s%s%s", Identifier_quote_character, database.name, Identifier_quote_character)
	var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Error dumping create database (%s)", database.name)
	if mr == nil {
		M_store_result_row_free(mr)
		return
	}
	if mr.Row == nil || !strings.Contains(string(mr.Row[1].AsString()), Identifier_quote_character_str) {
		log.Criticalf("Identifier quote [%s] not found when fetching %s", Identifier_quote_character_str, database.name)
		Errors++
	}
	G_string_append(statement, string(mr.Row[1].AsString()))
	G_string_append(statement, ";\n")

	if !write_data(outfile, statement) {
		log.Criticalf("Could not write create database for %s", database.name)
		Errors++
	}
	err = m_close(0, outfile, filename, 1, nil)
	M_store_result_row_free(mr)
	if SchemaChecksums {
		database.schema_checksum = write_checksum_into_file(conn, database, "", Checksum_database_defaults)
	}
	return
}

// write_table_definition_into_file dumps SHOW CREATE TABLE (and optional checksums) to the given file.
func write_table_definition_into_file(conn *DBConnection, dbt *db_table, filename string, checksum_filename bool, checksum_index_filename bool) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		Errors++
		return
	}

	var statement = G_string_sized_new(StatementSize)

	initialize_header_in_gstring(statement, SetNamesInFileForSct)
	if strings.EqualFold(SetNamesInConnForSct, AUTO_CHARSET) {
		if dbt.character_set != "" {
			Execute_set_names(conn, dbt.character_set)
		}
	} else {
		Execute_set_names(conn, SetNamesInConnForSct)
	}
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		Errors++
		return
	}
	query = fmt.Sprintf("SHOW CREATE TABLE %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	var mr *M_ROW = M_store_result_row(conn, query, M_critical, M_warning, "Error dumping schemas (%s.%s)", dbt.database.name, dbt.table)
	if mr.Res == nil {
		M_store_result_row_free(mr)
		Execute_set_names(conn, Set_names_in_conn_by_default)
		return
	}
	G_string_set_size(statement, 0)
	if Schema_sequence_fix {
		var create_table string
		create_table = Filter_sequence_schemas(string(mr.Row[1].AsString()))
		G_string_append(statement, create_table)
	} else {
		G_string_append(statement, string(mr.Row[1].AsString()))
	}
	M_store_result_row_free(mr)
	G_string_append(statement, ";\n")
	var alter_table_statement = G_string_sized_new(StatementSize)
	var alter_table_constraint_statement = G_string_sized_new(StatementSize)
	var create_table_statement = G_string_sized_new(StatementSize)
	var flag = Global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.table, true)
	if (flag&IS_TRX_TABLE) == 0 && TrxTables != 0 && SyncThreadLockMode != NO_LOCK {
		log.Errorf("Non transactional table found: `%s`.`%s` on a consitent backup attempt. Restart backup using --trx-tables=0 to indicate that you have non transactional tables.", dbt.database.name, dbt.table)
	}
	if SkipIndexes || SkipConstraints {
		if !write_data(outfile, create_table_statement) {
			log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
			Errors++
		}
		if !SkipIndexes {
			write_data(outfile, alter_table_statement)
		}
		if !SkipConstraints {
			write_data(outfile, alter_table_constraint_statement)
		}
	} else {
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
			Errors++
		}
	}

	m_close(0, outfile, filename, 1, dbt)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_table_structure)
	}
	if checksum_index_filename {
		dbt.indexes_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_table_indexes)
	}

}

// write_triggers_definition_into_file writes trigger CREATE statements from the result set to outfile.
func write_triggers_definition_into_file(conn *DBConnection, result *MYSQL_RES, database *database, message string, outfile *file_write) {
	var row []FieldValue
	var query string
	var statement = G_string_sized_new(StatementSize)
	var create_trigger = G_string_sized_new(StatementSize)
	var splited_st []string
	initialize_sql_statement(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write triggers for %s", message)
		Errors++
		return
	}
	for row = Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		set_charset(statement, row[8].AsString(), row[9].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write triggers data for %s", message)
			Errors++
			return
		}
		G_string_set_size(statement, 0)
		query = fmt.Sprintf("SHOW CREATE TRIGGER %s%s%s.%s%s%s", Identifier_quote_character, database.name, Identifier_quote_character, Identifier_quote_character, row[0].AsString(), Identifier_quote_character)
		var mr *M_ROW = M_store_result_single_row(conn, query, "Failed to execute SHOW CREATE TRIGGER %s.%s", database.name, row[0].AsString())
		var create string
		if mr.Row != nil {
			if SkipDefiner && strings.HasPrefix(string(row[2].AsString()), "CREATE") {
				create = Remove_definer_from_gchar(string(row[2].AsString()))
			}
			G_string_append_printf(statement, "DROP TRIGGER IF EXISTS %s%s%s;\n", Identifier_quote_character, row[0].AsString(), Identifier_quote_character)
			G_string_set_size(create_trigger, 0)
			G_string_append_printf(create_trigger, "%s", create)
			splited_st = strings.Split(create_trigger.Str.String(), ";\n")
			G_string_printf(create_trigger, "%s", strings.Join(splited_st, "; \n"))
			G_string_append(statement, create_trigger.Str.String())
			G_string_append(statement, ";\n")
			restore_charset(statement)
			if !write_data(outfile, statement) {
				log.Criticalf("Could not write triggers data for %s", message)
				Errors++
				return
			}
		}
		M_store_result_row_free(mr)
		G_string_set_size(statement, 0)
	}
	return
}

// write_triggers_definition_into_file_from_dbt dumps triggers for the table to filename (with optional checksum).
func write_triggers_definition_into_file_from_dbt(conn *DBConnection, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *MYSQL_RES
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		Errors++
		return
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", Identifier_quote_character, dbt.database.name, Identifier_quote_character, dbt.table)
	result = M_store_result_critical(conn, query, "Error dumping triggers (%s.%s)", dbt.database.name, dbt.table)
	if result == nil {
		return
	}
	var message = fmt.Sprintf("%s.%s", dbt.database.name, dbt.table)
	write_triggers_definition_into_file(conn, result, dbt.database, message, outfile)
	err = m_close(0, outfile, filename, 1, dbt)
	if result != nil {
		Mysql_free_result(result)
	}
	if checksum_filename {
		dbt.triggers_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_trigger_structure)
	}
	return
}

// write_triggers_definition_into_file_from_database dumps all triggers for the database to filename.
func write_triggers_definition_into_file_from_database(conn *DBConnection, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var result *MYSQL_RES
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		Errors++
		return
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s", Identifier_quote_character, database.name, Identifier_quote_character)
	result = M_store_result_critical(conn, query, "Error dumping triggers (%s)", database.name)
	if result != nil {
		write_triggers_definition_into_file(conn, result, database, database.name, outfile)
		Mysql_free_result(result)
		err = m_close(0, outfile, filename, 1, nil)
		if checksum_filename {
			database.triggers_checksum = write_checksum_into_file(conn, database, "", Checksum_trigger_structure_from_database)
		}
	}
	return
}

// write_view_definition_into_file dumps the view's temporary table and view CREATE to the given files.
func write_view_definition_into_file(conn *DBConnection, dbt *db_table, tmp_table_filename string, view_filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var statement = G_string_sized_new(StatementSize)
	var result *MYSQL_RES
	var row []FieldValue
	var err error
	initialize_sql_statement(statement)
	if !conn.UseDB(dbt.database.name) {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, conn.Err)
		Errors++
		return
	}
	outfile, err = m_open(&tmp_table_filename, "w")
	if outfile == nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, nil)
		Errors++
		return
	}

	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		Errors++
		return
	}
	query = fmt.Sprintf("SHOW FIELDS FROM %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	result = M_store_result_critical(conn, query, "Error dumping schemas (%s.%s)", dbt.database.name, dbt.table)
	if result == nil {
		return
	}
	G_string_set_size(statement, 0)
	G_string_append_printf(statement, "CREATE TABLE IF NOT EXISTS %s%s%s(\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	row = Mysql_fetch_row(result)
	G_string_append_printf(statement, "%s%s%s int", Identifier_quote_character, row[0].AsString(), Identifier_quote_character)
	for row = Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		G_string_append(statement, ",\n")
		G_string_append_printf(statement, "%s%s%s int", Identifier_quote_character, row[0].AsString(), Identifier_quote_character)
	}

	G_string_append(statement, "\n) ENGINE=")
	G_string_append(statement, table_engine_for_view_dependency)
	if Get_product() == SERVER_TYPE_PERCONA || Get_product() == SERVER_TYPE_MYSQL || Get_product() == SERVER_TYPE_RDS || Get_product() == SERVER_TYPE_DOLT {
		G_string_append(statement, " ENCRYPTION='N'")
	}
	G_string_append(statement, ";\n")
	if result != nil {
		Mysql_free_result(result)
	}
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write view schema for %s.%s", dbt.database.name, dbt.table)
		Errors++
	}
	err = m_close(0, outfile, tmp_table_filename, 1, dbt)
	G_string_set_size(statement, 0)
	query = fmt.Sprintf("SHOW CREATE VIEW %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	var mr *M_ROW = M_store_result_single_row(conn, query, "Error dumping view (%s.%s)", dbt.database.name, dbt.table)
	if mr.Res == nil || mr.Row == nil {
		M_store_result_row_free(mr)
		return
	}

	outfile, err = m_open(&view_filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		Errors++
		return
	}
	initialize_sql_statement(statement)

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		Errors++
		return
	}
	G_string_set_size(statement, 0)
	set_charset(statement, mr.Row[2].AsString(), mr.Row[3].AsString())
	var create string
	if SkipDefiner && strings.HasPrefix(string(row[1].AsString()), "CREATE") {
		create = Remove_definer_from_gchar(string(row[1].AsString()))
	}
	G_string_append(statement, create)
	G_string_append(statement, ";\n")
	restore_charset(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		Errors++
	}
	err = m_close(0, outfile, view_filename, 1, dbt)
	M_store_result_row_free(mr)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_view_structure)
	}
	return
}

// write_sequence_definition_into_file dumps the sequence definition (e.g. CREATE TABLE for sequence) to filename.
func write_sequence_definition_into_file(conn *DBConnection, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var statement = G_string_sized_new(StatementSize)
	var err error
	initialize_sql_statement(statement)
	conn.UseDB(dbt.database.name)

	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		Errors++
		return
	}

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		Errors++
		return
	}

	query = fmt.Sprintf("SHOW CREATE SEQUENCE %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	var mr = M_store_result_row(conn, query, M_critical, M_warning, "Error dumping schemas (%s.%s)", dbt.database.name, dbt.table)
	if mr.Res == nil {
		M_store_result_row_free(mr)
		return
	}
	G_string_set_size(statement, 0)
	var create string = string(mr.Row[1].AsString())
	if SkipDefiner && strings.HasPrefix(string(mr.Row[1].AsString()), "CREATE") {
		create = Remove_definer_from_gchar(string(mr.Row[1].AsString()))
	}
	G_string_append(statement, create)
	G_string_append(statement, ";\n")
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		Errors++
	}
	M_store_result_row_free(mr)
	query = fmt.Sprintf("SELECT next_not_cached_value FROM %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	mr = M_store_result_row(conn, query, M_critical, M_warning, "Error dumping schemas (%s.%s)", dbt.database.name, dbt.table)
	G_string_set_size(statement, 0)
	if mr.Row != nil {
		G_string_printf(statement, "DO SETVAL(%s%s%s, %s, 0);\n", Identifier_quote_character, dbt.table, Identifier_quote_character, mr.Row[0].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
			Errors++
		}
		err = m_close(0, outfile, filename, 1, dbt)
		if checksum_filename {
			write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_table_structure)
		}
	}
	M_store_result_row_free(mr)
	return
}

// write_routines_definition_into_file dumps stored procedures and functions for the database to filename.
func write_routines_definition_into_file(conn *DBConnection, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *MYSQL_RES
	var splited_st []string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		Errors++
		return
	}
	var statement = G_string_sized_new(StatementSize)
	initialize_sql_statement(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write %s", filename)
		Errors++
		return
	}
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write %s", filename)
		Errors++
		return
	}
	var charcol, collcol uint
	if DumpRoutines {
		var mr *M_ROW
		G_assert(nroutines > 0)
		var r uint
		for r = 0; r < nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE %s Db %s = '%s'", routine_type[r], case_sensitive_prefix, case_sensitive_suffix, database.escaped)
			result = M_store_result_critical(conn, query, "Error dumping %s from %s", routine_type[r], database.name)
			if result == nil {
				return
			}
			determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)

			for row := Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
				set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
				G_string_append_printf(statement, "DROP %s IF EXISTS %s%s%s;\n", routine_type[r], Identifier_quote_character, row[1].AsString(), Identifier_quote_character)
				if !write_data(outfile, statement) {
					log.Criticalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
					Errors++
					Mysql_free_result(result)
					return
				}
				G_string_set_size(statement, 0)
				query = fmt.Sprintf("SHOW CREATE %s %s%s%s.%s%s%s", routine_type[r], Identifier_quote_character, database.name, Identifier_quote_character, Identifier_quote_character, row[1].AsString(), Identifier_quote_character)
				mr = M_store_result_single_row(conn, query, "Failed to execute SHOW CREATE %s %s.%s %s", routine_type[r], database.name, row[1].AsString(), query)
				if mr.Row != nil {
					G_string_printf(statement, string(mr.Row[2].AsString()))
					if SkipDefiner && strings.HasPrefix(statement.Str.String(), "CREATE") {
						Remove_definer(statement)
					}
					splited_st = strings.Split(statement.Str.String(), ";\n")
					G_string_printf(statement, "%s", strings.Join(splited_st, "; \n"))
					G_string_append(statement, ";\n")
					restore_charset(statement)
					if !write_data(outfile, statement) {
						log.Criticalf("Could not write function data for %s.%s", database.name, row[1].AsString())
						Errors++
						return
					}
				}
				M_store_result_row_free(mr)
				G_string_set_size(statement, 0)
			}
		}

		if checksum_filename {
			database.post_checksum = write_checksum_into_file(conn, database, "", Checksum_process_structure)
		}
	}

	if DumpEvents {
		query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", Identifier_quote_character, database.name, Identifier_quote_character)
		result = M_store_result_critical(conn, query, "Error dumping events from %s", database.name)
		if result == nil {
			return
		}
		determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)
		for row := Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
			set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
			G_string_append_printf(statement, "DROP EVENT IF EXISTS %s%s%s;\n", Identifier_quote_character, row[1].AsString(), Identifier_quote_character)
			if !write_data(outfile, statement) {
				log.Criticalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
				Errors++
				Mysql_free_result(result)
				return
			}
			query = fmt.Sprintf("SHOW CREATE EVENT %s%s%s.%s%s%s", Identifier_quote_character, database.name, Identifier_quote_character, Identifier_quote_character, row[1].AsString(), Identifier_quote_character)
			var mr = M_store_result_row(conn, query, M_critical, M_warning, "Failed to execute SHOW CREATE EVENT %s.%s", database.name, row[1].AsString())
			if mr.Row != nil {
				G_string_printf(statement, "%s", mr.Row[3].AsString())
				if SkipDefiner && strings.HasPrefix(statement.Str.String(), "CREATE") {
					Remove_definer(statement)
				}
				splited_st = strings.Split(statement.Str.String(), ";\n")
				G_string_printf(statement, "%s", strings.Join(splited_st, "; \n"))
				G_string_append(statement, ";\n")
				restore_charset(statement)
				if !write_data(outfile, statement) {
					log.Criticalf("Could not write event data for %s.%s", database.name, row[1].AsString())
					Errors++
					M_store_result_row_free(mr)
					goto clean
				}
			}
			M_store_result_row_free(mr)
			G_string_set_size(statement, 0)
		}
	}
clean:
	err = m_close(0, outfile, filename, 1, nil)
	G_string_free(statement, true)
	return
}

// free_schema_job releases the schema job (no-op in Go; job structs are GC'd).
func free_schema_job(sj *schema_job) {
	sj = nil
}

// free_view_job releases the view job (no-op in Go).
func free_view_job(vj *view_job) {
	vj.tmp_table_filename = ""
	vj.view_filename = ""
}

// free_create_tablespace_job releases the tablespace job (no-op in Go).
func free_create_tablespace_job(ctj *create_tablespace_job) {
	ctj.filename = ""
}

// free_database_job releases the database job (no-op in Go).
func free_database_job(dj *database_job) {
	dj.filename = ""
	dj = nil
}

// free_table_checksum_job releases the table checksum job (no-op in Go).
func free_table_checksum_job(tcj *table_checksum_job) {
	tcj.filename = ""
	tcj = nil
}

// do_JOB_CREATE_DATABASE runs write_schema_definition_into_file for the job's database.
func do_JOB_CREATE_DATABASE(td *thread_data, job *job) {
	var dj = job.job_data.(*database_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping schema create for %s%s%s", td.thread_id, Identifier_quote_character, dj.database.filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping schema create for %s%s%s", td.thread_id, Identifier_quote_character, dj.database.name, Identifier_quote_character)

	}
	write_schema_definition_into_file(td.thrconn, dj.database, dj.filename)
	free_database_job(dj)
	job = nil
}

// do_JOB_CREATE_TABLESPACE runs write_tablespace_definition_into_file for the job's filename.
func do_JOB_CREATE_TABLESPACE(td *thread_data, job *job) {
	var ctj = job.job_data.(*create_tablespace_job)
	log.Infof("Thread %d: dumping create tablespace if any", td.thread_id)
	write_tablespace_definition_into_file(td.thrconn, ctj.filename)
	free_create_tablespace_job(ctj)
	job = nil
}

// do_JOB_SCHEMA_POST runs write_schema_definition_into_file for the database schema-post file.
func do_JOB_SCHEMA_POST(td *thread_data, job *job) {
	var sp = job.job_data.(*database_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping SP and VIEWs for %s%s%s", td.thread_id, Identifier_quote_character, sp.database.filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping SP and VIEWs for %s%s%s", td.thread_id, Identifier_quote_character, sp.database.name, Identifier_quote_character)
	}
	write_routines_definition_into_file(td.thrconn, sp.database, sp.filename, sp.checksum_filename)
	free_database_job(sp)
	job = nil
}

// do_JOB_SCHEMA_TRIGGERS runs write_triggers_definition_into_file_from_database for the job's database.
func do_JOB_SCHEMA_TRIGGERS(td *thread_data, job *job) {
	var sj = job.job_data.(*database_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping triggers for %s%s%s", td.thread_id, Identifier_quote_character, sj.database.filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping triggers for %s%s%s", td.thread_id, Identifier_quote_character, sj.database.name, Identifier_quote_character)
	}
	write_triggers_definition_into_file_from_database(td.thrconn, sj.database, sj.filename, sj.checksum_filename)
	free_database_job(sj)
	job = nil
}

// do_JOB_VIEW runs write_view_definition_into_file for the job's view.
func do_JOB_VIEW(td *thread_data, job *job) {
	var vj = job.job_data.(*view_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping view for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, vj.dbt.database.filename, Identifier_quote_character, Identifier_quote_character, vj.dbt.table_filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping view for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, vj.dbt.database.name, Identifier_quote_character, Identifier_quote_character, vj.dbt.table, Identifier_quote_character)
	}
	write_view_definition_into_file(td.thrconn, vj.dbt, vj.tmp_table_filename, vj.view_filename, vj.checksum_filename)
	job = nil
}

// do_JOB_SEQUENCE runs write_sequence_definition_into_file for the job's sequence.
func do_JOB_SEQUENCE(td *thread_data, job *job) {
	var sj = job.job_data.(*sequence_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping sequence for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.filename, Identifier_quote_character, Identifier_quote_character, sj.dbt.table_filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping sequence for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.name, Identifier_quote_character, Identifier_quote_character, sj.dbt.table, Identifier_quote_character)
	}
	write_sequence_definition_into_file(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	job = nil
}

// do_JOB_SCHEMA runs write_table_definition_into_file for the job's table schema.
func do_JOB_SCHEMA(td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping schema for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.filename, Identifier_quote_character, Identifier_quote_character, sj.dbt.table_filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping schema for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.name, Identifier_quote_character, Identifier_quote_character, sj.dbt.table, Identifier_quote_character)
	}
	write_table_definition_into_file(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename, sj.checksum_index_filename)
	free_schema_job(sj)
	job = nil
}

// do_JOB_TRIGGERS runs write_triggers_definition_into_file_from_dbt for the job's table.
func do_JOB_TRIGGERS(td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping triggers for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.filename, Identifier_quote_character, Identifier_quote_character, sj.dbt.table_filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping triggers for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, sj.dbt.database.name, Identifier_quote_character, Identifier_quote_character, sj.dbt.table, Identifier_quote_character)
	}
	write_triggers_definition_into_file_from_dbt(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	free_schema_job(sj)
	job = nil
}

// do_JOB_CHECKSUM runs write_checksum_into_file for the job's table and writes the checksum to the metadata file.
func do_JOB_CHECKSUM(td *thread_data, job *job) {
	var tcj = job.job_data.(*table_checksum_job)
	if masquerade_filename {
		log.Infof("Thread %d: dumping checksum for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, tcj.dbt.database.filename, Identifier_quote_character, Identifier_quote_character, tcj.dbt.table_filename, Identifier_quote_character)
	} else {
		log.Infof("Thread %d: dumping checksum for %s%s%s.%s%s%s", td.thread_id, Identifier_quote_character, tcj.dbt.database.name, Identifier_quote_character, Identifier_quote_character, tcj.dbt.table, Identifier_quote_character)
	}
	if UseSavepoints {
		M_query_critical(td.thrconn, fmt.Sprintf("SAVEPOINT %s", MYDUMPER), "Savepoint failed")
	}
	tcj.dbt.data_checksum = write_checksum_into_file(td.thrconn, tcj.dbt.database, tcj.dbt.table, Checksum_table)
	if UseSavepoints {
		M_query_critical(td.thrconn, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER), "Rollback to savepoint failed")
	}
	free_table_checksum_job(tcj)
	job = nil
}
