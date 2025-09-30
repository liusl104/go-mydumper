package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
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

func initialize_jobs() {
	initialize_database()
	if IgnoreGeneratedFields {
		log.Warnf("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	}
}

func write_checksum_into_file(conn *DBConnection, database *database, table string, fun checksum_fun) string {
	checksum := fun(conn, database.name, table)
	if checksum == "" {
		checksum = "0"
	}
	return checksum
}

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

func write_tablespace_definition_into_file(conn *DBConnection, filename string) {
	var query string
	var outfile *file_write
	var err error
	var row []mysql.FieldValue
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: Could not create output file %s (%v)", filename, err)
		errors++
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
			errors++
			return
		}
		G_string_set_size(statement, 0)
	}
}

func write_schema_definition_into_file(conn *DBConnection, database *database, filename string) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		errors++
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
		errors++
	}
	G_string_append(statement, string(mr.Row[1].AsString()))
	G_string_append(statement, ";\n")

	if !write_data(outfile, statement) {
		log.Criticalf("Could not write create database for %s", database.name)
		errors++
	}
	err = m_close(0, outfile, filename, 1, nil)
	M_store_result_row_free(mr)
	if SchemaChecksums {
		database.schema_checksum = write_checksum_into_file(conn, database, "", Checksum_database_defaults)
	}
	return
}

func write_table_definition_into_file(conn *DBConnection, dbt *db_table, filename string, checksum_filename bool, checksum_index_filename bool) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		errors++
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
		errors++
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
			errors++
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
			errors++
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

func write_triggers_definition_into_file(conn *DBConnection, result *MYSQL_RES, database *database, message string, outfile *file_write) {
	var row []mysql.FieldValue
	var query string
	var statement = G_string_sized_new(StatementSize)
	var create_trigger = G_string_sized_new(StatementSize)
	var splited_st []string
	initialize_sql_statement(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write triggers for %s", message)
		errors++
		return
	}
	for row = Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		set_charset(statement, row[8].AsString(), row[9].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write triggers data for %s", message)
			errors++
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
				errors++
				return
			}
		}
		M_store_result_row_free(mr)
		G_string_set_size(statement, 0)
	}
	return
}

func write_triggers_definition_into_file_from_dbt(conn *DBConnection, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *MYSQL_RES
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		errors++
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

func write_triggers_definition_into_file_from_database(conn *DBConnection, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var result *MYSQL_RES
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		errors++
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

func write_view_definition_into_file(conn *DBConnection, dbt *db_table, tmp_table_filename string, view_filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var statement = G_string_sized_new(StatementSize)
	var result *MYSQL_RES
	var row []mysql.FieldValue
	var err error
	initialize_sql_statement(statement)
	if !conn.UseDB(dbt.database.name) {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, conn.Err)
		errors++
		return
	}
	outfile, err = m_open(&tmp_table_filename, "w")
	if outfile == nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, nil)
		errors++
		return
	}

	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
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
		errors++
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
		errors++
		return
	}
	initialize_sql_statement(statement)

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
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
		errors++
	}
	err = m_close(0, outfile, view_filename, 1, dbt)
	M_store_result_row_free(mr)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_view_structure)
	}
	return
}

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
		errors++
		return
	}

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", Identifier_quote_character, dbt.table, Identifier_quote_character)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
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
		errors++
	}
	M_store_result_row_free(mr)
	query = fmt.Sprintf("SELECT next_not_cached_value FROM %s%s%s.%s%s%s", Identifier_quote_character, dbt.database.name, Identifier_quote_character, Identifier_quote_character, dbt.table, Identifier_quote_character)
	mr = M_store_result_row(conn, query, M_critical, M_warning, "Error dumping schemas (%s.%s)", dbt.database.name, dbt.table)
	G_string_set_size(statement, 0)
	if mr.Row != nil {
		G_string_printf(statement, "DO SETVAL(%s%s%s, %s, 0);\n", Identifier_quote_character, dbt.table, Identifier_quote_character, mr.Row[0].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
			errors++
		}
		err = m_close(0, outfile, filename, 1, dbt)
		if checksum_filename {
			write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_table_structure)
		}
	}
	M_store_result_row_free(mr)
	return
}

func write_routines_definition_into_file(conn *DBConnection, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *mysql.Result
	var result2 *mysql.Result
	var splited_st []string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		errors++
		return
	}
	var statement = G_string_sized_new(StatementSize)
	var q = Identifier_quote_character
	initialize_sql_statement(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write %s", filename)
		errors++
		return
	}
	var charcol, collcol uint
	if DumpRoutines {
		G_assert(nroutines > 0)
		var r uint
		for r = 0; r < nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", routine_type[r], database.escaped)
			result = conn.Execute(query)
			if conn.Err != nil {
				if SuccessOn1146 && conn.Code == 1146 {
					log.Warnf("Error dumping functions from %s: %v", database.escaped, conn.Err)
				} else {
					log.Criticalf("Error dumping functions from %s: %v", database.escaped, conn.Err)
					errors++
				}
				return
			}
			determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)

			for _, row := range result.Values {
				set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
				G_string_append_printf(statement, "DROP %s IF EXISTS %s%s%s;\n", routine_type[r], q, row[1].AsString(), q)
				if !write_data(outfile, statement) {
					log.Criticalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
					errors++
					return
				}
				G_string_set_size(statement, 0)
				query = fmt.Sprintf("SHOW CREATE %s %s%s%s.%s%s%s", routine_type[r], q, database.name, q, q, row[1].AsString(), q)
				result2 = conn.Execute(query)
				for _, row2 := range result2.Values {
					G_string_printf(statement, string(row2[2].AsString()))
					if SkipDefiner && strings.HasPrefix(statement.Str.String(), "CREATE") {
						Remove_definer(statement)
					}
					splited_st = strings.Split(statement.Str.String(), ";\n")
					G_string_printf(statement, "%s", strings.Join(splited_st, "; \n"))
					G_string_append(statement, ";\n")
					restore_charset(statement)
					if !write_data(outfile, statement) {
						log.Criticalf("Could not write function data for %s.%s", database.name, row[1].AsString())
						errors++
						return
					}
				}
				G_string_set_size(statement, 0)
			}
		}

		if checksum_filename {
			database.post_checksum = write_checksum_into_file(conn, database, "", Checksum_process_structure)
		}
	}

	if DumpEvents {
		query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", q, database.name, q)
		result = conn.Execute(query)
		if conn.Err != nil {
			if SuccessOn1146 && conn.Code == 1146 {
				log.Warnf("Error dumping events from %s: %v", database.name, conn.Err)
			} else {
				log.Criticalf("Error dumping events from %s: %v", database.name, conn.Err)
				errors++
			}
			return
		}
		determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)
		for _, row := range result.Values {
			set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
			G_string_append_printf(statement, "DROP EVENT IF EXISTS %s%s%s;\n", q, row[1].AsString(), q)
			if !write_data(outfile, statement) {
				log.Criticalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
				errors++
				return
			}
			query = fmt.Sprintf("SHOW CREATE EVENT %s%s%s.%s%s%s", q, database.name, q, q, row[1].AsString(), q)
			result2 = conn.Execute(query)
			for _, row2 := range result2.Values {
				G_string_printf(statement, "%s", row2[3].AsString())
				if SkipDefiner && strings.HasPrefix(statement.Str.String(), "CREATE") {
					Remove_definer(statement)
				}
				splited_st = strings.Split(statement.Str.String(), ";\n")
				G_string_printf(statement, "%s", strings.Join(splited_st, "; \n"))
				G_string_append(statement, ";\n")
				restore_charset(statement)
				if !write_data(outfile, statement) {
					log.Criticalf("Could not write event data for %s.%s", database.name, row[1].AsString())
					errors++
					return
				}
			}
			G_string_set_size(statement, 0)
		}
	}
	err = m_close(0, outfile, filename, 1, nil)
	return
}

func free_schema_job(sj *schema_job) {
	sj = nil
}

func free_view_job(vj *view_job) {
	vj.tmp_table_filename = ""
	vj.view_filename = ""
}

func free_create_tablespace_job(ctj *create_tablespace_job) {
	ctj.filename = ""
}

func free_database_job(dj *database_job) {
	dj.filename = ""
	dj = nil
}

func free_table_checksum_job(tcj *table_checksum_job) {
	tcj.filename = ""
	tcj = nil
}

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

func do_JOB_CREATE_TABLESPACE(td *thread_data, job *job) {
	var ctj = job.job_data.(*create_tablespace_job)
	log.Infof("Thread %d: dumping create tablespace if any", td.thread_id)
	write_tablespace_definition_into_file(td.thrconn, ctj.filename)
	free_create_tablespace_job(ctj)
	job = nil
}

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
