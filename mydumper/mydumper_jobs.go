package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	IgnoreGeneratedFields  bool
	OrderByPrimaryKey      bool
	Exec_per_thread        string
	ExecPerThreadExtension string
	DumpTriggers           bool
	SkipDefiner            bool
	exec_per_thread_cmd    []string
	m_open                 func(filename *string, mode string) (*file_write, error)
	m_close                func(thread_id uint, file *file_write, filename string, size float64, dbt *DB_Table) error
)

type checksum_fun func(conn *DBConnection, database, table string) string
type build_filename_fun func(dump_directory, database, table string, part uint64, sub_part uint) string

type schema_metadata_job struct {
	metadata_file        *os.File
	release_binlog_mutex *sync.Mutex
}

type schema_job struct {
	dbt                     *DB_Table
	filename                string
	checksum_filename       bool
	checksum_index_filename bool
}

type sequence_job struct {
	dbt               *DB_Table
	filename          string
	checksum_filename bool
}

type table_checksum_job struct {
	dbt      *DB_Table
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
	dbt                *DB_Table
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
	if conn.Err != nil && !(SuccessOn1146 && conn.Code == 1146) {
		errors++
		return ""
	}
	if checksum == "" {
		checksum = "0"
	}
	return checksum
}

func get_tablespace_query() string {
	if Get_product() == SERVER_TYPE_PERCONA || Get_product() == SERVER_TYPE_MYSQL || Get_product() == SERVER_TYPE_UNKNOWN {
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
	var result *mysql.Result
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping create tablespace: %v", err)
		} else {
			log.Criticalf("Error dumping create tablespace: %v", err)
			errors++
			return
		}
	}
	var q = Identifier_quote_character
	var statement = G_string_sized_new(StatementSize)
	initialize_sql_statement(statement)
	for _, row := range result.Values {
		G_string_append_printf(statement, fmt.Sprintf("CREATE TABLESPACE %s%s%s ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", q, row[0].AsString(), q, row[1].AsString(), row[2].AsString()))
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
	var q = Identifier_quote_character
	initialize_sql_statement(statement)

	query = fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS %s%s%s", q, database.name, q)
	result := conn.Execute(query)
	if err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping create database (%s): %v", database.name, err)
		} else {
			log.Criticalf("Error dumping create database (%s): %v", database.name, err)
		}
	}
	for _, row := range result.Values {
		if !strings.Contains(string(row[1].AsString()), Identifier_quote_character_str) {
			log.Criticalf("Identifier quote [%s] not found when fetching %s", Identifier_quote_character_str, database.name)
			errors++
		}
		G_string_append(statement, string(row[1].AsString()))
		G_string_append(statement, ";\n")
	}
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write create database for %s", database.name)
		errors++
	}
	err = m_close(0, outfile, filename, 1, nil)
	if SchemaChecksums {
		database.schema_checksum = write_checksum_into_file(conn, database, "", Checksum_database_defaults)
	}
	return
}

func write_table_definition_into_file(conn *DBConnection, dbt *DB_Table, filename string, checksum_filename bool, checksum_index_filename bool) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		errors++
		return
	}

	var q = Identifier_quote_character
	var statement = G_string_sized_new(StatementSize)

	initialize_sql_statement(statement)

	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
		return
	}
	query = fmt.Sprintf("SHOW CREATE TABLE %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result := conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Criticalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
			errors++
			return
		}
	}
	G_string_set_size(statement, 0)
	var create_table string
	row := result.Values[0]
	if Schema_sequence_fix {
		create_table = Filter_sequence_schemas(string(row[1].AsString()))
	} else {
		create_table = string(row[1].AsString())
	}
	G_string_append(statement, create_table)
	if Schema_sequence_fix {
		create_table = ""
	}
	G_string_append(statement, ";\n")
	if SkipIndexes || SkipConstraints {
		var alter_table_statement = G_string_sized_new(StatementSize)
		var alter_table_constraint_statement = G_string_sized_new(StatementSize)
		var create_table_statement = G_string_sized_new(StatementSize)
		Global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.table, true)
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

func write_triggers_definition_into_file(conn *DBConnection, result *mysql.Result, database *database, message string, outfile *file_write) {
	var result2 *mysql.Result
	var err error
	var query string
	var statement = G_string_sized_new(StatementSize)
	var q string = Identifier_quote_character
	var splited_st []string
	initialize_sql_statement(statement)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write triggers for %s", message)
		errors++
		return
	}

	for _, row := range result.Values {
		set_charset(statement, row[8].AsString(), row[9].AsString())
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write triggers data for %s", message)
			errors++
			return
		}
		G_string_set_size(statement, 0)
		query = fmt.Sprintf("SHOW CREATE TRIGGER %s%s%s.%s%s%s", q, database.name, q, q, row[0].AsString(), q)
		result2 = conn.Execute(query)
		if conn.Err != nil {
			log.Criticalf("show create trigger fail:%v", conn.Err)
			errors++
			return
		}
		row2 := result2.Values[0]
		var create string = string(row2[2].AsString())
		if SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = Remove_definer_from_gchar(create)
		}
		G_string_append_printf(statement, "%s", create)
		splited_st = strings.Split(statement.Str.String(), ";\n")

		G_string_printf(statement, strings.Join(splited_st, "; \n"))
		G_string_append(statement, ";\n")
		restore_charset(statement)
		if !write_data(outfile, statement) {
			log.Criticalf("Could not write triggers data for %s", message)
			errors++
			return
		}
		G_string_set_size(statement, 0)
	}
	return
}

func write_triggers_definition_into_file_from_dbt(conn *DBConnection, dbt *DB_Table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *mysql.Result
	var err error
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		errors++
		return
	}
	var q = Identifier_quote_character
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", q, dbt.database.name, q, dbt.table)
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping triggers (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Criticalf("Error dumping triggers (%s.%s): %v", dbt.database.name, dbt.table, err)
			errors++
		}
		return
	}
	message := fmt.Sprintf("%s.%s", dbt.database.name, dbt.table)
	write_triggers_definition_into_file(conn, result, dbt.database, message, outfile)
	err = m_close(0, outfile, filename, 1, dbt)
	if checksum_filename {
		dbt.triggers_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_trigger_structure)
	}
	return
}

func write_triggers_definition_into_file_from_database(conn *DBConnection, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var result *mysql.Result
	var q = Identifier_quote_character
	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
		errors++
		return
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s", q, database.name, q)
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping triggers (%s): %v", database.name, err)
		} else {
			log.Criticalf("Error dumping triggers (%s): %v", database.name, err)
			errors++
		}
	}
	write_triggers_definition_into_file(conn, result, database, database.name, outfile)
	err = m_close(0, outfile, filename, 1, nil)
	if checksum_filename {
		database.triggers_checksum = write_checksum_into_file(conn, database, "", Checksum_trigger_structure_from_database)
	}
	return
}

func write_view_definition_into_file(conn *DBConnection, dbt *DB_Table, filename string, filename2 string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var statement = G_string_sized_new(StatementSize)
	var result *mysql.Result
	var err error
	var q = Identifier_quote_character
	initialize_sql_statement(statement)
	if !conn.UseDB(dbt.database.name) {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, conn.Err)
		errors++
		return
	}
	outfile, err = m_open(&filename, "w")
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
	query = fmt.Sprintf("SHOW FIELDS FROM %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result = conn.Execute(query)
	if err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
		} else {
			log.Criticalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
			errors++
		}
		return
	}
	G_string_set_size(statement, 0)
	G_string_append_printf(statement, "CREATE TABLE IF NOT EXISTS %s%s%s(\n", q, dbt.table, q)
	for i, row := range result.Values {
		if i == 0 {
			G_string_append_printf(statement, "%s%s%s int", q, row[0].AsString(), q)
			continue
		}
		G_string_append(statement, ",\n")
		G_string_append_printf(statement, "%s%s%s int", q, row[0].AsString(), q)
	}
	G_string_append(statement, "\n) ENGINE=MEMORY ENCRYPTION='N';\n")
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write view schema for %s.%s", dbt.database.name, dbt.table)
		errors++
	}
	query = fmt.Sprintf("SHOW CREATE VIEW %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
		} else {
			log.Criticalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
			errors++
		}
		return
	}
	m_close(0, outfile, filename, 1, dbt)
	G_string_set_size(statement, 0)
	var outfile2 *file_write
	outfile2, err = m_open(&filename2, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		errors++
		return
	}
	initialize_sql_statement(statement)

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", q, dbt.table, q)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", q, dbt.table, q)
	if !write_data(outfile2, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
		return
	}
	G_string_set_size(statement, 0)
	row := result.Values[0]
	set_charset(statement, row[2].AsString(), row[3].AsString())
	create := string(row[1].AsString())
	if SkipDefiner && strings.HasPrefix(create, "CREATE") {
		create = Remove_definer_from_gchar(create)
	}
	G_string_append(statement, create)
	G_string_append(statement, ";\n")
	restore_charset(statement)
	if !write_data(outfile2, statement) {
		log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		errors++
	}
	err = m_close(0, outfile2, filename2, 1, dbt)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_view_structure)
	}
	return
}

func write_sequence_definition_into_file(conn *DBConnection, dbt *DB_Table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var statement = G_string_sized_new(StatementSize)
	initialize_sql_statement(statement)
	var result *mysql.Result
	var err error
	var q = Identifier_quote_character
	conn.UseDB(dbt.database.name)

	outfile, err = m_open(&filename, "w")
	if err != nil {
		log.Criticalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		errors++
		return
	}

	G_string_append_printf(statement, "DROP TABLE IF EXISTS %s%s%s;\n", q, dbt.table, q)
	G_string_append_printf(statement, "DROP VIEW IF EXISTS %s%s%s;\n", q, dbt.table, q)
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
		errors++
		return
	}

	query = fmt.Sprintf("SHOW CREATE SEQUENCE %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
		} else {
			log.Criticalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, conn.Err)
			errors++
		}
		return
	}
	G_string_set_size(statement, 0)
	for _, row := range result.Values {
		create := string(row[1].AsString())
		if SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = Remove_definer_from_gchar(create)
		}
		G_string_append(statement, create)
	}
	G_string_append(statement, ";\n")
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema for %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
		errors++
	}
	query = fmt.Sprintf("SELECT next_not_cached_value FROM %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result = conn.Execute(query)
	if conn.Err != nil {
		if SuccessOn1146 && conn.Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Criticalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
			errors++
		}
		return
	}
	G_string_set_size(statement, 0)
	for _, row := range result.Values {
		G_string_printf(statement, "DO SETVAL(%s%s%s, %s, 0);\n", q, dbt.table, q, row[0].AsString())
	}
	if !write_data(outfile, statement) {
		log.Criticalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		errors++
	}
	err = m_close(0, outfile, filename, 1, dbt)
	if checksum_filename {
		write_checksum_into_file(conn, dbt.database, dbt.table, Checksum_table_structure)
	}
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
	log.Infof("Thread %d: dumping schema create for `%s`", td.thread_id, dj.database.name)
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
	log.Infof("Thread %d: dumping SP and VIEWs for `%s`", td.thread_id, sp.database.name)
	write_routines_definition_into_file(td.thrconn, sp.database, sp.filename, sp.checksum_filename)
	free_database_job(sp)
	job = nil
}

func do_JOB_SCHEMA_TRIGGERS(td *thread_data, job *job) {
	var sj = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping triggers for `%s`", td.thread_id, sj.database.name)
	write_triggers_definition_into_file_from_database(td.thrconn, sj.database, sj.filename, sj.checksum_filename)
	free_database_job(sj)
	job = nil
}

func do_JOB_VIEW(td *thread_data, job *job) {
	var vj = job.job_data.(*view_job)
	log.Infof("Thread %d: dumping view for `%s`.`%s`", td.thread_id, vj.dbt.database.name, vj.dbt.table)
	write_view_definition_into_file(td.thrconn, vj.dbt, vj.tmp_table_filename, vj.view_filename, vj.checksum_filename)
	job = nil
}

func do_JOB_SEQUENCE(td *thread_data, job *job) {
	var sj = job.job_data.(*sequence_job)
	log.Infof("Thread %d dumping sequence for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_sequence_definition_into_file(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	job = nil
}

func do_JOB_SCHEMA(td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping schema for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_table_definition_into_file(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename, sj.checksum_index_filename)
	free_schema_job(sj)
	job = nil
}

func do_JOB_TRIGGERS(td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping triggers for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_triggers_definition_into_file_from_dbt(td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	free_schema_job(sj)
	job = nil
}

func do_JOB_CHECKSUM(td *thread_data, job *job) {
	var tcj = job.job_data.(*table_checksum_job)
	log.Infof("Thread %d: dumping checksum for `%s`.`%s`", td.thread_id, tcj.dbt.database.name, tcj.dbt.table)
	if UseSavepoints {
		_ = td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
		if td.thrconn.Err != nil {
			log.Criticalf("Savepoint failed: %v", td.thrconn.Err)
		}
	}
	tcj.dbt.data_checksum = write_checksum_into_file(td.thrconn, tcj.dbt.database, tcj.dbt.table, Checksum_table)
	if UseSavepoints {
		_ = td.thrconn.Execute(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER))
		if td.thrconn.Err != nil {
			log.Criticalf("Rollback to savepoint failed: %v", td.thrconn.Err)
		}
	}
	free_table_checksum_job(tcj)
	job = nil
}

func create_job_to_dump_table(conf *configuration, is_view bool, is_sequence bool, database *database, table string, collation string, engine string) {
	var j *job = new(job)
	var dtj *dump_table_job = new(dump_table_job)
	dtj.is_view = is_view
	dtj.is_sequence = is_sequence
	dtj.database = database
	dtj.table = table
	dtj.collation = collation
	dtj.engine = engine
	j.job_data = dtj
	j.types = JOB_TABLE
	G_async_queue_push(conf.initial_queue, j)
}

func create_job_to_dump_metadata(conf *configuration, mdfile *os.File) {
	var j *job = new(job)
	j.job_data = mdfile
	j.types = JOB_WRITE_MASTER_STATUS
	G_async_queue_push(conf.initial_queue, j)
}

func create_job_to_dump_tablespaces(conf *configuration, dump_directory string) {
	var j *job = new(job)
	var ctj *create_tablespace_job = new(create_tablespace_job)
	ctj.filename = build_tablespace_filename(dump_directory)
	j.types = JOB_CREATE_TABLESPACE
	j.job_data = ctj
	G_async_queue_push(conf.schema_queue, j)
}

func create_database_related_job(database *database, conf *configuration, types job_type, suffix string, dump_directory string) {
	var j = new(job)
	var dj = new(database_job)
	dj.database = database
	dj.filename = build_schema_filename(dump_directory, database.filename, suffix)
	dj.checksum_filename = SchemaChecksums
	j.types = types
	j.job_data = dj
	G_async_queue_push(conf.schema_queue, j)
	return
}

func create_job_to_dump_schema(database *database, conf *configuration) {
	create_database_related_job(database, conf, JOB_CREATE_DATABASE, "schema-create", dump_directory)
}

func create_job_to_dump_post(database *database, conf *configuration) {
	create_database_related_job(database, conf, JOB_SCHEMA_POST, "schema-post", dump_directory)
}

func create_job_to_dump_triggers(conn *DBConnection, dbt *DB_Table, conf *configuration) {
	var query string
	var result *mysql.Result
	var q = Identifier_quote_character
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", q, dbt.database.name, q, dbt.escaped_table)
	result = conn.Execute(query)
	if conn.Err != nil {
		log.Criticalf("Error Checking triggers for %s.%s. Err: %v St: %s", dbt.database.name, dbt.table, conn.Err, query)
		errors++
	}
	for _, row := range result.Values {
		_ = row
		var t = new(job)
		var st = new(schema_job)
		t.types = JOB_TRIGGERS
		st.dbt = dbt
		st.filename = build_schema_table_filename(dump_directory, dbt.database.filename, dbt.table_filename, "schema-triggers")
		st.checksum_filename = RoutineChecksums
		t.job_data = st
		G_async_queue_push(conf.post_data_queue, t)
	}
}

func create_job_to_dump_schema_triggers(database *database, conf *configuration) {
	var t = new(job)
	var st = new(database_job)
	st.database = database
	st.filename = build_schema_filename(dump_directory, database.filename, "schema-triggers")
	st.checksum_filename = RoutineChecksums
	t.job_data = st
	t.types = JOB_SCHEMA_TRIGGERS
	G_async_queue_push(conf.post_data_queue, t)
}

func create_job_to_dump_table_schema(dbt *DB_Table, conf *configuration) {
	var j = new(job)
	var sj = new(schema_job)
	j.types = JOB_SCHEMA
	sj.dbt = dbt
	sj.filename = build_schema_table_filename(dump_directory, dbt.database.filename, dbt.table_filename, "schema")
	sj.checksum_filename = SchemaChecksums
	sj.checksum_index_filename = SchemaChecksums
	j.job_data = sj
	G_async_queue_push(conf.schema_queue, j)
}

func create_job_to_dump_view(dbt *DB_Table, conf *configuration) {
	var j = new(job)
	var vj = new(view_job)
	vj.dbt = dbt
	j.types = JOB_VIEW
	vj.tmp_table_filename = build_schema_table_filename(dump_directory, dbt.database.filename, dbt.table_filename, "schema")
	vj.view_filename = build_schema_table_filename(dump_directory, dbt.database.filename, dbt.table_filename, "schema-view")
	vj.checksum_filename = SchemaChecksums
	j.job_data = vj
	G_async_queue_push(conf.post_data_queue, j)
	return
}

func create_job_to_dump_sequence(dbt *DB_Table, conf *configuration) {
	var j = new(job)
	var sj = new(sequence_job)
	sj.dbt = dbt
	sj.filename = build_schema_table_filename(dump_directory, dbt.database.filename, dbt.table_filename, "schema-sequence")
	sj.checksum_filename = SchemaChecksums
	j.types = JOB_SEQUENCE
	j.job_data = sj
	G_async_queue_push(conf.post_data_queue, j)
	return
}

func create_job_to_dump_checksum(dbt *DB_Table, conf *configuration) {
	var j = new(job)
	var tcj = new(table_checksum_job)
	tcj.dbt = dbt
	tcj.filename = build_meta_filename(dump_directory, dbt.database.filename, dbt.table_filename, "checksum")
	j.types = JOB_CHECKSUM
	j.job_data = tcj
	G_async_queue_push(conf.post_data_queue, j)
	return
}

func update_files_on_table_job(tj *table_job) bool {
	var csi *chunk_step_item = tj.chunk_step_item
	var err error
	if tj.rows.file.status == 0 {
		if csi.chunk_type == INTEGER {
			var s *integer_step = csi.chunk_step.integer_step
			if s.is_step_fixed_length {
				if s.is_unsigned {
					tj.sub_part = uint(s.types.unsign.min/s.step + 1)
				} else {
					tj.sub_part = uint(s.types.sign.min/int64(s.step) + 1)
				}
			}
		}
		tj.rows.filename = build_rows_filename(tj.dbt.database.filename, tj.dbt.table_filename, tj.nchunk, tj.sub_part)
		tj.rows.file, err = m_open(&tj.rows.filename, "w")
		if err != nil {
			log.Criticalf("open file %s fail: %v", tj.rows.filename, err)
			errors++
			return false
		}
		tj.rows.file.status = 1
		if tj.sql != nil {
			tj.rows.filename = build_sql_filename(tj.dbt.database.filename, tj.dbt.table_filename, tj.nchunk, tj.sub_part)
			tj.rows.file, err = m_open(&tj.sql.filename, "w")
			if err != nil {
				log.Criticalf("open file %s fail: %v", tj.sql.filename, err)
				errors++
				return false
			}
			tj.rows.file.status = 1
			return true
		}
	}
	return false
}

func new_table_job(dbt *DB_Table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item) *table_job {
	var tj = new(table_job)
	tj.partition = partition
	tj.chunk_step_item = chunk_step_item
	tj.where = ""
	tj.order_by = order_by
	tj.nchunk = nchunk
	tj.sub_part = 0
	tj.rows = new(table_job_file)
	tj.rows.file = new(file_write)
	tj.rows.filename = ""
	if output_format == SQL_INSERT {
		tj.sql = nil
	} else {
		tj.sql = new(table_job_file)
		tj.sql.file = new(file_write)
		tj.sql.filename = ""
	}
	tj.exec_out_filename = ""
	tj.dbt = dbt
	tj.st_in_file = 0
	tj.filesize = 0
	tj.char_chunk_part = CharChunk
	tj.child_process = 0
	tj.where = ""
	update_estimated_remaining_chunks_on_dbt(tj.dbt)
	return tj
}

func free_table_job(tj *table_job) {
	if tj.sql != nil {
		m_close(tj.td.thread_id, tj.sql.file, tj.sql.filename, tj.filesize, tj.dbt)
		tj.sql.file = nil
		tj.sql = nil
	}
	if tj.rows != nil {
		m_close(tj.td.thread_id, tj.rows.file, tj.rows.filename, tj.filesize, tj.dbt)
		tj.rows.file = nil
		tj.rows = nil
	}
	if tj.where != "" {
		tj.where = ""
	}
	if tj.order_by != "" {
		tj.order_by = ""
	}
	tj = nil
}

func create_job_to_dump_chunk_without_enqueuing(dbt *DB_Table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item) *job {
	var j = new(job)
	var tj = new_table_job(dbt, partition, nchunk, order_by, chunk_step_item)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	j.job_data = tj
	return j
}

func create_job_to_dump_chunk(dbt *DB_Table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item, f func(q *GAsyncQueue, task any), queue *GAsyncQueue) {
	var j = new(job)
	var tj = new_table_job(dbt, partition, nchunk, order_by, chunk_step_item)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	f(queue, j)
}

func create_job_defer(dbt *DB_Table, queue *GAsyncQueue) {
	var j *job = new(job)
	j.types = JOB_DEFER
	j.job_data = dbt
	G_async_queue_push(queue, j)
}

func create_job_to_determine_chunk_type(dbt *DB_Table, f func(q *GAsyncQueue, task any), queue *GAsyncQueue) {
	var j = new(job)
	j.job_data = dbt
	j.types = JOB_DETERMINE_CHUNK_TYPE
	f(queue, j)
	return
}

func create_job_to_dump_all_databases(conf *configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j = new(job)
	j.types = JOB_DUMP_ALL_DATABASES
	j.job_data = nil
	G_async_queue_push(conf.initial_queue, j)
	return
}

func create_job_to_dump_table_list(table_list []string, conf *configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j = new(job)
	var dtlj = new(dump_table_list_job)
	dtlj.table_list = table_list
	j.types = JOB_DUMP_TABLE_LIST
	j.job_data = dtlj
	G_async_queue_push(conf.initial_queue, j)
	return
}

func create_job_to_dump_database(database *database, conf *configuration) {
	atomic.AddInt64(&database_counter, 1)
	var j = new(job)
	var ddj = new(dump_database_job)
	ddj.database = database
	j.types = JOB_DUMP_DATABASE
	j.job_data = ddj
	G_async_queue_push(conf.initial_queue, j)
	return
}
