package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

type checksum_fun func(conn *client.Conn, database, table string) (string, error)
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

func initialize_jobs(o *OptionEntries) {
	initialize_database(o)
	if o.Extra.IgnoreGeneratedFields {
		log.Warnf("Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	}
	/*if o.Exec.Exec_per_thread_extension != "" {
		if o.Exec.Exec_per_thread == "" {
			log.Fatalf("--exec-per-thread needs to be set when --exec-per-thread-extension (%s) is used", o.Exec.Exec_per_thread_extension)
		}
	}*/
	/*if o.Exec.Exec_per_thread != "" {
		if o.Exec.Exec_per_thread[0] != '/' {
			log.Fatalf("Absolute path is only allowed when --exec-per-thread is used")
		}
		o.global.exec_per_thread_cmd = strings.Split(o.Exec.Exec_per_thread, " ")
	}*/
}

func write_checksum_into_file(o *OptionEntries, conn *client.Conn, database *database, table string, f checksum_fun) string {
	checksum, err := f(conn, database.name, table)
	if err != nil && !(o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146) {
		return ""
	}
	if checksum == "" {
		checksum = "0"
	}
	return checksum
}

func (o *OptionEntries) get_tablespace_query() string {
	if o.get_product() == SERVER_TYPE_PERCONA || o.get_product() == SERVER_TYPE_MYSQL || o.get_product() == SERVER_TYPE_UNKNOWN {
		if o.get_major() == 5 && o.get_secondary() == 7 {
			return "select NAME, PATH, FS_BLOCK_SIZE from information_schema.INNODB_SYS_TABLESPACES join information_schema.INNODB_SYS_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';"
		}
		if o.get_major() == 8 {
			return "select NAME,PATH,FS_BLOCK_SIZE,ENCRYPTION from information_schema.INNODB_TABLESPACES join information_schema.INNODB_DATAFILES using (space) where SPACE_TYPE='General' and NAME != 'mysql';"
		}
	}
	return ""
}

func write_tablespace_definition_into_file(o *OptionEntries, conn *client.Conn, filename string) {
	var query string
	outfile, err := o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: Could not create output file %s (%v)", filename, err)
		return
	}
	query = o.get_tablespace_query()
	if query == "" {
		log.Warnf("Tablespace resquested, but not possible due to server version not supported")
		return
	}
	var result *mysql.Result
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping create tablespace: %v", err)
		} else {
			log.Fatalf("Error dumping create tablespace: %v", err)
			return
		}
	}
	var q = o.Common.IdentifierQuoteCharacter
	var statement *strings.Builder = new(strings.Builder)
	initialize_sql_statement(o, statement)
	for _, row := range result.Values {
		statement.WriteString(fmt.Sprintf("CREATE TABLESPACE %s%s%s ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", q, row[0].AsString(), q, row[1].AsString(), row[2].AsString()))
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write tablespace data for %s", row[0].AsString())
			return
		}
		statement.Reset()
	}

}

func write_schema_definition_into_file(o *OptionEntries, conn *client.Conn, database *database, filename string) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	var statement *strings.Builder = new(strings.Builder)
	var q = o.Common.IdentifierQuoteCharacter
	initialize_sql_statement(o, statement)

	query = fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS %s%s%s", q, database.name, q)
	result, err := conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping create database (%s): %v", database.name, err)
		} else {
			log.Fatalf("Error dumping create database (%s): %v", database.name, err)
		}
	}
	for _, row := range result.Values {
		if !strings.Contains(string(row[1].AsString()), o.Common.IdentifierQuoteCharacter) {
			log.Fatalf("Identifier quote [%s] not found when fetching %s", o.Common.IdentifierQuoteCharacter, database.name)
		}
		statement.WriteString(string(row[1].AsString()))
		statement.WriteString(";\n")
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write create database for %s", database.name)
	}
	err = o.global.m_close(o, 0, outfile, filename, 1, nil)
	if o.Checksum.SchemaChecksums {
		database.schema_checksum = write_checksum_into_file(o, conn, database, "", checksum_database_defaults)
	}
	return
}

func write_table_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool, checksum_index_filename bool) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
	}

	var q = o.Common.IdentifierQuoteCharacter
	var statement *strings.Builder = new(strings.Builder)

	initialize_sql_statement(o, statement)

	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE TABLE %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result, err := conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement.Reset()
	var create_table string
	row := result.Values[0]
	if o.global.schema_sequence_fix {
		create_table = filter_sequence_schemas(string(row[1].AsString()))
	} else {
		create_table = string(row[1].AsString())
	}
	statement.WriteString(create_table)
	statement.WriteString(";\n")
	if o.Objects.SkipIndexes || o.Objects.SkipConstraints {
		var alter_table_statement *strings.Builder = new(strings.Builder)
		var alter_table_constraint_statement *strings.Builder = new(strings.Builder)
		var create_table_statement *strings.Builder = new(strings.Builder)
		global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.table, true)
		if !write_data(outfile, create_table_statement) {
			log.Errorf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		}
		if !o.Objects.SkipIndexes {
			write_data(outfile, alter_table_statement)
		}
		if !o.Objects.SkipConstraints {
			write_data(outfile, alter_table_constraint_statement)
		}
	} else {
		if !write_data(outfile, statement) {
			log.Errorf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
		}
	}

	o.global.m_close(o, 0, outfile, filename, 1, dbt)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_table_structure)
	}
	if checksum_index_filename {
		dbt.indexes_checksum = write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_table_indexes)
	}
}

func write_triggers_definition_into_file(o *OptionEntries, conn *client.Conn, result *mysql.Result, database *database, message string, outfile *file_write) {
	var result2 *mysql.Result
	var err error
	var query string
	var statement *strings.Builder = new(strings.Builder)
	var q string = o.Common.IdentifierQuoteCharacter
	var splited_st []string
	initialize_sql_statement(o, statement)
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write triggers for %s", message)
	}

	for _, row := range result.Values {
		set_charset(statement, row[8].AsString(), row[9].AsString())
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write triggers data for %s", message)
			return
		}
		statement.Reset()
		query = fmt.Sprintf("SHOW CREATE TRIGGER %s%s%s.%s%s%s", q, database.name, q, q, row[0].AsString(), q)
		result2, err = conn.Execute(query)
		if err != nil {
			log.Fatalf("show create trigger fail:%v", err)
		}
		row2 := result2.Values[0]
		var create string = string(row2[2].AsString())
		if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = remove_definer_from_gchar(create)
		}
		statement.WriteString(create)
		splited_st = strings.Split(statement.String(), ";\n")
		statement.Reset()
		statement.WriteString(strings.Join(splited_st, "; \n"))
		statement.WriteString(";\n")
		restore_charset(statement)
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write triggers data for %s", message)
			return
		}
		statement.Reset()
	}
	return
}

func write_triggers_definition_into_file_from_dbt(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *mysql.Result
	var err error
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		return
	}
	var q = o.Common.IdentifierQuoteCharacter
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", q, dbt.database.name, q, dbt.table)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping triggers (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping triggers (%s.%s): %v", dbt.database.name, dbt.table, err)
			return
		}
	}
	message := fmt.Sprintf("%s.%s", dbt.database.name, dbt.table)
	write_triggers_definition_into_file(o, conn, result, dbt.database, message, outfile)
	err = o.global.m_close(o, 0, outfile, filename, 1, dbt)
	if checksum_filename {
		dbt.triggers_checksum = write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_trigger_structure)
	}
	return
}

func write_triggers_definition_into_file_from_database(o *OptionEntries, conn *client.Conn, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var result *mysql.Result
	var q = o.Common.IdentifierQuoteCharacter
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s", q, database.name, q)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping triggers (%s): %v", database.name, err)
		} else {
			log.Fatalf("Error dumping triggers (%s): %v", database.name, err)
		}
	}
	write_triggers_definition_into_file(o, conn, result, database, database.name, outfile)
	err = o.global.m_close(o, 0, outfile, filename, 1, nil)
	if checksum_filename {
		database.triggers_checksum = write_checksum_into_file(o, conn, database, "", checksum_trigger_structure_from_database)
	}
	return
}

func write_view_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, filename2 string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var statement *strings.Builder
	var result *mysql.Result
	var q = o.Common.IdentifierQuoteCharacter
	initialize_sql_statement(o, statement)
	err = conn.UseDB(dbt.database.name)
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
	}
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
	}

	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW FIELDS FROM %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement.Reset()
	statement.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s%s%s(\n", q, dbt.table, q))

	for i, row := range result.Values {
		if i == 0 {
			statement.WriteString(fmt.Sprintf("%s%s%s int", q, row[0].AsString(), q))
			continue
		}
		statement.WriteString(",\n")
		statement.WriteString(fmt.Sprintf("%s%s%s int", q, row[0].AsString(), q))
	}
	statement.WriteString("\n) ENGINE=MEMORY ENCRYPTION='N';\n")
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write view schema for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE VIEW %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	o.global.m_close(o, 0, outfile, filename, 1, dbt)
	statement.Reset()
	var outfile2 *file_write
	outfile2, err = o.global.m_open(o, &filename2, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
	}
	initialize_sql_statement(o, statement)

	statement.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s%s%s;\n", q, dbt.table, q))
	statement.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS %s%s%s;\n", q, dbt.table, q))
	if !write_data(outfile2, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	statement.Reset()
	row := result.Values[0]
	set_charset(statement, row[2].AsString(), row[3].AsString())
	create := string(row[1].AsString())
	if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
		create = remove_definer_from_gchar(create)
	}
	statement.WriteString(create)
	statement.WriteString(";\n")
	restore_charset(statement)
	if !write_data(outfile2, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	err = o.global.m_close(o, 0, outfile2, filename2, 1, dbt)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_view_structure)
	}
	return
}

func write_sequence_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var statement *strings.Builder = new(strings.Builder)
	initialize_sql_statement(o, statement)
	var result *mysql.Result
	var q = o.Common.IdentifierQuoteCharacter
	err = conn.UseDB(dbt.database.name)

	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		return
	}

	statement.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s%s%s;\n", q, dbt.table, q))
	statement.WriteString(fmt.Sprintf("DROP VIEW IF EXISTS %s%s%s;\n", q, dbt.table, q))
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE SEQUENCE %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement.Reset()
	for _, row := range result.Values {
		create := string(row[1].AsString())
		if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = remove_definer_from_gchar(create)
		}
		statement.WriteString(create)
	}
	statement.WriteString(";\n")
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema for %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	}
	query = fmt.Sprintf("SELECT next_not_cached_value FROM %s%s%s.%s%s%s", q, dbt.database.name, q, q, dbt.table, q)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement.Reset()
	for _, row := range result.Values {
		statement.WriteString(fmt.Sprintf("DO SETVAL(%s%s%s, %s, 0);\n", q, dbt.table, q, row[0].AsString()))
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	err = o.global.m_close(o, 0, outfile, filename, 1, dbt)
	if checksum_filename {
		write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_table_structure)
	}
	return
}

func write_routines_definition_into_file(o *OptionEntries, conn *client.Conn, database *database, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *mysql.Result
	var result2 *mysql.Result
	var splited_st []string
	var err error
	outfile, err = o.global.m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	var statement *strings.Builder = new(strings.Builder)
	var q = o.Common.IdentifierQuoteCharacter
	initialize_sql_statement(o, statement)
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write %s", filename)
	}
	var charcol, collcol uint
	if o.Objects.DumpRoutines {
		if o.global.nroutines < 0 {
			log.Fatalf("nroutines need > 0")
		}
		var r uint
		for r = 0; r < o.global.nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", o.global.routine_type[r], database.escaped)
			result, err = conn.Execute(query)
			if err != nil {
				if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
					log.Warnf("Error dumping functions from %s: %v", database.escaped, err)
				} else {
					log.Fatalf("Error dumping functions from %s: %v", database.escaped, err)
				}
			}
			determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)

			for _, row := range result.Values {
				set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
				statement.WriteString(fmt.Sprintf("DROP %s IF EXISTS %s%s%s;\n", o.global.routine_type[r], q, row[1].AsString(), q))
				if !write_data(outfile, statement) {
					log.Fatalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
				}
				statement.Reset()
				query = fmt.Sprintf("SHOW CREATE %s %s%s%s.%s%s%s", o.global.routine_type[r], q, database.name, q, q, row[1].AsString(), q)
				result2, err = conn.Execute(query)
				for _, row2 := range result2.Values {
					statement.Write(row2[2].AsString())
					if o.Statement.SkipDefiner && strings.HasPrefix(statement.String(), "CREATE") {
						statement.Reset()
						statement.WriteString(remove_definer(statement.String()))
					}
					splited_st = strings.Split(statement.String(), ";\n")
					statement.Reset()
					statement.WriteString(fmt.Sprintf("%s", strings.Join(splited_st, "; \n")))
					statement.WriteString(";\n")
					restore_charset(statement)
					if !write_data(outfile, statement) {
						log.Fatalf("Could not write function data for %s.%s", database.name, row[1].AsString())
					}
				}
				statement.Reset()
			}
		}

		if checksum_filename {
			database.post_checksum = write_checksum_into_file(o, conn, database, "", checksum_process_structure)
		}
	}

	if o.Objects.DumpEvents {
		query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", q, database.name, q)
		result, err = conn.Execute(query)
		if err != nil {
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Error dumping events from %s: %v", database.name, err)
			} else {
				log.Fatalf("Error dumping events from %s: %v", database.name, err)
			}
		}
		determine_charset_and_coll_columns_from_show(result, &charcol, &collcol)
		for _, row := range result.Values {
			set_charset(statement, row[charcol].AsString(), row[collcol].AsString())
			statement.WriteString(fmt.Sprintf("DROP EVENT IF EXISTS %s%s%s;\n", q, row[1].AsString(), q))
			if !write_data(outfile, statement) {
				log.Fatalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
			}
			query = fmt.Sprintf("SHOW CREATE EVENT %s%s%s.%s%s%s", q, database.name, q, q, row[1].AsString(), q)
			result2, err = conn.Execute(query)
			statement.Reset()
			for _, row2 := range result2.Values {
				statement.WriteString(fmt.Sprintf("%s", row2[3].AsString()))
				if o.Statement.SkipDefiner && strings.HasPrefix(statement.String(), "CREATE") {
					statement.Reset()
					statement.WriteString(remove_definer(statement.String()))
				}
				statement.Reset()
				splited_st = strings.Split(statement.String(), ";\n")
				statement.WriteString(fmt.Sprintf("%s", strings.Join(splited_st, "; \n")))
				statement.WriteString(";\n")
				restore_charset(statement)
				if !write_data(outfile, statement) {
					log.Fatalf("Could not write event data for %s.%s", database.name, row[1].AsString())
				}
			}
			statement.Reset()
		}
	}
	err = o.global.m_close(o, 0, outfile, filename, 1, nil)
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

func do_JOB_CREATE_DATABASE(o *OptionEntries, td *thread_data, job *job) {
	var dj = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping schema create for `%s`", td.thread_id, dj.database.name)
	write_schema_definition_into_file(o, td.thrconn, dj.database, dj.filename)
	free_database_job(dj)
	job = nil
}

func do_JOB_CREATE_TABLESPACE(o *OptionEntries, td *thread_data, job *job) {
	var ctj = job.job_data.(*create_tablespace_job)
	log.Infof("Thread %d: dumping create tablespace if any", td.thread_id)
	write_tablespace_definition_into_file(o, td.thrconn, ctj.filename)
	free_create_tablespace_job(ctj)
	job = nil
}

func do_JOB_SCHEMA_POST(o *OptionEntries, td *thread_data, job *job) {
	var sp = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping SP and VIEWs for `%s`", td.thread_id, sp.database.name)
	write_routines_definition_into_file(o, td.thrconn, sp.database, sp.filename, sp.checksum_filename)
	free_database_job(sp)
	job = nil
}

func do_JOB_SCHEMA_TRIGGERS(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping triggers for `%s`", td.thread_id, sj.database.name)
	write_triggers_definition_into_file_from_database(o, td.thrconn, sj.database, sj.filename, sj.checksum_filename)
	free_database_job(sj)
	job = nil
}

func do_JOB_VIEW(o *OptionEntries, td *thread_data, job *job) {
	var vj = job.job_data.(*view_job)
	log.Infof("Thread %d: dumping view for `%s`.`%s`", td.thread_id, vj.dbt.database.name, vj.dbt.table)
	write_view_definition_into_file(o, td.thrconn, vj.dbt, vj.tmp_table_filename, vj.view_filename, vj.checksum_filename)
	job = nil
}

func do_JOB_SEQUENCE(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*sequence_job)
	log.Infof("Thread %d dumping sequence for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_sequence_definition_into_file(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	job = nil
}

func do_JOB_SCHEMA(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping schema for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_table_definition_into_file(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename, sj.checksum_index_filename)
	free_schema_job(sj)
	job = nil
}

func do_JOB_TRIGGERS(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping triggers for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_triggers_definition_into_file_from_dbt(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	free_schema_job(sj)
	job = nil
}

func do_JOB_CHECKSUM(o *OptionEntries, td *thread_data, job *job) {
	var tcj = job.job_data.(*table_checksum_job)
	log.Infof("Thread %d: dumping checksum for `%s`.`%s`", td.thread_id, tcj.dbt.database.name, tcj.dbt.table)
	if o.Lock.UseSavepoints {
		_, err := td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
		if err != nil {
			log.Fatalf("Savepoint failed: %v", err)
		}
	}
	tcj.dbt.data_checksum = write_checksum_into_file(o, td.thrconn, tcj.dbt.database, tcj.dbt.table, checksum_table)
	if o.Lock.UseSavepoints {
		_, err := td.thrconn.Execute(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER))
		if err != nil {
			log.Fatalf("Rollback to savepoint failed: %v", err)
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
	conf.initial_queue.push(j)
}

func create_job_to_dump_metadata(conf *configuration, mdfile *os.File) {
	var j *job = new(job)
	j.job_data = mdfile
	j.types = JOB_WRITE_MASTER_STATUS
	conf.initial_queue.push(j)
}

func create_job_to_dump_tablespaces(conf *configuration, dump_directory string) {
	var j *job = new(job)
	var ctj *create_tablespace_job = new(create_tablespace_job)
	ctj.filename = build_tablespace_filename(dump_directory)
	j.types = JOB_CREATE_TABLESPACE
	j.job_data = ctj
	conf.schema_queue.push(j)
}

func create_database_related_job(o *OptionEntries, database *database, conf *configuration, types job_type, suffix string, dump_directory string) {
	var j = new(job)
	var dj = new(database_job)
	dj.database = database
	dj.filename = build_schema_filename(dump_directory, database.filename, suffix)
	dj.checksum_filename = o.Checksum.SchemaChecksums
	j.types = types
	j.job_data = dj
	conf.schema_queue.push(j)
	return
}

func create_job_to_dump_schema(o *OptionEntries, database *database, conf *configuration) {
	create_database_related_job(o, database, conf, JOB_CREATE_DATABASE, "schema-create", o.global.dump_directory)
}

func create_job_to_dump_post(o *OptionEntries, database *database, conf *configuration) {
	create_database_related_job(o, database, conf, JOB_SCHEMA_POST, "schema-post", o.global.dump_directory)
}

func create_job_to_dump_triggers(o *OptionEntries, conn *client.Conn, dbt *db_table, conf *configuration) {
	var query string
	var result *mysql.Result
	var err error
	var q = o.Common.IdentifierQuoteCharacter
	query = fmt.Sprintf("SHOW TRIGGERS FROM %s%s%s LIKE '%s'", q, dbt.database.name, q, dbt.escaped_table)
	result, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("Error Checking triggers for %s.%s. Err: %v St: %s", dbt.database.name, dbt.table, err, query)
	}
	for _, row := range result.Values {
		_ = row
		var t = new(job)
		var st = new(schema_job)
		t.types = JOB_TRIGGERS
		st.dbt = dbt
		st.filename = build_schema_table_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "schema-triggers")
		st.checksum_filename = o.Checksum.RoutineChecksums
		t.job_data = st
		conf.post_data_queue.push(t)
	}
}

func create_job_to_dump_schema_triggers(o *OptionEntries, database *database, conf *configuration) {
	var t = new(job)
	var st = new(database_job)
	st.database = database
	st.filename = build_schema_filename(o.global.dump_directory, database.filename, "schema-triggers")
	st.checksum_filename = o.Checksum.RoutineChecksums
	t.job_data = st
	t.types = JOB_SCHEMA_TRIGGERS
	conf.post_data_queue.push(t)
}

func create_job_to_dump_table_schema(o *OptionEntries, dbt *db_table, conf *configuration) {
	var j = new(job)
	var sj = new(schema_job)
	j.types = JOB_SCHEMA
	sj.dbt = dbt
	sj.filename = build_schema_table_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "schema")
	sj.checksum_filename = o.Checksum.SchemaChecksums
	sj.checksum_index_filename = o.Checksum.SchemaChecksums
	j.job_data = sj
	conf.schema_queue.push(j)
}

func create_job_to_dump_view(o *OptionEntries, dbt *db_table, conf *configuration) {
	var j = new(job)
	var vj = new(view_job)
	vj.dbt = dbt
	j.types = JOB_VIEW
	vj.tmp_table_filename = build_schema_table_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "schema")
	vj.view_filename = build_schema_table_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "schema-view")
	vj.checksum_filename = o.Checksum.SchemaChecksums
	j.job_data = vj
	conf.post_data_queue.push(j)
	return
}

func create_job_to_dump_sequence(o *OptionEntries, dbt *db_table, conf *configuration) {
	var j = new(job)
	var sj = new(sequence_job)
	sj.dbt = dbt
	sj.filename = build_schema_table_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "schema-sequence")
	sj.checksum_filename = o.Checksum.SchemaChecksums
	j.types = JOB_SEQUENCE
	j.job_data = sj
	conf.post_data_queue.push(j)
	return
}

func create_job_to_dump_checksum(o *OptionEntries, dbt *db_table, conf *configuration) {
	var j = new(job)
	var tcj = new(table_checksum_job)
	tcj.dbt = dbt
	tcj.filename = build_meta_filename(o.global.dump_directory, dbt.database.filename, dbt.table_filename, "checksum")
	j.types = JOB_CHECKSUM
	j.job_data = tcj
	conf.post_data_queue.push(j)
	return
}

func update_files_on_table_job(o *OptionEntries, tj *table_job) bool {
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
		tj.rows.filename = build_rows_filename(o, tj.dbt.database.filename, tj.dbt.table_filename, tj.nchunk, tj.sub_part)
		tj.rows.file, err = o.global.m_open(o, &tj.rows.filename, "w")
		if err != nil {
			log.Fatalf("open file %s fail: %v", tj.rows.filename, err)
		}
		tj.rows.file.status = 1
		if tj.sql != nil {
			tj.rows.filename = build_sql_filename(o, tj.dbt.database.filename, tj.dbt.table_filename, tj.nchunk, tj.sub_part)
			tj.rows.file, err = o.global.m_open(o, &tj.sql.filename, "w")
			if err != nil {
				log.Fatalf("open file %s fail: %v", tj.sql.filename, err)
			}
			tj.rows.file.status = 1
			return true
		}
	}
	return false
}

func new_table_job(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item) *table_job {
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
	if o.global.output_format == SQL_INSERT {
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
	tj.char_chunk_part = o.Chunks.CharChunk
	tj.child_process = 0
	tj.where = ""
	update_estimated_remaining_chunks_on_dbt(tj.dbt)
	return tj
}

func free_table_job(o *OptionEntries, tj *table_job) {
	if tj.sql != nil {
		o.global.m_close(o, tj.td.thread_id, tj.sql.file, tj.sql.filename, tj.filesize, tj.dbt)
		tj.sql.file = nil
		tj.sql = nil
	}
	if tj.rows != nil {
		o.global.m_close(o, tj.td.thread_id, tj.rows.file, tj.rows.filename, tj.filesize, tj.dbt)
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

func create_job_to_dump_chunk_without_enqueuing(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item) *job {
	var j = new(job)
	var tj = new_table_job(o, dbt, partition, nchunk, order_by, chunk_step_item)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	j.job_data = tj
	return j
}

func create_job_to_dump_chunk(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step_item *chunk_step_item, f func(q *asyncQueue, task any), queue *asyncQueue) {
	var j = new(job)
	var tj = new_table_job(o, dbt, partition, nchunk, order_by, chunk_step_item)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	f(queue, j)
}

func create_job_defer(dbt *db_table, queue *asyncQueue) {
	var j *job = new(job)
	j.types = JOB_DEFER
	j.job_data = dbt
	queue.push(j)
}

func create_job_to_determine_chunk_type(dbt *db_table, f func(q *asyncQueue, task any), queue *asyncQueue) {
	var j = new(job)
	j.job_data = dbt
	j.types = JOB_DETERMINE_CHUNK_TYPE
	f(queue, j)
	return
}

func create_job_to_dump_all_databases(o *OptionEntries, conf *configuration) {
	atomic.AddInt64(&o.global.database_counter, 1)
	var j = new(job)
	j.types = JOB_DUMP_ALL_DATABASES
	j.job_data = nil
	conf.initial_queue.push(j)
	return
}

func create_job_to_dump_table_list(o *OptionEntries, table_list []string, conf *configuration) {
	atomic.AddInt64(&o.global.database_counter, 1)
	var j = new(job)
	var dtlj = new(dump_table_list_job)
	dtlj.table_list = table_list
	j.types = JOB_DUMP_TABLE_LIST
	j.job_data = dtlj
	conf.initial_queue.push(j)
	return
}

func create_job_to_dump_database(o *OptionEntries, database *database, conf *configuration) {
	atomic.AddInt64(&o.global.database_counter, 1)
	var j = new(job)
	var ddj = new(dump_database_job)
	ddj.database = database
	j.types = JOB_DUMP_DATABASE
	j.job_data = ddj
	conf.initial_queue.push(j)
	return
}
