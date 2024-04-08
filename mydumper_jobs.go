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
	outfile, err := m_open(o, &filename, "w")
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
	var statement string
	for _, row := range result.Values {
		statement = fmt.Sprintf("CREATE TABLESPACE `%s` ADD DATAFILE '%s' FILE_BLOCK_SIZE = %s ENGINE=INNODB;\n", row[0].AsString(), row[1].AsString(), row[2].AsString())
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write tablespace data for %s", row[0].AsString())
			return
		}
	}

}

func write_schema_definition_into_file(o *OptionEntries, conn *client.Conn, database *database, filename string) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	var statement string
	query = fmt.Sprintf("SHOW CREATE DATABASE IF NOT EXISTS `%s`", database.name)
	result, err := conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping create database (%s): %v", database.name, err)
		} else {
			log.Fatalf("Error dumping create database (%s): %v", database.name, err)
		}
	}
	for _, row := range result.Values {
		statement += string(row[1].AsString())
		statement += ";\n"
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write create database for %s", database.name)
	}
	m_close(o, 0, outfile, filename, 1, nil)
	if o.Checksum.SchemaChecksums {
		database.schema_checksum = write_checksum_into_file(o, conn, database, "", checksum_database_defaults)
	}
	return
}

func write_table_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool, checksum_index_filename bool) {
	var outfile *file_write
	var query string
	var err error
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
	}
	var statement string
	initialize_sql_statement(o, &statement)
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dbt.database.name, dbt.table)
	result, err := conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement = ""
	var create_table string
	row := result.Values[0]
	if o.global.schema_sequence_fix {
		create_table = filter_sequence_schemas(string(row[1].AsString()))
	} else {
		create_table = string(row[1].AsString())
	}
	statement += create_table
	statement += ";\n"
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	m_close(o, 0, outfile, filename, 1, dbt)
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
	var query, statement string
	var splited_st []string
	for _, row := range result.Values {
		set_charset(&statement, row[8].AsString(), row[9].AsString())
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write triggers data for %s", message)
			return
		}
		statement = ""
		query = fmt.Sprintf("SHOW CREATE TRIGGER `%s`.`%s`", database.name, row[0].AsString())
		result2, err = conn.Execute(query)
		if err != nil {
			log.Fatalf("show create trigger fail:%v", err)
		}
		row2 := result2.Values[0]
		var create string = string(row2[2].AsString())
		if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = remove_definer_from_gchar(create)
		}
		statement += create
		splited_st = strings.Split(statement, ";\n")
		statement = strings.Join(splited_st, "; \n")
		statement += ";\n"
		restore_charset(&statement)
		if !write_data(outfile, statement) {
			log.Fatalf("Could not write triggers data for %s", message)
			return
		}
		statement = ""
	}
	return
}

func write_triggers_definition_into_file_from_dbt(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var result *mysql.Result
	var err error
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", dbt.database.name, filename, err)
		return
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM `%s` LIKE '%s'", dbt.database.name, dbt.table)
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
	m_close(o, 0, outfile, filename, 1, dbt)
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
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	query = fmt.Sprintf("SHOW TRIGGERS FROM `%s`", database.name)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping triggers (%s): %v", database.name, err)
		} else {
			log.Fatalf("Error dumping triggers (%s): %v", database.name, err)
		}
	}
	write_triggers_definition_into_file(o, conn, result, database, database.name, outfile)
	m_close(o, 0, outfile, filename, 1, nil)
	if checksum_filename {
		database.triggers_checksum = write_checksum_into_file(o, conn, database, "", checksum_trigger_structure_from_database)
	}
	return
}

func write_view_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, filename2 string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var statement string
	var result *mysql.Result
	err = conn.UseDB(dbt.database.name)
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
	}

	if o.is_mysql_like() && o.global.set_names_statement != "" {
		statement = fmt.Sprintf("%s;\n", o.global.set_names_statement)
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW FIELDS FROM `%s`.`%s`", dbt.database.name, dbt.table)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement = ""
	statement += fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s%s%s(\n", o.Common.IdentifierQuoteCharacter, dbt.table, o.Common.IdentifierQuoteCharacter)

	for i, row := range result.Values {
		if i == 0 {
			statement += fmt.Sprintf("%s%s%s int", o.Common.IdentifierQuoteCharacter, row[0].AsString(), o.Common.IdentifierQuoteCharacter)
			continue
		}
		statement += ",\n"
		statement += fmt.Sprintf("%s%s%s int", o.Common.IdentifierQuoteCharacter, row[0].AsString(), o.Common.IdentifierQuoteCharacter)
	}
	statement += "\n);\n"
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write view schema for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", dbt.database.name, dbt.table)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	m_close(o, 0, outfile, filename, 1, dbt)
	statement = ""
	var outfile2 *file_write
	outfile2, err = m_open(o, &filename2, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
	}
	if o.is_mysql_like() && o.global.set_names_statement != "" {
		statement = fmt.Sprintf("%s;\n", o.global.set_names_statement)
	}
	statement += fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", dbt.table)
	statement += fmt.Sprintf("DROP VIEW IF EXISTS `%s`;\n", dbt.table)
	if !write_data(outfile2, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	statement = ""
	row := result.Values[0]
	set_charset(&statement, row[2].AsString(), row[3].AsString())
	create := string(row[1].AsString())
	if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
		create = remove_definer_from_gchar(create)
	}
	statement += create
	statement += ";\n"
	restore_charset(&statement)
	if !write_data(outfile2, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	m_close(o, 0, outfile2, filename2, 1, dbt)
	if checksum_filename {
		dbt.schema_checksum = write_checksum_into_file(o, conn, dbt.database, dbt.table, checksum_view_structure)
	}
	return
}

func write_sequence_definition_into_file(o *OptionEntries, conn *client.Conn, dbt *db_table, filename string, checksum_filename bool) {
	var outfile *file_write
	var query string
	var err error
	var statement string
	var result *mysql.Result
	err = conn.UseDB(dbt.database.name)

	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file (%v)", dbt.database.name, err)
		return
	}
	if o.Statement.SetNamesStr != "" {
		statement = fmt.Sprintf("%s;\n", o.Statement.SetNamesStr)
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	statement += fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", dbt.table)
	statement += fmt.Sprintf("DROP VIEW IF EXISTS `%s`;\n", dbt.table)
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema data for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SHOW CREATE SEQUENCE `%s`.`%s`", dbt.database.name, dbt.table)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement = ""
	for _, row := range result.Values {
		create := string(row[1].AsString())
		if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
			create = remove_definer_from_gchar(create)
		}
		statement += create
	}
	statement += ";\n"
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	query = fmt.Sprintf("SELECT next_not_cached_value FROM `%s`.`%s`", dbt.database.name, dbt.table)
	result, err = conn.Execute(query)
	if err != nil {
		if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
			log.Warnf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		} else {
			log.Fatalf("Error dumping schemas (%s.%s): %v", dbt.database.name, dbt.table, err)
		}
	}
	statement = ""
	for _, row := range result.Values {
		statement = fmt.Sprintf("SELECT SETVAL(`%s`, %s, 0);\n", dbt.table, row[0].AsString())
	}
	if !write_data(outfile, statement) {
		log.Fatalf("Could not write schema for %s.%s", dbt.database.name, dbt.table)
	}
	m_close(o, 0, outfile, filename, 1, dbt)
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
	outfile, err = m_open(o, &filename, "w")
	if err != nil {
		log.Fatalf("Error: DB: %s Could not create output file %s (%v)", database.name, filename, err)
	}
	var statement string
	if o.Objects.DumpRoutines {
		query = fmt.Sprintf("SHOW FUNCTION STATUS WHERE CAST(Db AS BINARY) = '%s'", database.escaped)
		result, err = conn.Execute(query)
		if err != nil {
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Error dumping functions from %s: %v", database.escaped, err)
			} else {
				log.Fatalf("Error dumping functions from %s: %v", database.escaped, err)
			}
		}
		for _, row := range result.Values {
			set_charset(&statement, row[8].AsString(), row[9].AsString())
			statement += fmt.Sprintf("DROP FUNCTION IF EXISTS `%s`;\n", row[1].AsString())
			if !write_data(outfile, statement) {
				log.Fatalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
			}
			statement = ""
			query = fmt.Sprintf("SHOW CREATE FUNCTION `%s`.`%s`", database.name, row[1].AsString())
			result2, err = conn.Execute(query)
			for _, row2 := range result2.Values {
				statement = string(row2[2].AsString())
				if o.Statement.SkipDefiner && strings.HasPrefix(statement, "CREATE") {
					statement = remove_definer(statement)
				}
				splited_st = strings.Split(statement, ";\n")
				statement = fmt.Sprintf("%s", strings.Join(splited_st, "; \n"))
				statement += ";\n"
				restore_charset(&statement)
				if !write_data(outfile, statement) {
					log.Fatalf("Could not write function data for %s.%s", database.name, row[1].AsString())
				}
			}
			statement = ""
		}
		query = fmt.Sprintf("SHOW PROCEDURE STATUS WHERE CAST(Db AS BINARY) = '%s'", database.escaped)
		result, err = conn.Execute(query)
		if err != nil {
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Error dumping stored procedures from %s: %v", database.name, err)
			} else {
				log.Fatalf("Error dumping stored procedures from %s: %v", database.name, err)
			}
		}
		for _, row := range result.Values {
			set_charset(&statement, row[8].AsString(), row[9].AsString())
			statement += fmt.Sprintf("DROP PROCEDURE IF EXISTS `%s`;\n", row[1].AsString())
			if !write_data(outfile, statement) {
				log.Fatalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
			}
			statement = ""
			query = fmt.Sprintf("SHOW CREATE PROCEDURE `%s`.`%s`", database.name, row[1].AsString())
			result2, err = conn.Execute(query)
			for _, row2 := range result2.Values {
				statement = fmt.Sprintf("%s", row2[2].AsString())
				create := string(row2[2].AsString())
				if o.Statement.SkipDefiner && strings.HasPrefix(create, "CREATE") {
					statement = remove_definer(statement)
				}
				splited_st = strings.Split(statement, ";\n")
				statement = fmt.Sprintf("%s", strings.Join(splited_st, "; \n"))
				statement += ";\n"
				restore_charset(&statement)
				if !write_data(outfile, statement) {
					log.Fatalf("Could not write function data for %s.%s", database.name, row[1].AsString())
				}
			}
			statement = ""
		}
		if checksum_filename {
			database.post_checksum = write_checksum_into_file(o, conn, database, "", checksum_process_structure)
		}
	}

	if o.Objects.DumpEvents {
		query = fmt.Sprintf("SHOW EVENTS FROM `%s`", database.name)
		result, err = conn.Execute(query)
		if err != nil {
			if o.Extra.SuccessOn1146 && mysqlError(err).Code == 1146 {
				log.Warnf("Error dumping events from %s: %v", database.name, err)
			} else {
				log.Fatalf("Error dumping events from %s: %v", database.name, err)
			}
		}
		for _, row := range result.Values {
			set_charset(&statement, row[12].AsString(), row[13].AsString())
			statement += fmt.Sprintf("DROP EVENT IF EXISTS `%s`;\n", row[1].AsString())
			if !write_data(outfile, statement) {
				log.Fatalf("Could not write stored procedure data for %s.%s", database.name, row[1].AsString())
			}
			query = fmt.Sprintf("SHOW CREATE EVENT `%s`.`%s`", database.name, row[1].AsString())
			result2, err = conn.Execute(query)
			for _, row2 := range result2.Values {
				statement = fmt.Sprintf("%s", row2[3].AsString())
				if o.Statement.SkipDefiner && strings.HasPrefix(statement, "CREATE") {
					statement = remove_definer(statement)
				}
				splited_st = strings.Split(statement, ";\n")
				statement = fmt.Sprintf("%s", strings.Join(splited_st, "; \n"))
				statement += ";\n"
				restore_charset(&statement)
				if !write_data(outfile, statement) {
					log.Fatalf("Could not write event data for %s.%s", database.name, row[1].AsString())
				}
			}
			statement = ""
		}
	}
	m_close(o, 0, outfile, filename, 1, nil)
	return
}

func free_schema_job(sj *schema_job) {
	sj.filename = ""
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
}

func free_table_checksum_job(tcj *table_checksum_job) {
	tcj.filename = ""
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

func create_job_to_dump_post(o *OptionEntries, database *database, conf *configuration) {
	create_database_related_job(o, database, conf, JOB_SCHEMA_POST, "schema-post", o.global.dump_directory)
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

func create_job_to_dump_all_databases(o *OptionEntries, conf *configuration) {
	atomic.AddInt64(&o.global.database_counter, 1)
	var j = new(job)
	j.types = JOB_DUMP_ALL_DATABASES
	j.job_data = nil
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

func create_job_to_dump_schema(o *OptionEntries, database *database, conf *configuration) {
	create_database_related_job(o, database, conf, JOB_CREATE_DATABASE, "schema-create", o.global.dump_directory)
}

func create_job_to_dump_triggers(o *OptionEntries, conn *client.Conn, dbt *db_table, conf *configuration) {
	var query string
	var result *mysql.Result
	var err error
	query = fmt.Sprintf("SHOW TRIGGERS FROM `%s` LIKE '%s'", dbt.database.name, dbt.escaped_table)
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

func do_JOB_CREATE_DATABASE(o *OptionEntries, td *thread_data, job *job) {
	var dj = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping schema create for `%s`", td.thread_id, dj.database.name)
	write_schema_definition_into_file(o, td.thrconn, dj.database, dj.filename)
	free_database_job(dj)
}

func do_JOB_CREATE_TABLESPACE(o *OptionEntries, td *thread_data, job *job) {
	var ctj = job.job_data.(*create_tablespace_job)
	log.Infof("Thread %d: dumping create tablespace if any", td.thread_id)
	write_tablespace_definition_into_file(o, td.thrconn, ctj.filename)
	free_create_tablespace_job(ctj)
}

func do_JOB_SCHEMA_POST(o *OptionEntries, td *thread_data, job *job) {
	var sp = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping SP and VIEWs for `%s`", td.thread_id, sp.database.name)
	write_routines_definition_into_file(o, td.thrconn, sp.database, sp.filename, sp.checksum_filename)
	free_database_job(sp)
}

func do_JOB_SCHEMA_TRIGGERS(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*database_job)
	log.Infof("Thread %d: dumping triggers for `%s`", td.thread_id, sj.database.name)
	write_triggers_definition_into_file_from_database(o, td.thrconn, sj.database, sj.filename, sj.checksum_filename)
	free_database_job(sj)
}

func do_JOB_VIEW(o *OptionEntries, td *thread_data, job *job) {
	var vj = job.job_data.(*view_job)
	log.Infof("Thread %d: dumping view for `%s`.`%s`", td.thread_id, vj.dbt.database.name, vj.dbt.table)
	write_view_definition_into_file(o, td.thrconn, vj.dbt, vj.tmp_table_filename, vj.view_filename, vj.checksum_filename)
}

func do_JOB_SEQUENCE(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*sequence_job)
	log.Infof("Thread %d dumping sequence for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_sequence_definition_into_file(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
}

func do_JOB_SCHEMA(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping schema for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_table_definition_into_file(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename, sj.checksum_index_filename)
	free_schema_job(sj)
}

func do_JOB_TRIGGERS(o *OptionEntries, td *thread_data, job *job) {
	var sj = job.job_data.(*schema_job)
	log.Infof("Thread %d: dumping triggers for `%s`.`%s`", td.thread_id, sj.dbt.database.name, sj.dbt.table)
	write_triggers_definition_into_file_from_dbt(o, td.thrconn, sj.dbt, sj.filename, sj.checksum_filename)
	free_schema_job(sj)
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
}

func new_table_job(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step *chunk_step, update_where bool) *table_job {
	var tj = new(table_job)
	tj.partition = partition
	tj.chunk_step = chunk_step
	tj.where = ""
	tj.order_by = order_by
	tj.nchunk = nchunk
	tj.sub_part = 0
	tj.dat_file = nil
	tj.dat_filename = ""
	tj.sql_file = nil
	tj.sql_filename = ""
	tj.exec_out_filename = ""
	tj.dbt = dbt
	tj.st_in_file = 0
	tj.filesize = 0
	tj.char_chunk_part = int(o.Chunks.CharChunk)
	tj.child_process = 0
	if update_where {
		update_where_on_table_job(o, nil, tj)
	}
	return tj
}

func create_job_to_dump_chunk(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step *chunk_step, queue *asyncQueue, update_where bool) {
	var j = new(job)
	var tj = new_table_job(o, dbt, partition, nchunk, order_by, chunk_step, update_where)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	queue.push(j)
}

func update_files_on_table_job(o *OptionEntries, tj *table_job) bool {
	if tj.sql_file == nil {
		if tj.dbt.chunk_type == INTEGER && tj.chunk_step != nil && o.global.min_rows_per_file == o.global.rows_per_file && o.global.max_rows_per_file == o.global.rows_per_file {
			if tj.chunk_step.integer_step.is_unsigned {
				tj.sub_part = uint(tj.chunk_step.integer_step.types.unsign.min/tj.chunk_step.integer_step.step + 1)
			} else {
				tj.sub_part = uint(tj.chunk_step.integer_step.types.sign.min/int64(tj.chunk_step.integer_step.step) + 1)
			}
		}
		if o.Statement.LoadData {
			initialize_load_data_fn(o, tj)
			tj.sql_filename = build_data_filename(o.global.dump_directory, tj.dbt.database.filename, tj.dbt.table_filename, tj.nchunk, tj.sub_part)
			tj.sql_file, _ = m_open(o, &tj.sql_filename, "w")
			return true
		} else {
			initialize_sql_fn(o, tj)
		}
	}
	return false
}

func create_job_to_dump_chunk_without_enqueuing(o *OptionEntries, dbt *db_table, partition string, nchunk uint64, order_by string, chunk_step *chunk_step, update_where bool) *job {
	var j = new(job)
	var tj = new_table_job(o, dbt, partition, nchunk, order_by, chunk_step, update_where)
	j.job_data = tj
	if dbt.is_innodb {
		j.types = JOB_DUMP
	} else {
		j.types = JOB_DUMP_NON_INNODB
	}
	j.job_data = tj
	return j
}

func get_ref_table(o *OptionEntries, k string) string {
	o.global.ref_table_mutex.Lock()
	var val string
	val, _ = o.global.ref_table[k]
	if val == "" {
		var t = k
		val = determine_filename(o, t)
		o.global.ref_table[t] = val
	}
	o.global.ref_table_mutex.Unlock()
	return val
}

func create_job_to_determine_chunk_type(dbt *db_table, queue *asyncQueue) {
	var j = new(job)
	j.job_data = dbt
	j.types = JOB_DETERMINE_CHUNK_TYPE
	queue.push(j)
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

func get_primary_key_string(o *OptionEntries, conn *client.Conn, database string, table string) string {
	if !o.Extra.OrderByPrimaryKey {
		return ""
	}
	var res *mysql.Result
	var field_list string
	var err error
	query := fmt.Sprintf("SELECT k.COLUMN_NAME, ORDINAL_POSITION FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type IN ('PRIMARY KEY', 'UNIQUE') AND t.table_schema='%s' AND t.table_name='%s' ORDER BY t.constraint_type, ORDINAL_POSITION; ", database, table)
	res, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("get %s.%s prumary key fail:%v", database, table, err)
	}
	var first = true
	for _, row := range res.Values {
		if first {
			first = false
		} else if row[1].AsInt64() > 1 {
			field_list += ","
		} else {
			break
		}
		tb := fmt.Sprintf("`%s`", row[0].AsString())
		field_list += tb
	}
	return field_list
}

func initialize_sql_fn(o *OptionEntries, tj *table_job) {
	tj.child_process, tj.sql_file = initialize_fn(o, &tj.sql_filename, tj.dbt, tj.sql_file, tj.nchunk, tj.sub_part, "sql", build_data_filename, &tj.exec_out_filename)
}

func initialize_load_data_fn(o *OptionEntries, tj *table_job) {
	tj.child_process, tj.dat_file = initialize_fn(o, &tj.dat_filename, tj.dbt, tj.dat_file, tj.nchunk, tj.sub_part, "dat", build_load_data_filename, &tj.exec_out_filename)
}

func initialize_fn(o *OptionEntries, sql_filename *string, dbt *db_table, sql_file *file_write, fn uint64, sub_part uint, extension string, f build_filename_fun, stdout_fn *string) (int, *file_write) {
	_ = stdout_fn
	_ = extension
	var r int
	var err error
	if *sql_filename != "" {
		*sql_filename = ""
	}
	*sql_filename = f(o.global.dump_directory, dbt.database.filename, dbt.table_filename, fn, sub_part)
	sql_file, err = m_open(o, sql_filename, "w")
	if err != nil {
		log.Warnf("open file %s fail:%v", *sql_filename, err)
	}
	return r, sql_file

}
