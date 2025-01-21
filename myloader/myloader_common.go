package myloader

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ignore_set_list            []string
	refresh_table_list_counter int64 = 1
	db_hash_mutex              *sync.Mutex
	tbl_hash                   map[string]string
	db_hash                    map[string]*database
	database_db                *database
	source_gtid                string
)

type check_sum func(conn *DBConnection, database, table string) string

func initialize_common() {
	if IgnoreSet != "" {
		var ignore_set_items = strings.Split(IgnoreSet, ",")
		var i = 0
		for i = 0; i < len(ignore_set_items); i++ {
			ignore_set_list = append(ignore_set_list, ignore_set_items[i])
		}
	}
	refresh_table_list_counter = int64(RefreshTableListInterval)
	db_hash_mutex = G_mutex_new()
	tbl_hash = make(map[string]string)
	db_hash = make(map[string]*database)
	if DB != "" {
		database_db = get_db_hash(DB, DB)
	}
}

func is_in_list(haystack string, list []string) bool {
	return slices.Contains(list, haystack)
}

func is_in_ignore_set_list(haystack string) bool {
	return is_in_list(haystack, ignore_set_list)
}

func get_value(kf *ini.File, group string, key string) string {
	section := kf.Section(group)
	if !section.HasKey(key) {
		return ""
	}
	value, _ := section.GetKey(key)
	return value.Value()
}

func change_master(kf *ini.File, group string, output_statement *GString) {
	var val string
	var i uint
	var length int
	var traditional_change_source *GString = G_string_new("")
	var aws_change_source *GString = G_string_new("")
	G_string_append(traditional_change_source, Change_replication_source)
	G_string_append(traditional_change_source, " TO ")
	var group_name = strings.SplitN(group, ".", 2)
	var channel_name string
	if len(group_name) > 1 {
		channel_name = group_name[1]
	}
	var keys = kf.Section(group).Keys()
	length = len(keys)
	var exec_change_source, exec_reset_replica, exec_start_replica int
	var auto_position bool
	var source_ssl bool
	var source_host string
	var source_port uint = 3306
	var source_user string
	var source_password string
	var source_log_file string
	var source_log_pos uint64
	var first bool = true

	for i = 0; i < uint(length); i++ {
		if strings.EqualFold(keys[i].Name(), "myloader_exec_reset_slave") && strings.EqualFold(keys[i].Name(), "myloader_exec_reset_replica") {
			exec_reset_replica, _ = strconv.Atoi(keys[i].Value())
		} else if strings.EqualFold(keys[i].Name(), "myloader_exec_change_master") && strings.EqualFold(keys[i].Name(), "myloader_exec_change_source") {
			if G_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				exec_change_source = 1
			}
		} else if strings.EqualFold(keys[i].Name(), "myloader_exec_start_slave") && strings.EqualFold(keys[i].Name(), "myloader_exec_start_replica") {
			if G_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				exec_start_replica = 1
			}
		} else if strings.EqualFold(keys[i].Name(), "executed_gtid_set") {
			source_gtid = G_key_file_get_value(kf, group, keys[i].Name())
		} else if strings.EqualFold(keys[i].Name(), "channel_name") {
			channel_name = G_key_file_get_value(kf, group, keys[i].Name())
		} else {
			if first {
				first = false
			} else {
				G_string_append_printf(traditional_change_source, ", ")
			}
			if strings.EqualFold(keys[i].Name(), "SOURCE_AUTO_POSITION") {
				auto_position = G_ascii_strtoull(keys[i].Value()) > 0
				G_string_append_printf(traditional_change_source, "%s = %d", keys[i], auto_position)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_SSL") {
				source_ssl = G_ascii_strtoull(G_key_file_get_value(kf, group, keys[i].Name())) > 0
				G_string_append_printf(traditional_change_source, "%s = %d", keys[i], boolToInt(source_ssl))
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_HOST") {
				source_host = G_key_file_get_value(kf, group, keys[i].Name())
				G_string_append_printf(traditional_change_source, "%s = %s", keys[i], source_host)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_PORT") {
				source_port = uint(G_ascii_strtoull(G_key_file_get_value(kf, group, keys[i].Name())))
				G_string_append_printf(traditional_change_source, "%s = %d", keys[i], source_port)

			} else if strings.EqualFold(keys[i].Name(), "SOURCE_USER") {
				source_user = G_key_file_get_value(kf, group, keys[i].Name())
				G_string_append_printf(traditional_change_source, "%s = %s", keys[i], source_user)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_PASSWORD") {
				source_password = G_key_file_get_value(kf, group, keys[i].Name())
				G_string_append_printf(traditional_change_source, "%s = %s", keys[i], source_password)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_LOG_FILE") {
				source_log_file = G_key_file_get_value(kf, group, keys[i].Name())
				G_string_append_printf(traditional_change_source, "%s = %s", keys[i], source_log_file)

			} else if strings.EqualFold(keys[i].Name(), "SOURCE_LOG_POS") {
				source_log_pos, _ = strconv.ParseUint(G_key_file_get_value(kf, group, keys[i].Name()), 10, 64)

				G_string_append_printf(traditional_change_source, "%s = %d", keys[i], source_log_pos)
			} else {
				val = G_key_file_get_value(kf, group, keys[i].Name())
				if val != "" {
					G_string_append_printf(traditional_change_source, "%s = %s", keys[i], val)
				}
			}
		}
	}
	if auto_position {
		G_string_append(aws_change_source, "CALL mysql.rds_set_external_master_with_auto_position")
	} else {
		G_string_append(aws_change_source, "CALL mysql.rds_set_external_master")
	}
	G_string_append_printf(aws_change_source, "( %s, %d, %s, %s, ", source_host, source_port, source_user, source_password)
	if !auto_position {
		G_string_append_printf(aws_change_source, "%s, %d, ", source_log_file, source_log_pos)
	}
	G_string_append_printf(aws_change_source, "%d );\n", boolToInt(source_ssl))
	G_string_append(traditional_change_source, "")
	G_string_append(traditional_change_source, "FOR CHANNEL ")
	if channel_name == "" {
		G_string_append(traditional_change_source, "''")
	} else {
		G_string_append(traditional_change_source, channel_name)
	}
	G_string_append(traditional_change_source, ";\n")

	if SetGtidPurge {
		if Source_control_command == TRADITIONAL {
			G_string_append_printf(output_statement, "RESET MASTER;\nSET GLOBAL gtid_purged=%s;\n", source_gtid)
		}
		if Source_control_command == AWS {
			G_string_append_printf(output_statement, "CALL mysql.rds_set_gtid_purged (%s);\n", source_gtid)
		}
	}
	if intToBool(exec_change_source) {
		if intToBool(exec_reset_replica) {
			G_string_append(output_statement, Stop_replica)
			G_string_append(output_statement, ";\n")

			G_string_append(output_statement, Reset_replica)
			if Source_control_command == TRADITIONAL {
				G_string_append(output_statement, " ")
				if exec_reset_replica > 1 {
					G_string_append(output_statement, "ALL ")
				}
				if channel_name != "" {
					G_string_append_printf(output_statement, "FOR CHANNEL %s ", channel_name)
				}
			}
			G_string_append(output_statement, ";\n")
		}
		if Source_control_command == TRADITIONAL {
			G_string_append(output_statement, traditional_change_source.Str.String())
		} else {
			G_string_append(output_statement, aws_change_source.Str.String())
		}

		if intToBool(exec_start_replica) {
			G_string_append(output_statement, Start_replica)
			G_string_append(output_statement, ";\n")
		}
		if Source_control_command == TRADITIONAL {
			if channel_name != "" {
				log.Infof("Change master will be executed for channel: %s", channel_name)
			} else {
				log.Infof("Change master will be executed for channel: default channel")
			}
		}

	}
}

func m_filename_has_suffix(str string, suffix string) bool {
	if has_exec_per_thread_extension(str) {
		return strings.ToLower(path.Ext(str)) == ExecPerThreadExtension
	} else if strings.HasSuffix(str, GZIP_EXTENSION) {
		return strings.HasSuffix(path.Ext(str[:len(str)-len(GZIP_EXTENSION)]), suffix)
	} else if strings.HasSuffix(str, ZSTD_EXTENSION) {
		strings.HasSuffix(path.Ext(str[:len(str)-len(ZSTD_EXTENSION)]), suffix)
	}
	return strings.HasSuffix(str, suffix)
}

func new_database(db_name string, filename string) *database {
	var d = new(database)
	d.name = db_name
	if DB != "" {
		d.real_database = DB
	} else {
		d.real_database = db_name
	}
	d.filename = filename
	d.mutex = G_mutex_new()
	d.queue = G_async_queue_new(BufferSize)
	d.schema_state = NOT_FOUND
	d.schema_checksum = ""
	d.post_checksum = ""
	d.triggers_checksum = ""
	return d
}

func get_db_hash(filename, name string) *database {
	db_hash_mutex.Lock()
	d, _ := db_hash[filename]
	if d == nil {
		d = new_database(name, filename)
		db_hash[filename] = d
		if filename != name {
			db_hash[name] = d
		}
		d = db_hash[name]
	} else {
		if filename != name {
			d.name = name
			if DB != "" {
				d.real_database = DB
			} else {
				d.real_database = d.name
			}
		}
	}
	db_hash_mutex.Unlock()
	return d
}

func eval_table(db_name string, table_name string, mutex *sync.Mutex) bool {
	if table_name == "" {
		log.Errorf("Table name is null on eval_table()")
	}
	mutex.Lock()
	if len(Tables) > 0 {
		if !Is_table_in_list(db_name, table_name, Tables) {
			mutex.Unlock()
			return false
		}
	}
	if TablesSkiplistFile != "" && Check_skiplist(db_name, table_name) {
		mutex.Unlock()
		return false
	}
	mutex.Unlock()
	return Eval_regex(db_name, table_name)
}

func execute_use(cd *connection_data) bool {
	var query = fmt.Sprintf("USE `%s`", cd.current_database.real_database)
	_ = cd.thrconn.Execute(query)
	if cd.thrconn.Err != nil {
		return true
	}
	return false
}

func execute_use_if_needs_to(cd *connection_data, database *database, msg string) {
	if database != nil && (DB == "" || cd.current_database == nil) {
		if cd.current_database == nil || strings.Compare(database.real_database, cd.current_database.real_database) != 0 {
			cd.current_database = database
			if execute_use(cd) {
				log.Criticalf("Thread %d: Error switching to database `%s` %s: %v", cd.thread_id, cd.current_database.real_database, msg, cd.thrconn.Err)
			}
		}
	}
	return
}

func get_file_type(filename string) file_type {
	if strings.Compare(filename, "metadata") == 0 || strings.Contains(filename, "metadata.header") || (strings.Contains(filename, "metadata.partial") && strings.HasSuffix(filename, ".sql")) {
		return METADATA_GLOBAL
	}
	if SourceDb != "" && !(strings.HasPrefix(filename, SourceDb) && len(filename) > len(SourceDb) && (strings.Contains(filename[:len(SourceDb)], ".")) ||
		strings.Contains(filename[:len(SourceDb)], "-")) && !strings.HasPrefix(filename, "mydumper_") {
		return IGNORED
	}
	if m_filename_has_suffix(filename, "-schema.sql") {
		return SCHEMA_TABLE
	}
	if strings.Compare(filename, "all-schema-create-tablespace.sql") == 0 {
		return SCHEMA_TABLESPACE
	}
	if strings.Compare(filename, "resume") == 0 {
		if !Resume {
			log.Critical("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.")
		}
		return RESUME
	}
	if strings.Compare(filename, "resume.partial") == 0 {
		log.Critical("\"resume.partial file found. Remove it and restart process if you consider that it will be safe.")
	}

	if m_filename_has_suffix(filename, "-checksum") {
		return CHECKSUM
	}

	if m_filename_has_suffix(filename, "-schema-view.sql") {
		return SCHEMA_VIEW
	}

	if m_filename_has_suffix(filename, "-schema-sequence.sql") {
		return SCHEMA_SEQUENCE
	}

	if m_filename_has_suffix(filename, "-schema-triggers.sql") {
		return SCHEMA_TRIGGER
	}

	if m_filename_has_suffix(filename, "-schema-post.sql") {
		return SCHEMA_POST
	}

	if m_filename_has_suffix(filename, "-schema-create.sql") {
		return SCHEMA_CREATE
	}

	if m_filename_has_suffix(filename, ".sql") {
		return DATA
	}

	if m_filename_has_suffix(filename, ".dat") {
		return LOAD_DATA
	}
	return IGNORED
}

func get_database_table_from_file(filename string, sufix string, database *string, table *string) {
	split_filename := strings.Split(filename, sufix)
	split := strings.Split(split_filename[0], ".")
	count := len(split)
	if count > 2 {
		log.Warnf("We need to get the db and table name from the create table statement")
		return
	}
	if count == 1 {
		*database = split[0]
		return
	}
	*table = split[1]
	*database = split[0]
	return
}

/*
func append_alter_table(alter_table_statement *string, database string, table string) {
	*alter_table_statement += fmt.Sprintf("ALTER TABLE `%s`.`%s` ", database, table)
}

func finish_alter_table(alter_table_statement string) string {
	c := len(alter_table_statement) - 5
	l := alter_table_statement[c:]
	if strings.Contains(l, ";") {
		alter_table_statement += "\n"
	} else {
		alter_table_statement += ";\n"
	}
	return alter_table_statement
}
*/
/*
func process_create_table_statement(statement string, create_table_statement *string, alter_table_statement *string, alter_table_constraint_statement *string, dbt *db_table, split_indexes bool) int {
	var flag int
	var split_file = strings.Split(statement, "\n")
	var autoinc_column string
	append_alter_table(alter_table_statement, dbt.database.real_database, dbt.real_table)
	append_alter_table(alter_table_constraint_statement, dbt.database.real_database, dbt.real_table)
	var i, fulltext_counter int
	for i = 0; i < len(split_file); i++ {
		if split_indexes && strings.HasPrefix(split_file[i], "  KEY") ||
			strings.HasPrefix(split_file[i], "  UNIQUE") ||
			strings.HasPrefix(split_file[i], "  SPATIAL") ||
			strings.HasPrefix(split_file[i], "  FULLTEXT") ||
			strings.HasPrefix(split_file[i], "  INDEX") {
			if autoinc_column != "" && strings.HasPrefix(split_file[i], autoinc_column) {
				*create_table_statement += split_file[i]
				*create_table_statement += "\n"
			} else {
				flag |= IS_ALTER_TABLE_PRESENT
				if strings.HasPrefix(split_file[i], "  FULLTEXT") {
					fulltext_counter++
				}
				if fulltext_counter > 1 {
					fulltext_counter = 1
					*alter_table_statement = finish_alter_table(*alter_table_statement)
					append_alter_table(alter_table_statement, dbt.database.real_database, dbt.real_table)
				}
				*alter_table_statement += "\n ADD"
				*alter_table_statement += split_file[i]
			}
		} else {
			if strings.HasPrefix(split_file[i], "  CONSTRAINT") {
				flag |= INCLUDE_CONSTRAINT
				*alter_table_constraint_statement += "\n ADD"
				*alter_table_constraint_statement += split_file[i]
			} else {
				if split_file[i] == "AUTO_INCREMENT" {
					var autoinc_split = strings.SplitN(split_file[i], "`", 3)
					autoinc_column = fmt.Sprintf("(`%s`", autoinc_split[1])
				}
				*create_table_statement += split_file[i]
				*create_table_statement += "\n"
			}
		}
		if split_file[i] == "ENGINE=InnoDB" {
			flag |= IS_INNODB_TABLE
		}
	}
	return flag
}
*/

func process_create_table_statement(statement *GString, create_table_statement *GString, alter_table_statement *GString, alter_table_constraint_statement *GString, dbt *db_table, split_indexes bool) int {
	return Global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.real_table, split_indexes)
}

func build_dbt_key(a, b string) string {
	return fmt.Sprintf("%s%s%s.%s%s%s", Identifier_quote_character, a, Identifier_quote_character, Identifier_quote_character, b, Identifier_quote_character)
}

func compare_dbt(a *db_table, b *db_table, table_hash map[string]*db_table) bool {
	var a_key = build_dbt_key(a.database.real_database, a.table)
	var b_key = build_dbt_key(b.database.real_database, b.table)
	a_val, _ := table_hash[a_key]
	b_val, _ := table_hash[b_key]
	return a_val.rows < b_val.rows
}

func compare_dbt_short(a *db_table, b *db_table) bool {
	return a.rows < b.rows
}

func refresh_table_list_without_table_hash_lock(conf *configuration, force bool) {
	if force || G_atomic_int_dec_and_test(&refresh_table_list_counter) {
		var table_list []*db_table
		conf.table_list_mutex.Lock()
		var dbt *db_table
		for _, dbt = range conf.table_hash {
			table_list = append(table_list, dbt)
		}
		conf.table_list = table_list
		atomic.AddInt64(&refresh_table_list_counter, int64(RefreshTableListInterval))
		conf.table_list_mutex.Unlock()
	}
}

func refresh_table_list(conf *configuration) {
	conf.table_hash_mutex.Lock()
	refresh_table_list_without_table_hash_lock(conf, true)
	conf.table_hash_mutex.Unlock()
}

func checksum_template(dbt_checksum, checksum, err_templ, info_templ, message, _db, _table string) bool {
	G_assert(checksum_mode != CHECKSUM_SKIP)
	if dbt_checksum != checksum {
		if _table != "" {
			if checksum_mode == CHECKSUM_WARN {
				log.Warnf(err_templ, message, _db, _table, checksum, dbt_checksum)
			} else {
				log.Criticalf(err_templ, message, _db, _table, checksum, dbt_checksum)
			}
		} else {
			if checksum_mode == CHECKSUM_WARN {
				log.Warnf(err_templ, message, _db, checksum, dbt_checksum)
			} else {
				log.Critical(err_templ, message, _db, checksum, dbt_checksum)
			}
		}
		return false
	} else {
		log.Infof(info_templ, message, _db, _table)
	}
	return true
}

func checksum_dbt_template(dbt *db_table, dbt_checksum string, conn *DBConnection, message string, fun check_sum) bool {
	var _db = dbt.database.real_database
	var _table = dbt.real_table
	var err error
	var checksum string
	checksum = fun(conn, _db, _table)
	if conn.Err != nil {
		log.Warnf("Error getting checksum for %s.%s: %v", _db, _table, err)
	}
	return checksum_template(dbt_checksum, checksum, "%s mismatch found for %s.%s: got %s, expecting %s", "%s confirmed for %s.%s", message, _db, _table)
}

func checksum_database_template(_db, dbt_checksum string, conn *DBConnection, message string, fun check_sum) bool {
	var err error
	var checksum string
	checksum = fun(conn, _db, "")
	if conn.Err != nil {
		log.Warnf("Error getting checksum for %s: %v", _db, err)
	}
	return checksum_template(dbt_checksum, checksum, "%s mismatch found for %s: got %s, expecting %s", "%s confirmed for %s", message, _db, "")
}

func checksum_dbt(dbt *db_table, conn *DBConnection) bool {
	var checksum_ok = true
	if checksum_mode != CHECKSUM_SKIP {
		if !NoSchemas {
			if dbt.schema_checksum != "" {
				if dbt.is_view {
					checksum_ok = checksum_dbt_template(dbt, dbt.schema_checksum, conn, "View checksum", Checksum_view_structure)
				} else {
					checksum_ok = checksum_dbt_template(dbt, dbt.schema_checksum, conn, "Structure checksum", Checksum_table_structure)
				}
			}
			if dbt.indexes_checksum != "" {
				checksum_ok = checksum_dbt_template(dbt, dbt.indexes_checksum, conn, "Schema index checksum", Checksum_table_indexes)
			}
		}
		if dbt.triggers_checksum != "" && !SkipTriggers {
			checksum_ok = checksum_dbt_template(dbt, dbt.triggers_checksum, conn, "Trigger checksum", Checksum_trigger_structure)
		}
		if dbt.data_checksum != "" && !NoData {
			checksum_ok = checksum_dbt_template(dbt, dbt.data_checksum, conn, "Data checksum", Checksum_table)
		}
	}
	return checksum_ok
}

func has_exec_per_thread_extension(filename string) bool {
	return ExecPerThreadExtension != "" && strings.HasSuffix(filename, ExecPerThreadExtension)
}

func execute_file_per_thread(sql_fn string, exec string) (*osFile, error) {
	var sql_file *os.File
	var outfile *osFile
	var err error
	sql_file, err = os.Open(sql_fn)
	if err != nil {
		log.Errorf("fail open file name: %s ", sql_fn)
		return nil, err
	}
	outfile = new(osFile)
	outfile.file = sql_file
	switch exec {
	case GZIP_EXTENSION:
		var out *gzip.Reader
		out, err = gzip.NewReader(sql_file)

		outfile.close = out.Close
		outfile.writerTo = out.WriteTo
		outfile.metadata = out.Name
		outfile.read = out.Read

	case ZSTD_EXTENSION:
		var out *zstd.Decoder
		out, err = zstd.NewReader(sql_file)
		outfile.close = func() error {
			out.Close()
			return nil
		}
		outfile.writerTo = out.WriteTo
		outfile.metadata = ""
		outfile.read = out.Read
	default:
		outfile.close = sql_file.Close
		outfile.write = sql_file.Write
		outfile.writerTo = sql_file.WriteTo
		outfile.metadata = sql_file.Name()
		outfile.read = sql_file.Read
		// outfile.sync = sql_file.Sync()
	}

	return outfile, nil
}

func get_command_and_basename(filename string, basename *string) bool {
	var length int
	if has_exec_per_thread_extension(filename) {
		length = len(ExecPerThreadExtension)
	} else if strings.HasSuffix(filename, ZSTD_EXTENSION) {
		length = len(ZSTD_EXTENSION)
	} else if strings.HasSuffix(filename, GZIP_EXTENSION) {
		length = len(GZIP_EXTENSION)
	}
	if length != 0 {
		*basename = path.Base(filename)
		return true
	}
	return false
}

func initialize_thread_data(td *thread_data, conf *configuration, status thread_states, thread_id uint, dbt *db_table) {
	(*td).conf = conf
	(*td).status = status
	(*td).thread_id = thread_id
	(*td).granted_connections = 0
	(*td).dbt = dbt
}

func m_query(conn *DBConnection, query string, log_fun func(string, ...any), args string) bool {
	_ = conn.Execute(query)

	if conn.Err != nil {
		log_fun(args)
		return false
	}

	return true
}

func status2str(status schema_status) string {
	switch status {
	case NOT_FOUND:
		return "NOT_FOUND"
	case NOT_FOUND_2:
		return "NOT_FOUND_2"
	case NOT_CREATED:
		return "NOT_CREATED"
	case CREATING:
		return "CREATING"
	case CREATED:
		return "CREATED"
	case DATA_DONE:
		return "DATA_DONE"
	case INDEX_ENQUEUED:
		return "INDEX_ENQUEUED"
	case ALL_DONE:
		return "ALL_DONE"
	}
	return ""
}
