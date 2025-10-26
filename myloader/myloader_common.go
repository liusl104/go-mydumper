package myloader

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"os/exec"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	refresh_table_list_counter              int64 = 1
	db_hash_mutex                           *sync.Mutex
	tbl_hash                                map[string]string
	db_hash                                 map[string]*database
	database_db                             *database
	max_number_tables_to_sort_in_table_list int = 100000
	zstd_decompress_cmd                     []string
	gzip_decompress_cmd                     []string
)

type replication_statements struct {
	gtid_purge                *GString
	stop_replica              *GString
	reset_replica             *GString
	start_replica_until       *GString
	change_replication_source *GString
	start_replica             *GString
}

type check_sum func(conn *DBConnection, database, table string) string

func initialize_common() {
	refresh_table_list_counter = int64(RefreshTableListInterval)
	db_hash_mutex = G_mutex_new()
	tbl_hash = make(map[string]string)
	db_hash = make(map[string]*database)
	if DB != "" {
		database_db = get_db_hash(DB, DB)
	}
	var err error
	var tmpcmd string
	if ExecPerThread != "" {
		exec_per_thread_cmd = strings.Split(ExecPerThread, " ")
		tmpcmd, err = exec.LookPath(exec_per_thread_cmd[0])
		if err != nil {
			log.Criticalf("%s was not found in PATH, use --exec-per-thread for non default locations", exec_per_thread_cmd[0])
		}
		exec_per_thread_cmd[0] = tmpcmd
	}
	tmpcmd, err = exec.LookPath(ZSTD)
	if err != nil {
		log.Warnf("%s was not found in PATH, use --exec-per-thread for non default locations", ZSTD)
	} else {
		zstd_decompress_cmd = strings.Split(fmt.Sprintf("%s -c -d", tmpcmd), " ")
	}
	tmpcmd, err = exec.LookPath(GZIP)
	if err != nil {
		log.Warnf("%s was not found in PATH, use --exec-per-thread for non default locations", GZIP)
	} else {
		gzip_decompress_cmd = strings.Split(fmt.Sprintf("%s -c -d", tmpcmd), " ")
	}
}

func is_in_list(haystack string, list []string) bool {
	return slices.Contains(list, haystack)
}

func is_in_ignore_set_list(haystack string) bool {
	return is_in_list(haystack, ignore_set_list)
}

func remove_ignore_set_session_from_hash() {
	var l = ignore_set_list
	for _, data := range l {
		delete(set_session_hash, data)
	}
}

func get_value(kf *ini.File, group string, key string) string {
	section := kf.Section(group)
	if !section.HasKey(key) {
		return ""
	}
	value, _ := section.GetKey(key)
	return value.Value()
}

func execute_replication_commands(conn *DBConnection, statement string) {
	M_query_warning(conn, "COMMIT", "COMMIT failed")
	var line []string = strings.Split(statement, "\n;")
	for i := 0; i < len(line); i++ {
		var str *GString = G_string_new(line[i])
		G_string_append_c(str, ';')
		M_query_warning(conn, str.Str.String(), "Sending replication command: %s", str.Str.String())
	}
	M_query_warning(conn, "START TRANSACTION", "START TRANSACTION failed")
}

func change_master(kf *ini.File, group string, rs *replication_statements, rep_set *Replication_settings) {
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
	var _exec_change_source, _exec_reset_replica, _exec_start_replica, _exec_start_replica_until bool
	var _auto_position bool
	var _source_ssl bool
	var source_host string
	var source_port uint = 3306
	var source_user string
	var source_password string
	var source_log_file string
	var source_log_pos uint64
	var first bool = true
	var source_gtid string

	for i = 0; i < uint(length); i++ {
		if strings.EqualFold(keys[i].Name(), "myloader_exec_reset_slave") && strings.EqualFold(keys[i].Name(), "myloader_exec_reset_replica") {
			_exec_reset_replica = keys[i].Value() != "0"
		} else if strings.EqualFold(keys[i].Name(), "myloader_exec_change_master") && strings.EqualFold(keys[i].Name(), "myloader_exec_change_source") {
			if G_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				_exec_change_source = true
			}
		} else if strings.EqualFold(keys[i].Name(), "myloader_exec_start_slave") && strings.EqualFold(keys[i].Name(), "myloader_exec_start_replica") {
			if G_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				_exec_start_replica = true
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
				_auto_position = G_ascii_strtoull(keys[i].Value()) > 0
				G_string_append_printf(traditional_change_source, "%s = %v", keys[i], _auto_position)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_SSL") {
				_source_ssl = G_ascii_strtoull(G_key_file_get_value(kf, group, keys[i].Name())) > 0
				G_string_append_printf(traditional_change_source, "%s = %d", keys[i], boolToInt(_source_ssl))
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
	if rep_set.Enabled {
		_exec_reset_replica = rep_set.Exec_reset_replica
		_exec_change_source = rep_set.Exec_change_source
		_exec_start_replica = rep_set.Exec_start_replica
		_source_ssl = rep_set.Source_ssl
		_auto_position = rep_set.Auto_position
		_exec_start_replica_until = rep_set.Exec_start_replica_until
	}
	G_assert((_exec_start_replica_until != false && (_exec_reset_replica == false && _exec_change_source == false)) || (_exec_start_replica_until == false))
	if _source_ssl {
		G_string_append_printf(traditional_change_source, "SOURCE_SSL = %d", boolToInt(_source_ssl))
	}

	if _auto_position {
		G_string_append_printf(traditional_change_source, "SOURCE_AUTO_POSITION = %d", boolToInt(_auto_position))
		G_string_append(aws_change_source, "CALL mysql.rds_set_external_master_with_auto_position")
	} else {
		G_string_append(aws_change_source, "CALL mysql.rds_set_external_master")
	}
	G_string_append_printf(aws_change_source, "( %s, %d, %s, %s, ", source_host, source_port, source_user, source_password)
	if !_auto_position {
		G_string_append_printf(aws_change_source, "%s, %d, %d, );\\n", source_log_file, source_log_pos, boolToInt(_source_ssl))
	} else {
		G_string_append_printf(aws_change_source, "%d, 0);\n", boolToInt(_source_ssl))
	}
	G_string_append(traditional_change_source, "FOR CHANNEL '")
	if channel_name != "" {
		G_string_append(traditional_change_source, channel_name)
	}
	G_string_append(traditional_change_source, "';\n")

	if SetGtidPurge {
		if rs.gtid_purge == nil {
			rs.gtid_purge = G_string_new("")
		}
		if Source_control_command == TRADITIONAL {
			G_string_append_printf(rs.gtid_purge, "%s;\nSET GLOBAL gtid_purged=%s;\n", Reset_replica, source_gtid)
		}
		if Source_control_command == AWS {
			G_string_append_printf(rs.gtid_purge, "CALL mysql.rds_set_gtid_purged (%s);\n", source_gtid)
		}
	}
	if _exec_reset_replica {
		if rs.reset_replica == nil {
			rs.reset_replica = G_string_new("")
		}
		G_string_append(rs.reset_replica, Stop_replica)
		G_string_append(rs.reset_replica, ";\n")

		G_string_append(rs.reset_replica, Reset_replica)
		if Source_control_command == TRADITIONAL {
			G_string_append(rs.reset_replica, " ")
			if _exec_reset_replica {
				G_string_append(rs.reset_replica, "ALL ")
			}
			if channel_name != "" {
				G_string_append_printf(rs.reset_replica, "FOR CHANNEL '%s'", channel_name)
			}
		}
		G_string_append(rs.reset_replica, ";\n")
	}
	if _exec_change_source {
		if rs.change_replication_source == nil {
			rs.change_replication_source = G_string_new("")
		}
		if Source_control_command == TRADITIONAL {
			G_string_append(rs.change_replication_source, traditional_change_source.Str.String())
		} else {
			G_string_append(rs.change_replication_source, aws_change_source.Str.String())
		}
	}

	if _exec_start_replica {
		if rs.start_replica == nil {
			rs.start_replica = G_string_new("")
		}
		G_string_append(rs.start_replica, Start_replica)
		G_string_append(rs.start_replica, ";\n")
	}

	if Source_control_command == TRADITIONAL {
		if channel_name != "" {
			log.Infof("Change master will be executed for channel: %s", channel_name)
		} else {
			log.Infof("Change master will be executed for channel: %s", "default channel")
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
		d.real_database = d.name
	}
	d.filename = filename
	d.mutex = G_mutex_new()
	d.sequence_queue = G_async_queue_new()
	d.queue = G_async_queue_new()
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
	if cd.current_database != nil {
		var query = fmt.Sprintf("USE `%s`", cd.current_database.real_database)
		if M_query_warning(cd.thrconn, query, "Thread %d: Error switching to database `%s`", cd.thread_id, cd.current_database.real_database) {
			return true
		}

	} else {
		log.Warnf("Thread %d with connection %d: Not able to switch database", cd.thread_id, cd.connection_id)
	}
	return false
}

func execute_use_if_needs_to(cd *connection_data, database *database, msg string) {
	if database != nil && (DB == "" || cd.current_database == nil) {
		if cd.current_database == nil || strings.Compare(database.real_database, cd.current_database.real_database) != 0 {
			cd.current_database = database
			if !execute_use(cd) {
				log.Criticalf("Thread %d with connection %d: Error switching to database `%s` %s: %s", cd.thread_id, cd.connection_id, cd.current_database.real_database, msg, Mysql_error(cd.thrconn))
			}
		}
	}
	return
}

func get_file_type(filename string) file_type {
	if (strings.Compare(filename, "metadata") == 0 || strings.Contains(filename, "metadata.header") ||
		strings.Contains(filename, "metadata.partial")) && !(strings.HasSuffix(filename, ".sql") ||
		has_exec_per_thread_extension(filename)) {
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
		log.Critical("resume.partial file found. Remove it and restart process if you consider that it will be safe.")
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
	*table = split[1]
	*database = split[0]
	return
}

func process_create_table_statement(statement *GString, create_table_statement *GString, alter_table_statement *GString, alter_table_constraint_statement *GString, dbt *db_table, split_indexes bool) int {
	return Global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.real_table, split_indexes)
}

func compare_dbt(a *db_table, b *db_table, table_hash map[string]*db_table) bool {
	var a_key = Build_dbt_key(a.database.real_database, a.table)
	var b_key = Build_dbt_key(b.database.real_database, b.table)
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
			if SkipTableSorting || len(table_list) > max_number_tables_to_sort_in_table_list {
				table_list = append(table_list, dbt)
			} else {
				table_list = append(table_list, dbt)
				sort.Slice(table_list, func(i, j int) bool {
					return table_list[i].rows < table_list[j].rows
				})
			}

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
	var checksum string
	checksum = fun(conn, dbt.database.real_database, dbt.real_table)
	return checksum_template(dbt_checksum, checksum,
		"%s mismatch found for %s.%s: got %s, expecting %s",
		"%s confirmed for %s.%s", message, dbt.database.real_database, dbt.real_table)
}

func checksum_database_template(_db, dbt_checksum string, conn *DBConnection, message string, fun check_sum) bool {
	var checksum string
	checksum = fun(conn, _db, "")
	return checksum_template(dbt_checksum, checksum,
		"%s mismatch found for %s: got %s, expecting %s",
		"%s confirmed for %s", message, _db, "")
}

func checksum_dbt(dbt *db_table, conn *DBConnection) bool {
	var checksum_ok = true
	if checksum_mode != CHECKSUM_SKIP {
		if !NoSchemas {
			if dbt.schema_checksum != "" {
				if dbt.is_view {
					// TODO checksum_ok&=checksum_dbt_template
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

func execute_file_per_thread(sql_fn string, sql_fn3 string, exec []string) (*osFile, error) {
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
	switch {
	case slices.Contains(exec, GZIP_EXTENSION):
		var out *gzip.Reader
		out, err = gzip.NewReader(sql_file)

		outfile.close = out.Close
		outfile.writerTo = out.WriteTo
		outfile.metadata = out.Name
		outfile.read = out.Read

	case slices.Contains(exec, ZSTD_EXTENSION):
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

func get_command_and_basename(filename string, command *[]string, basename *string) bool {
	var length int
	if has_exec_per_thread_extension(filename) {
		*command = exec_per_thread_cmd
		length = len(ExecPerThreadExtension)
	} else if strings.HasSuffix(filename, ZSTD_EXTENSION) {
		*command = zstd_decompress_cmd
		length = len(ZSTD_EXTENSION)
	} else if strings.HasSuffix(filename, GZIP_EXTENSION) {
		*command = gzip_decompress_cmd
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

func show_warnings_if_possible(conn *DBConnection) string {
	if !ShowWarnings {
		return ""
	}
	var result *MYSQL_RES = M_store_result(conn, "SHOW WARNINGS", M_critical, "Error on SHOW WARNINGS")
	if result == nil {
		return ""
	}
	var _error *GString = G_string_new("")
	for row := Mysql_fetch_row(result); row != nil; row = Mysql_fetch_row(result) {
		G_string_append(_error, string(row[2].AsString()))
		G_string_append(_error, "\n")
	}
	return _error.Str.String()
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
