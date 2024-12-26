package myloader

import (
	"container/list"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	IS_INNODB_TABLE        = 2
	INCLUDE_CONSTRAINT     = 4
	IS_ALTER_TABLE_PRESENT = 8
)

func (o *OptionEntries) initialize_common() {
	if o.IgnoreSet != "" {
		var ignore_set_items = strings.Split(o.IgnoreSet, ",")
		var i = 0
		for i = 0; i < len(ignore_set_items); i++ {
			o.global.ignore_set_list.PushFront(ignore_set_items[i])
		}
	}
	o.global.refresh_table_list_counter = o.RefreshTableListInterval
	o.global.db_hash_mutex = g_mutex_new()
	o.global.tbl_hash = make(map[string]string)
	o.global.db_hash = make(map[string]*database)
	if o.DB != "" {
		o.global.database_db = o.get_db_hash(o.DB, o.DB)
	}
}

func is_in_list(haystack string, list *list.List) bool {
	for e := list.Front(); e != nil; e = e.Next() {
		if strings.Compare(e.Value.(string), haystack) == 0 {
			return true
		}
	}
	return false

}

func (o *OptionEntries) is_in_ignore_set_list(haystack string) bool {
	return is_in_list(haystack, o.global.ignore_set_list)
}

func get_value(kf *ini.File, group string, key string) string {
	section := kf.Section(group)
	if !section.HasKey(key) {
		return ""
	}
	value, _ := section.GetKey(key)
	return value.Value()
}

func (o *OptionEntries) change_master(kf *ini.File, group string, output_statement *string) {
	var val string
	var i uint
	var length int
	var traditional_change_source string
	var aws_change_source string
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
			if g_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				exec_change_source = 1
			}
		} else if strings.EqualFold(keys[i].Name(), "myloader_exec_start_slave") && strings.EqualFold(keys[i].Name(), "myloader_exec_start_replica") {
			if g_key_file_get_value(kf, group, keys[i].Name()) == "1" {
				exec_start_replica = 1
			}
		} else if strings.EqualFold(keys[i].Name(), "executed_gtid_set") {
			o.global.source_gtid = g_key_file_get_value(kf, group, keys[i].Name())
		} else if strings.EqualFold(keys[i].Name(), "channel_name") {
			channel_name = g_key_file_get_value(kf, group, keys[i].Name())
		} else {
			if first {
				first = false
			} else {
				traditional_change_source += ","
			}
			if strings.EqualFold(keys[i].Name(), "SOURCE_AUTO_POSITION") {
				auto_position = g_ascii_strtoull(keys[i].Value()) > 0
				traditional_change_source += fmt.Sprintf("%s = %d", keys[i].Name(), boolToInt(auto_position))
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_SSL") {
				source_ssl = g_ascii_strtoull(g_key_file_get_value(kf, group, keys[i].Name())) > 0
				traditional_change_source += fmt.Sprintf("%s = %d", keys[i].Name(), boolToInt(source_ssl))
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_HOST") {
				source_host = g_key_file_get_value(kf, group, keys[i].Name())
				traditional_change_source += fmt.Sprintf("%s = %s", keys[i].Name(), source_host)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_PORT") {
				source_port = uint(g_ascii_strtoull(g_key_file_get_value(kf, group, keys[i].Name())))
				traditional_change_source += fmt.Sprintf("%s = %d", keys[i], source_port)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_USER") {
				source_user = g_key_file_get_value(kf, group, keys[i].Name())
				traditional_change_source += fmt.Sprintf("%s = %s", keys[i].Name(), source_user)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_PASSWORD") {
				source_password = g_key_file_get_value(kf, group, keys[i].Name())
				traditional_change_source += fmt.Sprintf("%s = %s", keys[i].Name(), source_password)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_LOG_FILE") {
				source_log_file = g_key_file_get_value(kf, group, keys[i].Name())
				traditional_change_source += fmt.Sprintf("%s = %s", keys[i].Name(), source_log_file)
			} else if strings.EqualFold(keys[i].Name(), "SOURCE_LOG_POS") {
				source_log_pos, _ = strconv.ParseUint(g_key_file_get_value(kf, group, keys[i].Name()), 10, 64)
				traditional_change_source += fmt.Sprintf("%s = %d", keys[i], source_log_pos)
			} else {
				val = g_key_file_get_value(kf, group, keys[i].Name())
				if val != "" {
					traditional_change_source += fmt.Sprintf("%s = %s", keys[i].Name(), val)
				}
			}
		}
	}
	if auto_position {
		aws_change_source += "CALL mysql.rds_set_external_master_with_auto_position"
	} else {
		aws_change_source += "CALL mysql.rds_set_external_master"
	}
	if !auto_position {
		aws_change_source += fmt.Sprintf("%s, %d", source_log_file, source_log_pos)
	}
	aws_change_source += fmt.Sprintf("%d );\n", boolToInt(source_ssl))
	traditional_change_source += " "
	traditional_change_source += "FOR CHANNEL "
	if channel_name == "" {
		traditional_change_source += "''"
	} else {
		traditional_change_source += channel_name
	}
	traditional_change_source += ";\n"

	if o.SetGtidPurge {
		if o.global.source_control_command == TRADITIONAL {
			*output_statement += fmt.Sprintf("RESET MASTER;\nSET GLOBAL gtid_purged=%s;\n", o.global.source_gtid)
		}
		if o.global.source_control_command == AWS {
			*output_statement += fmt.Sprintf("CALL mysql.rds_set_gtid_purged (%s);\n", o.global.source_gtid)
		}
	}
	if intToBool(exec_change_source) {
		if intToBool(exec_reset_replica) {
			*output_statement += o.global.stop_replica
			*output_statement += ";\n"
			*output_statement += o.global.reset_replica
			if o.global.source_control_command == TRADITIONAL {
				*output_statement += " "
				if exec_reset_replica > 1 {
					*output_statement += "ALL "
				}
				if channel_name != "" {
					*output_statement += fmt.Sprintf("FOR CHANNEL %s ", channel_name)
				}
			}
			*output_statement += ";\n"
		}
		if o.global.source_control_command == TRADITIONAL {
			*output_statement += traditional_change_source
		} else {
			*output_statement += aws_change_source
		}

		if intToBool(exec_start_replica) {
			*output_statement += o.global.start_replica
			*output_statement += ";\n"
		}
		if o.global.source_control_command == TRADITIONAL {
			if channel_name != "" {
				log.Infof("Change master will be executed for channel: %s", channel_name)
			} else {
				log.Infof("Change master will be executed for channel: default channel")
			}
		}

	}
}

func (o *OptionEntries) m_filename_has_suffix(str string, suffix string) bool {
	if o.has_exec_per_thread_extension(str) {
		return strings.ToLower(path.Ext(str)) == o.ExecPerThreadExtension
	} else if strings.HasSuffix(str, GZIP_EXTENSION) {
		return strings.HasSuffix(path.Ext(str[:len(str)-len(GZIP_EXTENSION)]), suffix)
	} else if strings.HasSuffix(str, ZSTD_EXTENSION) {
		strings.HasSuffix(path.Ext(str[:len(str)-len(ZSTD_EXTENSION)]), suffix)
	}
	return strings.HasSuffix(str, suffix)
}

func (o *OptionEntries) new_database(db_name string, filename string) *database {
	var d = new(database)
	d.name = db_name
	if o.DB != "" {
		d.real_database = o.DB
	} else {
		d.real_database = db_name
	}
	d.filename = filename
	d.mutex = g_mutex_new()
	d.queue = g_async_queue_new(o.BufferSize)
	d.schema_state = NOT_FOUND
	d.schema_checksum = ""
	d.post_checksum = ""
	d.triggers_checksum = ""
	return d
}

func (o *OptionEntries) get_db_hash(filename, name string) *database {
	o.global.db_hash_mutex.Lock()
	d, _ := o.global.db_hash[filename]
	if d == nil {
		d = o.new_database(name, filename)
		o.global.db_hash[filename] = d
		if filename != name {
			o.global.db_hash[name] = d
		}
		d = o.global.db_hash[name]
	} else {
		if filename != name {
			d.name = name
			if o.DB != "" {
				d.real_database = o.DB
			} else {
				d.real_database = d.name
			}
		}
	}
	o.global.db_hash_mutex.Unlock()
	return d
}

func (o *OptionEntries) eval_table(db_name string, table_name string, mutex *sync.Mutex) bool {
	if table_name == "" {
		log.Errorf("Table name is null on eval_table()")
	}
	mutex.Lock()
	if len(o.global.tables) > 0 {
		if !is_table_in_list(db_name, table_name, o.global.tables) {
			mutex.Unlock()
			return false
		}
	}
	if o.TablesSkiplistFile != "" && check_skiplist(o, db_name, table_name) {
		mutex.Unlock()
		return false
	}
	mutex.Unlock()
	return eval_regex(o, db_name, table_name)
}

func execute_use(cd *connection_data) bool {
	var query = fmt.Sprintf("USE `%s`", cd.current_database.real_database)
	_, cd.thrconn.err = cd.thrconn.conn.Execute(query)
	if cd.thrconn.err != nil {
		return true
	}
	return false
}

func (o *OptionEntries) execute_use_if_needs_to(cd *connection_data, database *database, msg string) {
	if database != nil && (o.DB == "" || cd.current_database == nil) {
		if cd.current_database == nil || strings.Compare(database.real_database, cd.current_database.real_database) != 0 {
			cd.current_database = database
			if execute_use(cd) {
				log.Fatalf(fmt.Sprintf("Thread %d: Error switching to database `%s` %s: %v", cd.thread_id, cd.current_database.real_database, msg, cd.thrconn.err))
			}
		}
	}
	return
}

func (o *OptionEntries) get_file_type(filename string) file_type {
	if strings.Compare(filename, "metadata") == 0 || strings.Contains(filename, "metadata.header") || (strings.Contains(filename, "metadata.partial") && strings.HasSuffix(filename, ".sql")) {
		return METADATA_GLOBAL
	}
	if o.SourceDb != "" && !(strings.HasPrefix(filename, o.SourceDb) && len(filename) > len(o.SourceDb) && (strings.Contains(filename[:len(o.SourceDb)], ".")) ||
		strings.Contains(filename[:len(o.SourceDb)], "-")) && !strings.HasPrefix(filename, "mydumper_") {
		return IGNORED
	}
	if o.m_filename_has_suffix(filename, "-schema.sql") {
		return SCHEMA_TABLE
	}
	if strings.Compare(filename, "all-schema-create-tablespace.sql") == 0 {
		return SCHEMA_TABLESPACE
	}
	if strings.Compare(filename, "resume") == 0 {
		if !o.Resume {
			log.Fatalf("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.")
		}
		return RESUME
	}
	if strings.Compare(filename, "resume.partial") == 0 {
		log.Fatalf("\"resume.partial file found. Remove it and restart process if you consider that it will be safe.")
	}

	if o.m_filename_has_suffix(filename, "-checksum") {
		return CHECKSUM
	}

	if o.m_filename_has_suffix(filename, "-schema-view.sql") {
		return SCHEMA_VIEW
	}

	if o.m_filename_has_suffix(filename, "-schema-sequence.sql") {
		return SCHEMA_SEQUENCE
	}

	if o.m_filename_has_suffix(filename, "-schema-triggers.sql") {
		return SCHEMA_TRIGGER
	}

	if o.m_filename_has_suffix(filename, "-schema-post.sql") {
		return SCHEMA_POST
	}

	if o.m_filename_has_suffix(filename, "-schema-create.sql") {
		return SCHEMA_CREATE
	}

	if o.m_filename_has_suffix(filename, ".sql") {
		return DATA
	}

	if o.m_filename_has_suffix(filename, ".dat") {
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
	return global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt.real_table, split_indexes)
}

func (o *OptionEntries) build_dbt_key(a, b string) string {
	return fmt.Sprintf("%s%s%s.%s%s%s", o.global.identifier_quote_character, a, o.global.identifier_quote_character, o.global.identifier_quote_character, b, o.global.identifier_quote_character)
}

func (o *OptionEntries) compare_dbt(a *db_table, b *db_table, table_hash map[string]*db_table) bool {
	var a_key = o.build_dbt_key(a.database.real_database, a.table)
	var b_key = o.build_dbt_key(b.database.real_database, b.table)
	a_val, _ := table_hash[a_key]
	b_val, _ := table_hash[b_key]
	return a_val.rows < b_val.rows
}

func compare_dbt_short(a *db_table, b *db_table) bool {
	return a.rows < b.rows
}

func (o *OptionEntries) refresh_table_list_without_table_hash_lock(conf *configuration, force bool) {
	if force || g_atomic_int_dec_and_test(&o.global.refresh_table_list_counter) {
		var table_list []*db_table
		conf.table_list_mutex.Lock()
		var dbt *db_table
		for _, dbt = range conf.table_hash {
			table_list = append(table_list, dbt)
		}
		conf.table_list = table_list
		atomic.AddInt64(&o.global.refresh_table_list_counter, o.RefreshTableListInterval)
		conf.table_list_mutex.Unlock()
	}
}

func (o *OptionEntries) refresh_table_list(conf *configuration) {
	conf.table_hash_mutex.Lock()
	o.refresh_table_list_without_table_hash_lock(conf, true)
	conf.table_hash_mutex.Unlock()
}

func (o *OptionEntries) checksum_template(dbt_checksum, checksum, err_templ, info_templ, message, _db, _table string) bool {
	if o.global.checksum_mode != CHECKSUM_SKIP {
		if dbt_checksum != checksum {
			if _table != "" {
				if o.global.checksum_mode == CHECKSUM_WARN {
					log.Warnf(err_templ, message, _db, _table, checksum, dbt_checksum)
				} else {
					log.Errorf(err_templ, message, _db, _table, checksum, dbt_checksum)
				}
			} else {
				if o.global.checksum_mode == CHECKSUM_WARN {
					log.Warnf(err_templ, message, _db, checksum, dbt_checksum)
				} else {
					log.Errorf(err_templ, message, _db, checksum, dbt_checksum)
				}
			}
			return false
		} else {
			log.Infof(info_templ, message, _db, _table)
		}
	} else {
		log.Fatalf("checksum_mode is CHECKSUM_SKIP")
	}
	return true
}

func (o *OptionEntries) checksum_dbt_template(dbt *db_table, dbt_checksum string, conn *db_connection, message string, fun check_sum) bool {
	var _db = dbt.database.real_database
	var _table = dbt.real_table
	var err error
	var checksum string
	checksum, err = fun(conn, _db, _table)
	if err != nil {
		log.Warnf("Error getting checksum for %s.%s: %v", _db, _table, err)
	}
	return o.checksum_template(dbt_checksum, checksum, "%s mismatch found for %s.%s: got %s, expecting %s", "%s confirmed for %s.%s", message, _db, _table)
}

func (o *OptionEntries) checksum_database_template(_db, dbt_checksum string, conn *db_connection, message string, fun check_sum) bool {
	var err error
	var checksum string
	checksum, err = fun(conn, _db, "")
	if err != nil {
		log.Warnf("Error getting checksum for %s: %v", _db, err)
	}
	return o.checksum_template(dbt_checksum, checksum, "%s mismatch found for %s: got %s, expecting %s", "%s confirmed for %s", message, _db, "")
}

func (o *OptionEntries) checksum_dbt(dbt *db_table, conn *db_connection) bool {
	var checksum_ok = true
	if o.global.checksum_mode != CHECKSUM_SKIP {
		if !o.NoSchemas {
			if dbt.schema_checksum != "" {
				if dbt.is_view {
					checksum_ok = o.checksum_dbt_template(dbt, dbt.schema_checksum, conn, "View checksum", checksum_view_structure)
				} else {
					checksum_ok = o.checksum_dbt_template(dbt, dbt.schema_checksum, conn, "Structure checksum", checksum_table_structure)
				}
			}
			if dbt.indexes_checksum != "" {
				checksum_ok = o.checksum_dbt_template(dbt, dbt.indexes_checksum, conn, "Schema index checksum", checksum_table_indexes)
			}
		}
		if dbt.triggers_checksum != "" && !o.SkipTriggers {
			checksum_ok = o.checksum_dbt_template(dbt, dbt.triggers_checksum, conn, "Trigger checksum", checksum_trigger_structure)
		}
		if dbt.data_checksum != "" && !o.NoData {
			checksum_ok = o.checksum_dbt_template(dbt, dbt.data_checksum, conn, "Data checksum", checksum_table)
		}
	}
	return checksum_ok
}

func (o *OptionEntries) has_exec_per_thread_extension(filename string) bool {
	return o.ExecPerThreadExtension != "" && strings.HasSuffix(filename, o.ExecPerThreadExtension)
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

func (o *OptionEntries) get_command_and_basename(filename string, basename *string) bool {
	var length int
	if o.has_exec_per_thread_extension(filename) {
		length = len(o.ExecPerThreadExtension)
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

func m_query(conn *db_connection, query string, log_fun func(string, ...any), args string) bool {
	_ = conn.Execute(query)

	if conn.err != nil {
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

func change_master(kf *ini.File, group string, output_statement string) {

}
