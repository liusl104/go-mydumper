package mydumper

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-ini/ini"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	VERSION                   = GitBranch
	Log_output                *os.File
	Json                      bool
	Logger                    *os.File
	IgnoreErrorsList          []uint16
	Help                      bool
	show_warnings             bool
	No_delete                 bool
	stream                    bool
	BufferSize                uint
	CheckRowCount             bool
	IgnoreErrors              string
	Stream_queue              *GAsyncQueue
	SetNamesInConnForSct      string
	SetNamesInFileForSct      string
	SetNamesInFileByDefault   string
	Throttle_time             int = 0
	Throttle_max_usleep_limit int = 60000000
)

const (
	MYLOADER_MODE                   = "myloader_mode"
	DEFAULTS_FILE                   = "/etc/mydumper.cnf"
	DB_LIBRARY                      = "MySQL"
	MYSQL_VERSION_STR               = "8.0.31"
	EXIT_FAILURE                    = 1
	EXIT_SUCCESS                    = 0
	WIDTH                           = 40
	MIN_THREAD_COUNT                = 2
	IS_INNODB_TABLE                 = 2
	IS_TRX_TABLE                    = 2
	INCLUDE_CONSTRAINT              = 4
	IS_ALTER_TABLE_PRESENT          = 8
	START_SLAVE                     = "START SLAVE"
	START_SLAVE_SQL_THREAD          = "START SLAVE SQL_THREAD"
	CALL_START_REPLICATION          = "CALL mysql.rds_start_replication();"
	STOP_SLAVE_SQL_THREAD           = "STOP SLAVE SQL_THREAD"
	STOP_SLAVE                      = "STOP SLAVE"
	CALL_STOP_REPLICATION           = "CALL mysql.rds_stop_replication();"
	RESET_SLAVE                     = "RESET SLAVE"
	CALL_RESET_EXTERNAL_MASTER      = "CALL mysql.rds_reset_external_master()"
	SHOW_SLAVE_STATUS               = "SHOW SLAVE STATUS"
	SHOW_ALL_SLAVES_STATUS          = "SHOW ALL SLAVES STATUS"
	START_REPLICA                   = "START REPLICA"
	START_REPLICA_SQL_THREAD        = "START REPLICA SQL_THREAD"
	STOP_REPLICA                    = "STOP REPLICA"
	STOP_REPLICA_SQL_THREAD         = "STOP REPLICA SQL_THREAD"
	RESET_REPLICA                   = "RESET REPLICA"
	SHOW_REPLICA_STATUS             = "SHOW REPLICA STATUS"
	SHOW_ALL_REPLICAS_STATUS        = "SHOW ALL REPLICAS STATUS"
	SHOW_MASTER_STATUS              = "SHOW MASTER STATUS"
	SHOW_BINLOG_STATUS              = "SHOW BINLOG STATUS"
	SHOW_BINARY_LOG_STATUS          = "SHOW BINARY LOG STATUS"
	CHANGE_MASTER                   = "CHANGE MASTER"
	CHANGE_REPLICATION_SOURCE       = "CHANGE REPLICATION SOURCE"
	FLUSH_TABLES_WITH_READ_LOCK     = "FLUSH TABLES WITH READ LOCK"
	FLUSH_NO_WRITE_TO_BINLOG_TABLES = "FLUSH NO_WRITE_TO_BINLOG TABLES"
	ZSTD_EXTENSION                  = ".zst"
	GZIP_EXTENSION                  = ".gz"
	GZIP                            = "gzip"
	ZSTD                            = "zstd"
	BZIP2_EXTENSION                 = ".bz2"
	LZ4_EXTENSION                   = ".lz4"
	EMPTY_STRING                    = ""
	CAST                            = "CAST("
	AS_BINARY                       = "AS BINARY)"
	BINARY_CHARSET                  = "binary"
	AUTO_CHARSET                    = "auto"
)

type Function_pointer struct {
	Fun_ptr         func(str string) FieldValue
	Is_pre          bool
	Value           string
	Parse           []string
	Delimiters      []string
	Memory          map[string]string
	Replace_null    bool
	Max_length      int
	Null_max_length int
	Unique_list     []string
	Unique          bool
}
type file_write struct {
	write  write_fun
	close  close_fun
	flush  flush_fun
	status int
}

type Object_to_export struct {
	No_data    bool
	No_schema  bool
	No_trigger bool
}
type Configuration_per_table struct {
	All_anonymized_function         map[string]map[string]*Function_pointer
	All_where_per_table             map[string]string
	All_limit_per_table             map[string]string
	All_num_threads_per_table       map[string]uint
	All_columns_on_select_per_table map[string]string
	All_columns_on_insert_per_table map[string]string
	All_object_to_export            map[string]string
	All_partition_regex_per_table   map[string]*regexp.Regexp
	All_rows_per_table              map[string]string
}
type write_fun func(p []byte) (int, error)
type close_fun func() error
type flush_fun func() error

// Initialize_share_common performs one-time shared common initialization. Currently a no-op.
func Initialize_share_common() {
}

// get_zstd_cmd initializes or returns the zstd compression command. Currently a no-op.
func get_zstd_cmd() {
}

// get_gzip_cmd initializes or returns the gzip compression command. Currently a no-op.
func get_gzip_cmd() {
}

// Initialize_hash_of_session_variables returns a map of session variable names to values (e.g. WAIT_TIMEOUT for MySQL-like).
func Initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = make(map[string]string)
	if Is_mysql_like() {
		set_session_hash["WAIT_TIMEOUT"] = "2147483"
		set_session_hash["NET_WRITE_TIMEOUT"] = "2147483"
	}
	return set_session_hash
}

// Initialize_set_names sets default charset variables (Set_names_in_conn_by_default, SetNamesInFileByDefault, etc.) if not already set.
func Initialize_set_names() {
	if Set_names_in_conn_by_default == "" {
		Set_names_in_conn_by_default = BINARY_CHARSET
	}
	if SetNamesInConnForSct == "" {
		SetNamesInConnForSct = AUTO_CHARSET
	}
	if SetNamesInFileByDefault == "" {
		SetNamesInFileByDefault = BINARY_CHARSET
	}
	if SetNamesInFileForSct == "" {
		SetNamesInFileForSct = SetNamesInFileByDefault
	}

}

// set_names_statement_template returns the SQL fragment for SET NAMES with the given charset.
func set_names_statement_template(_set_names string) string {
	return fmt.Sprintf("/*!40101 SET NAMES %s*/", _set_names)
}

// Free_set_names clears SetNamesStr and Set_names_statement.
func Free_set_names() {
	SetNamesStr = ""
	Set_names_statement = ""
}

// generic_checksum runs a parameterized query (database/table) and returns the checksum string from the given column index.
func generic_checksum(conn *DBConnection, database, table, query_template string, column_number int) string {
	var query string
	if table == "" {
		query = fmt.Sprintf(query_template, database)
	} else {
		query = fmt.Sprintf(query_template, database, table)
	}
	conn.Query = query
	var mr *M_ROW = M_store_result_single_row(conn, query, "Error dumping checksum (%s.%s)", database, table)
	var r string
	/* There should never be more than one Row */
	if mr.Row != nil {
		switch mr.Row[column_number].Value().(type) {
		case []byte:
			r = fmt.Sprintf("%s", mr.Row[column_number].AsString())
		case int64:
			r = fmt.Sprintf("%d", mr.Row[column_number].AsInt64())
		case uint64:
			r = fmt.Sprintf("%d", mr.Row[column_number].AsUint64())
		default:
			r = fmt.Sprintf("%v", mr.Row[column_number].Value())
		}

	}
	M_store_result_row_free(mr)
	return r
}

// Checksum_table returns the CHECKSUM TABLE result for the given database.table.
func Checksum_table(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "CHECKSUM TABLE `%s`.`%s`", 1)
}

// Checksum_table_structure returns a CRC32-based checksum of the table column definitions from information_schema.
func Checksum_table_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0)
}

// Checksum_process_structure returns a checksum of routine definitions in the schema.
func Checksum_process_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0)
}

// Checksum_trigger_structure returns a checksum of trigger action statements for the given table.
func Checksum_trigger_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';", 0)
}

// Checksum_trigger_structure_from_database returns a checksum of all trigger actions in the schema.
func Checksum_trigger_structure_from_database(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';", 0)
}

// Checksum_view_structure returns a checksum of the view definition for the given database.table.
func Checksum_view_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", 0)
}

// Checksum_database_defaults returns a checksum of the schema default charset and collation.
func Checksum_database_defaults(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;", 0)
}

// Checksum_table_indexes returns a checksum of index/column metadata from information_schema.STATISTICS.
func Checksum_table_indexes(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0)
}

// Load_config_file loads and returns an ini.File from the given path; returns nil on error.
func Load_config_file(config_file string) *ini.File {
	kf, err := ini.Load(config_file)
	if err != nil {
		log.Warnf("Failed to load config file %s: %v", config_file, err)
		return nil
	}
	return kf
}

// parse_key_file_group reads host, user, password from the ini group and sets connection globals.
func parse_key_file_group(kf *ini.File, group string) {
	// var keys []string
	// keys = kf.GetKeyList(group)
	section := kf.Section(group)
	if section == nil {
		log.Errorf("Loading configuration on section %s is null", group)
		return
	}
	keys := section.Keys()
	for _, key := range keys {
		if strings.EqualFold(key.Name(), "host") {
			Hostname = key.Value()
		} else if strings.EqualFold(key.Name(), "user") {
			Username = key.Value()
		} else if strings.EqualFold(key.Name(), "password") {
			Password = key.Value()
		}
	}
	log.Infof("Config file loaded")
}

// load_hash_from_key_file fills set_session_hash with key-value pairs from the given ini group.
func load_hash_from_key_file(kf *ini.File, set_session_hash map[string]string, group_variables string) {
	var keys []*ini.Key
	var value string
	keys = kf.Section(group_variables).Keys()
	if set_session_hash == nil {
		log.Fatalf("set_session_hash is nil")
	}
	for i := 0; i < len(keys); i++ {
		value = G_key_file_get_value(kf, group_variables, keys[i].Name())
		set_session_hash[keys[i].Name()] = value
	}

}

// Load_per_table_info_from_key_file loads per-table config from ini: where, limit, num_threads, columns, partition_regex, anonymized functions, etc.
func Load_per_table_info_from_key_file(kf *ini.File, cpt *Configuration_per_table, init_function_pointer func(str string) *Function_pointer) {
	if kf == nil {
		log.Errorf("assertion 'key_file != NULL' failed")
		return
	}
	var groups = kf.SectionStrings()
	var i int
	var keys []*ini.Key
	var ht map[string]*Function_pointer = make(map[string]*Function_pointer)
	var value string
	for i = 0; i < len(groups); i++ {
		if strings.Contains(groups[i], "`.`") && strings.HasPrefix(groups[i], "`") && strings.HasSuffix(groups[i], "`") {
			keys = kf.Section(groups[i]).Keys()
			for _, key := range keys {
				if strings.HasPrefix(key.Name(), "`") && strings.HasSuffix(key.Name(), "`") {
					if init_function_pointer != nil {
						value = G_key_file_get_value(kf, groups[i], key.Name())
						var fp *Function_pointer = init_function_pointer(value)
						ht[key.Name()] = fp
					}
				} else {
					if strings.Compare(key.Name(), "where") == 0 {
						cpt.All_where_per_table[groups[i]] = key.Value()
					}
					if strings.Compare(key.Name(), "limit") == 0 {
						cpt.All_limit_per_table[groups[i]] = key.Value()
					}
					if strings.Compare(key.Name(), "num_threads") == 0 {
						value = key.Value()
						n, _ := strconv.Atoi(value)
						cpt.All_num_threads_per_table[groups[i]] = uint(n)
					}
					if strings.Compare(key.Name(), "columns_on_select") == 0 {
						cpt.All_columns_on_select_per_table[groups[i]] = key.Value()
					}
					if strings.Compare(key.Name(), "columns_on_insert") == 0 {
						cpt.All_columns_on_insert_per_table[groups[i]] = key.Value()
					}
					if strings.Compare(key.Name(), "Object_to_export") == 0 {
						cpt.All_object_to_export[groups[i]] = key.Value()
					}
					if strings.Compare(key.Name(), "partition_regex") == 0 {
						var r *regexp.Regexp
						init_regex(&r, key.Value())
						cpt.All_partition_regex_per_table[groups[i]] = r
					}
					if strings.Compare(key.Name(), "Rows") == 0 {
						cpt.All_rows_per_table[groups[i]] = key.Value()
					}
				}

			}
			cpt.All_anonymized_function[groups[i]] = ht
		}
	}
}

// Load_hash_of_all_variables_perproduct_from_key_file loads session variables for product name and version (e.g. mysql_8_0_22).
func Load_hash_of_all_variables_perproduct_from_key_file(kf *ini.File, set_session_hash map[string]string, str string) {
	if set_session_hash == nil {
		log.Criticalf("set_session_hash is nil")
	}
	var s *GString = G_string_new(str)
	load_hash_from_key_file(kf, set_session_hash, s.Str.String())
	G_string_append(s, "_")
	G_string_append(s, Get_product_name())
	load_hash_from_key_file(kf, set_session_hash, s.Str.String())
	G_string_append_printf(s, "_%d", Get_major())
	load_hash_from_key_file(kf, set_session_hash, s.Str.String())
	G_string_append_printf(s, "_%d", Get_secondary())
	load_hash_from_key_file(kf, set_session_hash, s.Str.String())
	G_string_append_printf(s, "_%d", Get_revision())
	load_hash_from_key_file(kf, set_session_hash, s.Str.String())
}

// Free_hash_table clears all entries from the hash map.
func Free_hash_table(hash map[string]string) {
	for key, _ := range hash {
		delete(hash, key)
	}
}

// refresh_set_from_hash appends SET SESSION/GLOBAL key = value statements to ss from set_hash (handles /*! comments).
func refresh_set_from_hash(ss *GString, kind string, set_hash map[string]string) {
	if set_hash == nil {
		log.Fatalf("set_hash is nil")
	}
	for lkey, e := range set_hash {
		var idx = strings.Index(e, "/*!")
		if idx != -1 {
			var c string = e[idx+1:]
			e = e[:idx]
			G_string_append_printf(ss, "/%s SET %s %s = %s */;\n", c, kind, lkey, e)
		} else {
			G_string_append_printf(ss, "SET %s %s = %s ;\n", kind, lkey, e)
		}
	}
}

// set_session_hash_insert inserts or overwrites a key in the session hash.
func set_session_hash_insert(set_session_hash map[string]string, _key string, value string) {
	set_session_hash[_key] = value
}

// Refresh_set_session_from_hash builds SET SESSION statements from set_session_hash into ss (ensures FOREIGN_KEY_CHECKS=0).
func Refresh_set_session_from_hash(ss *GString, set_session_hash map[string]string) {
	G_string_set_size(ss, 0)
	if _, ok := set_session_hash["FOREIGN_KEY_CHECKS"]; !ok {
		set_session_hash["FOREIGN_KEY_CHECKS"] = "0"
	}
	refresh_set_from_hash(ss, "SESSION", set_session_hash)
}

// set_global_rollback_from_hash builds SELECT @@var INTO @var and SET GLOBAL var = @var for rollback, and appends to ss/sr.
func set_global_rollback_from_hash(ss *GString, sr *GString, set_hash map[string]string) {
	var stmp *GString
	if len(set_hash) > 0 {
		var i int
		stmp = G_string_new(" INTO")
		for lkey, _ := range set_hash {
			if i == 0 {
				G_string_append(ss, "SELECT ")
				G_string_append_printf(stmp, " @%s", lkey)
				G_string_append_printf(sr, "SET GLOBAL %s = @%s ;\n", lkey, lkey)
				G_string_append_printf(ss, " @@%s", lkey)
				i++
				continue
			}
			G_string_append_printf(stmp, ", @%s", lkey)
			G_string_append_printf(sr, "SET GLOBAL %s = @%s ;\n", lkey, lkey)
			G_string_append_printf(ss, ", @@%s", lkey)
			i++
		}
		G_string_append_printf(ss, "%s ;\n", stmp.Str.String())
	}
}

// Refresh_set_global_from_hash builds rollback and SET GLOBAL statements from set_global_hash into ss and sr.
func Refresh_set_global_from_hash(ss *GString, sr *GString, set_global_hash map[string]string) {
	set_global_rollback_from_hash(ss, sr, set_global_hash)
	refresh_set_from_hash(ss, "GLOBAL", set_global_hash)
}

// free_hash deletes all keys from the given map.
func free_hash(set_session_hash map[string]string) {
	for key, _ := range set_session_hash {
		delete(set_session_hash, key)
	}
}

// Execute_gstring splits ss by ";\n" and executes each non-empty statement on conn (e.g. SET session).
func Execute_gstring(conn *DBConnection, ss *GString) {
	if ss != nil {
		var line []string = strings.Split(ss.Str.String(), ";\n")
		var i int
		if conn.Rows != nil {
			_ = conn.Rows.Close()
		}
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 3 {
				_, conn.Err = conn.Conn.Exec(line[i])
				if conn.Err != nil {
					log.Warnf("Set session failed: %s", line[i])
				}
			}
		}
	}
}

// write_file writes the buffer to the file and returns bytes written and error.
func write_file(file *file_write, buff string) (int, error) {
	return file.write([]byte(buff))
}

// Replace_escaped_strings escapes special characters in b (delegates to Escape).
func Replace_escaped_strings(b string) string {
	return Escape(b)
}

// escape_tab_with copies bytes from to into itself, escaping tab as \t in place (buffer must be large enough).
func escape_tab_with(to []byte) {
	var from []byte = make([]byte, 0)
	copy(from, to)
	var i, j int
	for i, _ = range from {
		if from[i] == '\t' {
			to[j] = '\\'
			j++
			to[j] = 't'
		} else {
			to[j] = from[i]
		}
		j++
	}
	to[j] = from[i]
	from = nil
}

// Create_dir creates the given directory with mode 0750. Returns false if it already exists or on error.
func Create_dir(directory string) bool {
	if !Help {
		err := os.Mkdir(directory, 0750)
		if err != nil {
			// Consistent with C: if directory already exists (os.IsExist), do not error, just return false
			if !os.IsExist(err) {
				log.Criticalf("Unable to create `%s': %v", directory, err)
			}
			return false
		}
		return true
	}
	return true
}

// g_dir_make_tmp creates a temporary directory under os.TempDir with prefix mydumper_; fatals on error.
func g_dir_make_tmp() string {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "mydumper_")
	if err != nil {
		log.Fatalf("Unable to create `%s': %v", tmpDir, err)
	}
	return tmpDir
}

// create_fifo_dir creates the directory for FIFO files; fatals if path is invalid or mkdir fails.
func create_fifo_dir(new_fifo_directory string) {
	if new_fifo_directory == "" {
		log.Warnf("Fifo directoy provided was invalid")
		return
	}
	if err := os.MkdirAll(new_fifo_directory, 0660); err != nil {
		log.Fatalf("Unable to create `%s': %v", new_fifo_directory, err)
	}
}

// Build_tmp_dir_name returns the system temp directory path.
func Build_tmp_dir_name() string {
	return os.TempDir()
}

// Create_backup_dir creates the main backup directory and optionally the FIFO directory.
func Create_backup_dir(new_directory, new_fifo_directory string) {
	if Help {
		return
	}
	if err := os.MkdirAll(new_directory, 0750); err != nil {
		log.Criticalf("Unable to create `%s': %v", new_directory, err)
	}
	if new_fifo_directory != "" {
		create_fifo_dir(new_fifo_directory)
	}

}

// strcount returns the number of newline-separated lines in text.
func strcount(text string) int {
	count := 0
	t := text
	for {
		index := strings.Index(t, "\n")
		if index == -1 {
			if len(t) > 0 {
				count++
			}
			break
		}
		count++
		t = t[index+1:]
	}
	return count
}

// Remove_new_line removes all newline characters from the string.
func Remove_new_line(to string) string {
	return strings.ReplaceAll(to, "\n", "")
}

// m_remove0 removes the file at directory/filename and logs success or failure.
func m_remove0(directory string, filename string) {
	remove_path := filepath.Join(directory, filename)
	log.Infof("Removing file: %s ", remove_path)
	err := os.Remove(remove_path)
	if err != nil {
		log.Warnf("Remove failed: %s (%s)", remove_path, err.Error())
	}
}

// M_remove removes the file when Stream is set and No_delete is false; always returns true.
func M_remove(directory, filename string) bool {
	if Stream != "" && No_delete == false {
		m_remove0(directory, filename)
	}
	return true
}

// matchText returns true if a and b are equal ignoring case.
func matchText(a string, b string) bool {
	return strings.EqualFold(a, b)
}

// Is_table_in_list returns true if database.table is in tl (supports % and _ wildcards).
func Is_table_in_list(database string, table string, tl []string) bool {
	var table_name_lower = fmt.Sprintf("%s.%s", database, table)
	var tb_lower string
	var match bool
	for i := 0; i < len(tl); i++ {
		if !strings.Contains(tl[i], "%") && !strings.Contains(tl[i], "_") {
			if strings.EqualFold(tl[i], table_name_lower) {
				match = true
				break
			}
		} else {
			tb_lower = strings.ToLower(tl[i])
			if matchText(tb_lower, table_name_lower) {
				match = true
				break
			}
		}
	}
	return match
}

// Is_mysql_special_tables returns true if the table is mysql.general_log, slow_log, or innodb_*_stats.
func Is_mysql_special_tables(database string, table string) bool {
	return strings.Compare(database, "mysql") == 0 &&
		(strings.Compare(table, "general_log") == 0 ||
			strings.Compare(table, "slow_log") == 0 ||
			strings.Compare(table, "innodb_index_stats") == 0 ||
			strings.Compare(table, "innodb_table_stats") == 0)
}

// m_key_file_merge merges sections from ini file a into b (child sections as keys or new sections).
func m_key_file_merge(b *ini.File, a *ini.File) {
	var groups = a.SectionStrings()
	for _, group := range groups {
		children := a.ChildSections(group)
		for _, keys := range children {
			if b.HasSection(group) {
				_, _ = b.NewRawSection(keys.Name(), keys.Body())
			} else {
				newSection, _ := b.NewSection(group)
				_, _ = newSection.NewKey(keys.Name(), keys.Body())
			}
		}
	}
}

// Initialize_common_options loads defaults file, extra file, parses connection group and client, merges config, sets OptimizeKeyEngines.
func Initialize_common_options(group string) {
	if len(OptimizeKeyEngines) == 0 {
		OptimizeKeyEngines = []string{"InnoDB", "ROCKSDB"}
	}
	if DefaultsFile == "" {
		if G_file_test(DEFAULTS_FILE) {
			DefaultsFile = DEFAULTS_FILE
		}
	} else {
		if !G_file_test(DefaultsFile) {
			log.Criticalf("Default file %s not found", DefaultsFile)
		}
	}
	if DefaultsExtraFile != "" {
		if !G_file_test(DefaultsExtraFile) {
			log.Criticalf("Default extra file %s not found", DefaultsExtraFile)
		}
	} else {
		if DefaultsFile == "" {
			log.Infof("Using no configuration file")
			return
		}
	}
	if DefaultsFile == "" {
		DefaultsFile = DefaultsExtraFile
		DefaultsExtraFile = ""
	}
	var new_defaults_file string
	if !path.IsAbs(DefaultsFile) {
		new_defaults_file = path.Join(G_get_current_dir(), DefaultsFile)
		DefaultsFile = new_defaults_file
	}
	Key_file = Load_config_file(DefaultsFile)
	if Key_file != nil {
		if G_key_file_has_group(Key_file, group) {
			parse_key_file_group(Key_file, group)
			set_connection_defaults_file_and_group(DefaultsFile, group)
		}
		if G_key_file_has_group(Key_file, "client") {
			set_connection_defaults_file_and_group(DefaultsFile, "")
		}
	} else {
		set_connection_defaults_file_and_group(DefaultsFile, "")
	}
	if DefaultsExtraFile == "" {
		return
	}
	if !path.IsAbs(DefaultsExtraFile) {
		new_defaults_file = path.Join(G_get_current_dir(), DefaultsExtraFile)
		DefaultsExtraFile = new_defaults_file
	}
	var extra_key_file *ini.File = Load_config_file(DefaultsExtraFile)
	if extra_key_file != nil {
		if G_key_file_has_group(extra_key_file, group) {
			log.Infof("Parsing extra key file")
			parse_key_file_group(extra_key_file, group)
			set_connection_defaults_file_and_group(DefaultsExtraFile, group)
		}
		if G_key_file_has_group(extra_key_file, "client") {
			set_connection_defaults_file_and_group(DefaultsExtraFile, "")
		}
	} else {
		set_connection_defaults_file_and_group(DefaultsExtraFile, "")
	}
	log.Infof("Merging config files user: ")
	if SocketPath != "" {
		Protocol = "socket"
	}
	m_key_file_merge(Key_file, extra_key_file)
}

// Get_table_list splits tables_list by comma and validates each entry is in DATABASE.TABLE format; fatals on invalid.
func Get_table_list(tables_list string) []string {
	tl := strings.Split(tables_list, ",")
	for _, table := range tl {
		if !strings.Contains(table, ".") {
			log.Fatalf("Table name %s is not in DATABASE.TABLE format", table)
		}
	}
	return tl
}

// Remove_definer_from_gchar strips the DEFINER= clause from a SQL string (e.g. CREATE VIEW) and returns the result.
func Remove_definer_from_gchar(str string) string {
	definer := " DEFINER="
	// Find the " DEFINER=" substring
	indexDefiner := strings.Index(str, definer)
	if indexDefiner != -1 {
		// Find the first space after " DEFINER="
		substrFromDefiner := str[indexDefiner+len(definer):]
		indexSpace := strings.Index(substrFromDefiner, " ")
		if indexSpace != -1 {
			// Replace all chars between " DEFINER=" and next space with space
			before := str[:indexDefiner]
			after := substrFromDefiner[indexSpace:]
			// spaces := strings.Repeat(" ", indexSpace+len(definer))
			// return before + spaces + after
			return before + after
		} else {
			// If no space after " DEFINER=", remove to end
			before := str[:indexDefiner]
			return before
		}
	}
	// If " DEFINER=" not found, return original string
	return str
}

// Remove_definer overwrites data with the result of Remove_definer_from_gchar (strips DEFINER from CREATE VIEW etc.).
func Remove_definer(data *GString) {
	str := data.Str.String()
	data.Str.Reset()
	data.Str.WriteString(Remove_definer_from_gchar(str))
}

// Print_version prints the program version, Git hash, branch, build time, and Go version to stdout.
func Print_version(program string) {
	// Use fmt.Printf to print version info in the specified format
	fmt.Printf("%s %s, built against %s %s with SSL support\n", program, VERSION, DB_LIBRARY, MYSQL_VERSION_STR)

	// Print Git commit hash of the source
	fmt.Printf("Git Commit Hash: %s\n", GitHash)

	// Print Git branch name of the source
	fmt.Printf("Git Branch: %s\n", GitBranch)

	// Print program build time
	fmt.Printf("Build Time: %s\n", BuildTS)

	// Print Go version used to build the program
	fmt.Printf("Go Version: %s\n", GoVersion)
}

// Stream_arguments_callback parses Stream flag and sets stream, No_delete, No_stream; returns true if Stream was set.
func Stream_arguments_callback() bool {
	if Stream != "" {
		stream = true
		UseDefer = false
		if strings.EqualFold(Stream, "TRADITIONAL") {
			return true
		}
		if strings.EqualFold(Stream, "NO_DELETE") {
			No_delete = true
			return true
		}
		if strings.EqualFold(Stream, "NO_STREAM_AND_NO_DELETE") {
			No_delete = true
			No_stream = true
			return true
		}
		if strings.EqualFold(Stream, "NO_STREAM") {
			No_stream = true
			return true
		}
	}
	return false
}

// Check_num_threads ensures NumThreads is valid: if <= 0 sets it to CPU count; warns if below MIN_THREAD_COUNT.
func Check_num_threads() {
	// If configured thread count is <= 0, set it to the number of system processors
	if NumThreads <= 0 {
		NumThreads = g_get_num_processors()
	}
	// If thread count is less than minimum, log warning and set to minimum
	if NumThreads < MIN_THREAD_COUNT {
		log.Warnf("Invalid number of threads %d, setting to %d", NumThreads, MIN_THREAD_COUNT)
		// NumThreads = MIN_THREAD_COUNT
	}
}

// M_message logs msg with args at info level.
func M_message(msg string, args ...any) {
	log.Infof(msg, args...)
}

// M_error restores global session from Set_global_back and logs msg at error level.
func M_error(msg string, args ...any) {
	Execute_gstring(main_connection, Set_global_back)
	log.Errorf(msg, args...)
}

// M_critical restores global session, logs at critical level, and exits with EXIT_FAILURE.
func M_critical(msg string, args ...any) {
	Execute_gstring(main_connection, Set_global_back)
	log.Criticalf(msg, args...)
	os.Exit(EXIT_FAILURE)
}

// M_warning logs msg with args at warning level.
func M_warning(msg string, args ...any) {
	log.Warnf(msg, args...)
}

// Filter_sequence_schemas replaces quoted schema.table references in create_table with the first match from a regex.
func Filter_sequence_schemas(create_table string) string {
	re, err := regexp.Compile(fmt.Sprintf("%s\\w+%s\\.(%s\\w+%s)", Identifier_quote_character, Identifier_quote_character, Identifier_quote_character, Identifier_quote_character))
	if err != nil {
		log.Criticalf("filter table schema fail:%v", err)
	}
	fss := re.FindAllStringSubmatch(create_table, -1)
	return re.ReplaceAllString(create_table, fss[0][1])
}

// Read_data reads one line from infile into data, increments *line, sets *eof on scan error or EOF.
func Read_data(infile *bufio.Scanner, data *GString, eof *bool, line *int) bool {
	if !infile.Scan() {
		*eof = true
		return true
	}
	G_string_append_b(data, infile.Bytes())
	G_string_append_c(data, '\n')
	*line++
	if infile.Err() != nil {
		*eof = true
		return true
	}
	return true
}

// M_date_time_new_now_local returns current local time formatted as 2006-01-02 15:04:05.000000.
func M_date_time_new_now_local() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

// Double_quoute_protect doubles double-quote characters in r for escaping.
func Double_quoute_protect(r string) string {
	return strings.ReplaceAll(r, "\"", "\"\"")
}

// Backtick_protect doubles backtick characters in r for escaping.
func Backtick_protect(r string) string {
	return strings.ReplaceAll(r, "`", "``")
}

// Newline_protect replaces newlines with Unicode placeholder U+10000.
func Newline_protect(r string) string {
	return strings.ReplaceAll(r, "\n", "\u10000")
}

// newline_unprotect restores newlines from Unicode placeholder U+10000.
func newline_unprotect(r string) string {
	return strings.ReplaceAll(r, "\u10000", "\n")
}

// widthCompletion pads key with spaces so total length is width.
func widthCompletion(width int, key string) string {
	return key + strings.Repeat(" ", width-len(key))
}

// Print_int prints key=val to stdout with width-padded key.
func Print_int(key string, val int) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}

// Print_uint prints key=val to stdout with width-padded key.
func Print_uint(key string, val uint) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}

// Print_uint64 prints key=val to stdout with width-padded key.
func Print_uint64(key string, val uint64) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}

// Print_string prints key=val or commented key= when val is empty.
func Print_string(key string, val string) {
	if val != "" {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), val)
	} else {
		fmt.Printf("# %s=\n", widthCompletion(WIDTH-2, key))
	}

}

// Print_bool prints key= TRUE or # key= FALSE with width-padded key.
func Print_bool(key string, val bool) {
	if val {
		fmt.Printf("%s= TRUE\n", widthCompletion(WIDTH, key))
	} else {
		fmt.Printf("# %s= FALSE\n", widthCompletion(WIDTH-2, key))
	}
}

// Print_list prints key= comma-joined val or # key= "" when empty.
func Print_list(key string, val []string) {
	if len(val) != 0 {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), strings.Join(val, ","))
	} else {
		fmt.Printf("# %s= \"\"\n", widthCompletion(WIDTH-2, key))
	}
}

// append_alter_table appends "ALTER TABLE `table` " to the statement.
func append_alter_table(alter_table_statement *GString, table string) {
	G_string_append(alter_table_statement, "ALTER TABLE `")
	G_string_append(alter_table_statement, table)
	G_string_append(alter_table_statement, "` ")
}

// finish_alter_table removes a trailing comma if present and appends ";\n".
func finish_alter_table(alter_table_statement *GString) {

	lastCommaIndex := strings.LastIndex(alter_table_statement.Str.String(), ",")
	if lastCommaIndex > alter_table_statement.Len-5 {
		alter_table_statement = G_string_new(alter_table_statement.Str.String()[:lastCommaIndex])
		G_string_append(alter_table_statement, ";\n")
	} else {
		G_string_append(alter_table_statement, ";\n")
	}
}

// Global_process_create_table_statement splits CREATE TABLE into create_table, alter_table, alter_constraint; returns flags (IS_ALTER_TABLE_PRESENT, etc.).
func Global_process_create_table_statement(statement *GString, create_table_statement *GString, alter_table_statement *GString, alter_table_constraint_statement *GString, real_table string, split_indexes bool) int {
	var flag int
	var split_file = strings.Split(statement.Str.String(), "\n")
	var autoinc_column string
	append_alter_table(alter_table_statement, real_table)
	append_alter_table(alter_table_constraint_statement, real_table)
	var fulltext_counter int
	var i int
	for i = 0; i < len(split_file); i++ {
		if split_indexes && (strings.Contains(split_file[i], "  KEY") ||
			strings.Contains(split_file[i], "  UNIQUE") ||
			strings.Contains(split_file[i], "  SPATIAL") ||
			strings.Contains(split_file[i], "  FULLTEXT") ||
			strings.Contains(split_file[i], "  INDEX")) {
			if autoinc_column != "" && strings.Contains(split_file[i], autoinc_column) {
				G_string_append(create_table_statement, split_file[i])
				G_string_append(create_table_statement, "\n")
			} else {
				flag |= IS_ALTER_TABLE_PRESENT
				if strings.Contains(split_file[i], "  FULLTEXT") {
					fulltext_counter++
				}
				if fulltext_counter > 1 {
					fulltext_counter = 1
					finish_alter_table(alter_table_statement)
					append_alter_table(alter_table_statement, real_table)
				}
				G_string_append(alter_table_statement, "\n ADD")
				G_string_append(alter_table_statement, split_file[i])
			}
		} else {
			if strings.Contains(split_file[i], "  CONSTRAINT") {
				flag |= INCLUDE_CONSTRAINT
				G_string_append(alter_table_constraint_statement, "\n ADD")
				G_string_append(alter_table_constraint_statement, split_file[i])
			} else {
				if strings.Contains(split_file[i], "AUTO_INCREMENT") {
					var autoinc_split = strings.SplitN(split_file[i], "`", 3)
					if len(autoinc_split) < 2 {
						autoinc_split = append(autoinc_split, "null")
					}
					autoinc_column = fmt.Sprintf("(`%s`", autoinc_split[1])
				}
				G_string_append(create_table_statement, split_file[i])
				G_string_append(create_table_statement, "\n")
			}
		}
		if strings.Contains(split_file[i], "ENGINE=") {
			for j := 0; j < len(OptimizeKeyEngines); j++ {
				if strings.Contains(split_file[i], OptimizeKeyEngines[j]) {
					flag |= IS_TRX_TABLE
				}
			}
		}
	}
	G_string_replace(create_table_statement, ",\n)", "\n)")
	finish_alter_table(alter_table_statement)
	finish_alter_table(alter_table_constraint_statement)
	return flag
}

// Initialize_conf_per_table allocates and initializes all per-table config maps in cpt.
func Initialize_conf_per_table(cpt *Configuration_per_table) {
	cpt.All_anonymized_function = make(map[string]map[string]*Function_pointer)
	cpt.All_where_per_table = make(map[string]string)
	cpt.All_limit_per_table = make(map[string]string)
	cpt.All_num_threads_per_table = make(map[string]uint)
	cpt.All_columns_on_select_per_table = make(map[string]string)
	cpt.All_columns_on_insert_per_table = make(map[string]string)
	cpt.All_object_to_export = make(map[string]string)
	cpt.All_partition_regex_per_table = make(map[string]*regexp.Regexp)
	cpt.All_rows_per_table = make(map[string]string)
}

// str_list_has_str returns true if str is in str_list.
func str_list_has_str(str_list []string, str string) bool {
	return slices.Contains(str_list, str)
}

// Parse_object_to_export parses val (comma-separated DATA, SCHEMA, TRIGGER, ALL, NONE) and sets object_to_export flags.
func Parse_object_to_export(object_to_export *Object_to_export, val string) {
	if val == "" {
		object_to_export.No_data = false
		object_to_export.No_schema = false
		object_to_export.No_trigger = false
		return
	}
	var split_option []string = strings.SplitN(val, ",", 4)
	object_to_export.No_data = !str_list_has_str(split_option, "DATA")
	object_to_export.No_schema = !str_list_has_str(split_option, "SCHEMA")
	object_to_export.No_trigger = !str_list_has_str(split_option, "TRIGGER")
	if str_list_has_str(split_option, "ALL") {
		object_to_export.No_data = false
		object_to_export.No_schema = false
		object_to_export.No_trigger = false
	}
	if str_list_has_str(split_option, "NONE") {
		object_to_export.No_data = true
		object_to_export.No_schema = true
		object_to_export.No_trigger = true
	}
}

// Build_dbt_key returns a quoted "a.b" key using Identifier_quote_character (e.g. `db`.`table`).
func Build_dbt_key(a, b string) string {
	return fmt.Sprintf("%s%s%s.%s%s%s", Identifier_quote_character, a, Identifier_quote_character, Identifier_quote_character, b, Identifier_quote_character)
}

// Discard_mysql_output closes conn.Rows to release the result set so the connection can be reused for Exec/Query.
func Discard_mysql_output(conn *DBConnection) {
	if conn != nil && conn.Rows != nil {
		conn.Rows.Close()
		conn.Rows = nil
	}
}

// m_log logs msg (formatted with args) via log_fun_1 or log_fun_2 if the error is in IgnoreErrorsList.
func m_log(conn *DBConnection, log_fun_1 func(fmt string, a ...any), log_fun_2 func(fmt string, a ...any), msg string, args ...any) {
	if msg != "" && log_fun_1 != nil {
		var c = fmt.Sprintf(msg, args...)
		if log_fun_2 != nil && slices.Contains(IgnoreErrorsList, conn.Code) {
			log_fun_2("%s - ERROR %d: %v", c, Mysql_errno(conn), Mysql_error(conn))
		} else {
			if Mysql_errno(conn) != 0 {
				log_fun_1("%s - ERROR %d: %s", c, Mysql_errno(conn), Mysql_error(conn))
				Errors++
			} else {
				log_fun_1("%s", c)
			}
		}
	}
}

// m_queryv executes query on conn; on error logs via m_log and returns true. Returns false on success.
// Must discard conn.Rows after a successful MySQLQuery so the connection is not left with an unread result set (which would block subsequent Exec).
func m_queryv(conn *DBConnection, query string, log_fun_1 func(fmt string, a ...any), log_fun_2 func(fmt string, a ...any), msg string, args ...any) bool {
	conn.Query = query
	// res, err := conn.Conn.Query(query)
	if conn.MySQLQuery(query) {
		m_log(conn, log_fun_1, log_fun_2, msg, args...)
		return true
	}
	return false
}

// m_query executes query and logs on error; wrapper around m_queryv with single log function.
func m_query(conn *DBConnection, query string, log_fun func(fmt string, a ...any), msg string, args ...any) bool {
	conn.Query = query
	return m_queryv(conn, query, log_fun, nil, msg, args...)
}

// M_query_warning executes the query; on error logs warning (or critical if not ignored). Returns true on error.
func M_query_warning(conn *DBConnection, query string, fmt string, args ...any) bool {
	return m_queryv(conn, query, M_warning, nil, fmt, args...)
}

// M_query_critical executes the query; on error logs critical (or warning if ignored). Returns true on error.
func M_query_critical(conn *DBConnection, query string, fmt string, args ...any) bool {
	return m_queryv(conn, query, M_critical, M_warning, fmt, args...)
}

// m_query_ext executes query with two log functions (primary and fallback for ignored errors).
func m_query_ext(conn *DBConnection, query string, log_fun_1 func(fmt string, a ...any), log_fun_2 func(fmt string, a ...any), fmt string, args ...any) bool {
	return m_queryv(conn, query, log_fun_1, log_fun_2, fmt, args...)
}

// M_query_verbose executes the query and logs "query: OK" on success; returns true on error.
func M_query_verbose(conn *DBConnection, q string, log_fun func(fmt string, a ...any), fmt string, args ...any) bool {
	var res bool = m_queryv(conn, q, log_fun, nil, fmt, args...)
	if !res {
		log.Infof("%s: OK", q)
	}
	return res
}

// m_resultv runs the query, then calls m_result(conn) to build MYSQL_RES; logs and returns nil on query or result error.
func m_resultv(m_result func(conn *DBConnection) *MYSQL_RES, conn *DBConnection, query string, log_fun_1 func(fmt string, a ...any), log_fun_2 func(fmt string, a ...any), fmt string, args ...any) *MYSQL_RES {
	if m_queryv(conn, query, log_fun_1, log_fun_2, fmt, args...) {
		return nil
	}
	res := m_result(conn)
	if res == nil {
		m_log(conn, log_fun_1, log_fun_2, fmt, args...)
	}
	return res
}

// M_store_result_critical executes query and fetches full result with Mysql_store_result; logs critical on error.
func M_store_result_critical(conn *DBConnection, query string, fmt string, args ...any) *MYSQL_RES {
	return m_resultv(Mysql_store_result, conn, query, M_critical, M_warning, fmt, args...)
}

// M_store_result executes query and streams result with Mysql_use_result; returns nil on error.
func M_store_result(conn *DBConnection, query string, log_fun func(fmt string, a ...any), fmt string, args ...any) *MYSQL_RES {
	return m_resultv(Mysql_use_result, conn, query, log_fun, nil, fmt, args...)
}

// M_store_result_row executes query, fetches full result, and returns the first row as M_ROW (or nil).
func M_store_result_row(conn *DBConnection, query string, log_fun_1 func(fmt string, a ...any), log_fun_2 func(fmt string, a ...any), fmt string, args ...any) *M_ROW {
	var mr *M_ROW = new(M_ROW)
	mr.Row = nil
	mr.Res = m_resultv(Mysql_store_result, conn, query, log_fun_1, log_fun_2, fmt, args...)
	if mr.Res != nil {
		mr.Row = Mysql_fetch_row(mr.Res)
	}
	return mr
}

// M_store_result_single_row executes query, fetches full result, returns first row; logs critical if no row.
func M_store_result_single_row(conn *DBConnection, query string, fmt string, args ...any) *M_ROW {
	var mr *M_ROW = new(M_ROW)
	mr.Row = nil
	mr.Res = m_resultv(Mysql_store_result, conn, query, M_critical, M_warning, fmt, args...)
	if mr.Res != nil {
		mr.Row = Mysql_fetch_row(mr.Res)
		if mr.Row == nil {
			m_log(conn, M_critical, M_warning, fmt, args...)
		}
	}
	return mr
}

// M_use_result executes query and returns streaming result (Mysql_use_result); nil on error.
func M_use_result(conn *DBConnection, query string, log_fun_1 func(fmt string, a ...any), fmt string, args ...any) *MYSQL_RES {
	return m_resultv(Mysql_use_result, conn, query, log_fun_1, nil, fmt, args...)
}

// Execute_set_names runs SET NAMES with the given charset on conn.
func Execute_set_names(conn *DBConnection, _set_names string) {
	var _set_names_statement = set_names_statement_template(_set_names)
	M_query_warning(conn, _set_names_statement, "Not able to execute SET NAMES statement")
}

// M_thread_new starts a new goroutine with G_thread_new; fatals with error_text if thread is nil.
func M_thread_new(title string, f func(any), data any, error_text string) *GThread {
	var thread *GThread = G_thread_new(title, f, data, 0)
	if thread == nil {
		M_critical(error_text)
	}
	return thread
}

// Monitor_throttling_thread is the throttling monitor goroutine entry; currently a no-op (c is queue).
func Monitor_throttling_thread(c any) {
	if c == nil {
		return
	}
	queue := c.(*GAsyncQueue)
	_ = queue
}
