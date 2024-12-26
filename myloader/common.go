package myloader

import (
	"bufio"
	"container/list"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"go-mydumper/src"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type schema_status int
type thread_states int
type file_type int
type check_sum func(conn *db_connection, database, table string) (string, error)

const (
	WIDTH                        = 40
	MYLOADER                     = "myloader"
	EXIT_SUCCESS                 = 0
	EXIT_FAILURE                 = 1
	AFTER_IMPORT_PER_TABLE       = "AFTER_IMPORT_PER_TABLE"
	AFTER_IMPORT_ALL_TABLES      = "AFTER_IMPORT_ALL_TABLES"
	SKIP                         = "SKIP"
	BACKTICK                     = "`"
	DOUBLE_QUOTE                 = "\""
	TRADITIONAL                  = 0
	AWS                          = 1
	ZSTD_EXTENSION               = ".zst"
	GZIP_EXTENSION               = ".gz"
	VERSION                      = "0.16.7-5"
	DB_LIBRARY                   = "MySQL"
	MYSQL_VERSION_STR            = "8.0.31"
	MIN_THREAD_COUNT             = 2
	DEFAULTS_FILE                = "/etc/mydumper.cnf"
	SEQUENCE                     = "sequence"
	TRIGGER                      = "trigger"
	POST                         = "post"
	TABLESPACE                   = "tablespace"
	CREATE_DATABASE              = "create database"
	VIEW                         = "view"
	INDEXES                      = "indexes"
	CONSTRAINTS                  = "constraints"
	RESTORE_JOB_RUNNING_INTERVAL = 10
	BINARY                       = "binary"
)
const (
	NOT_FOUND schema_status = iota
	NOT_FOUND_2
	NOT_CREATED
	CREATING
	CREATED
	DATA_DONE
	INDEX_ENQUEUED
	ALL_DONE
)
const (
	WAITING thread_states = iota
	STARTED
	COMPLETED
)
const (
	INIT file_type = iota
	SCHEMA_TABLESPACE
	SCHEMA_CREATE
	CJT_RESUME
	SCHEMA_TABLE
	DATA
	SCHEMA_VIEW
	SCHEMA_SEQUENCE
	SCHEMA_TRIGGER
	SCHEMA_POST
	CHECKSUM
	//  METADATA_TABLE
	METADATA_GLOBAL
	RESUME
	IGNORED
	LOAD_DATA
	SHUTDOWN
	INCOMPLETE
	DO_NOT_ENQUEUE
	THREAD
	INDEX
	INTERMEDIATE_ENDED
)

type restore_errors struct {
	data_errors        uint64
	index_errors       uint64
	schema_errors      uint64
	trigger_errors     uint64
	view_errors        uint64
	sequence_errors    uint64
	tablespace_errors  uint64
	post_errors        uint64
	constraints_errors uint64
	retries            uint64
}
type database struct {
	name              string
	real_database     string
	filename          string
	schema_state      schema_status
	queue             *asyncQueue
	sequence_queue    *asyncQueue
	mutex             *sync.Mutex
	schema_checksum   string
	post_checksum     string
	triggers_checksum string
}

type thread_data struct {
	conf                *configuration
	thread_id           uint
	status              thread_states
	granted_connections uint
	dbt                 *db_table
}

type configuration struct {
	database_queue   *asyncQueue
	table_queue      *asyncQueue
	retry_queue      *asyncQueue
	data_queue       *asyncQueue
	post_table_queue *asyncQueue
	view_queue       *asyncQueue
	post_queue       *asyncQueue
	ready            *asyncQueue
	pause_resume     *asyncQueue
	stream_queue     *asyncQueue
	table_list       []*db_table
	table_list_mutex *sync.Mutex
	table_hash       map[string]*db_table
	table_hash_mutex *sync.Mutex
	checksum_list    []string
	mutex            *sync.Mutex
	index_queue      *asyncQueue
	done             bool
}
type object_to_export struct {
	no_data     bool
	no_schema   bool
	no_triggers bool
}

type db_table struct {
	database                *database
	table                   string
	real_table              string
	object_to_export        *object_to_export
	rows                    uint64
	restore_job_list        *list.List
	current_threads         uint
	max_threads             uint
	max_connections_per_job uint
	retry_count             uint
	mutex                   *sync.Mutex
	indexes                 *GString
	constraints             *GString
	count                   uint
	schema_state            schema_status
	index_enqueued          bool
	start_data_time         time.Time
	finish_data_time        time.Time
	start_index_time        time.Time
	finish_time             time.Time
	remaining_jobs          int64
	data_checksum           string
	schema_checksum         string
	indexes_checksum        string
	triggers_checksum       string
	is_view                 bool
	is_sequence             bool
}
type GString struct {
	str string
	len int
}

type configuration_per_table struct {
	all_anonymized_function         map[string]string
	all_where_per_table             map[string]string
	all_limit_per_table             map[string]string
	all_num_threads_per_table       map[string]string
	all_columns_on_select_per_table map[string]string
	all_columns_on_insert_per_table map[string]string
	all_object_to_export            map[string]string
	all_partition_regex_per_table   map[string]string
	all_rows_per_table              map[string]string
}
type function_pointer struct {
	function func(string)
}

func common_arguments_callback(o *OptionEntries) bool {
	if o.SourceControlCommand != "" {
		if strings.ToUpper(o.SourceControlCommand) == "TRADITIONAL" {
			o.global.source_control_command = TRADITIONAL
			return true
		}
		if strings.ToUpper(o.SourceControlCommand) == "AWS" {
			o.global.source_control_command = AWS
			return true
		}
	}
	return false
}

func g_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

func g_cond_new() *sync.WaitGroup {
	return new(sync.WaitGroup)
}
func matchText(a, b string) bool {
	ai := 0
	bi := 0

	for ai < len(a) && a[ai] != '%' && a[ai] != '\x00' && bi < len(b) && b[bi] != '\x00' {
		if a[ai] == '_' || a[ai] == b[bi] {
			ai++
			bi++
		} else if a[ai] == '\\' && ai+1 < len(a) && a[ai+1] == '_' && b[bi] == '_' {
			ai += 2
			bi++
		} else {
			return false
		}
	}

	if ai == len(a) || a[ai] == '\x00' {
		return bi == len(b) || b[bi] == '\x00'
	}

	for ai < len(a) && (a[ai] == '%' || a[ai] == '\x00') {
		if a[ai] == '\x00' {
			return true
		}
		ai++
	}

	for bi < len(b) {
		if matchText(a[ai:], b[bi:]) {
			return true
		}
		bi++
	}

	return false
}

func is_table_in_list(database string, table_name string, tl []string) bool {
	var table_name_lower = strings.ToLower(fmt.Sprintf("%s.%s", database, table_name))
	var tb_lower string
	var match bool
	for i := 0; i < len(tl); i++ {
		if !strings.Contains(tl[i], "%") && !strings.Contains(tl[i], "_") {
			if strings.ToLower(tl[i]) == table_name_lower {
				match = true
				break
			}
		} else {
			tb_lower = strings.ToLower(tl[i])
			if tb_lower == table_name_lower {
				match = true
				break
			}
		}
		tb_lower = strings.ToLower(tl[i])
		if matchText(tb_lower, table_name_lower) {
			match = true
			break
		}
	}
	return match
}

func (o *OptionEntries) initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = make(map[string]string)
	if o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_MARIADB {
		set_session_hash["WAIT_TIMEOUT"] = "2147483"
		set_session_hash["NET_WRITE_TIMEOUT"] = "2147483"
	}
	return set_session_hash
}

func generic_checksum(conn *db_connection, database, table, query_template string, column_number int) (string, error) {
	var query string
	if table == "" {
		query = fmt.Sprintf(query_template, database)
	} else {
		query = fmt.Sprintf(query_template, database, table)
	}

	result := conn.Execute(query)
	if conn.err != nil {
		log.Errorf("Error dumping checksum (%s.%s): %v", database, table, conn.err)
	}
	var r string

	for _, row := range result.Values {
		if result.Fields[column_number].Type <= mysql.MYSQL_TYPE_INT24 {
			r = fmt.Sprintf("%d", row[column_number].AsUint64())
		} else {
			r = fmt.Sprintf("%s", row[column_number].AsString())
		}

	}
	return r, nil
}

func checksum_table(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "CHECKSUM TABLE `%s`.`%s`", 1)
}

func checksum_table_structure(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0)
}

func checksum_process_structure(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0)
}

func checksum_trigger_structure(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';", 0)
}

func checksum_trigger_structure_from_database(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';", 0)
}

func checksum_view_structure(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", 0)
}

func checksum_database_defaults(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;", 0)
}

func checksum_table_indexes(conn *db_connection, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0)
}

func g_file_test(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	return false
}

func m_critical(msg string, args ...any) {
	log.Fatalf(msg, args...)
}

func m_warning(msg string, args ...any) {
	log.Warnf(msg, args...)
}

func m_error(msg string, args ...any) {
	log.Errorf(msg, args...)
}

func g_atomic_int_dec_and_test(a *int64) bool {
	atomic.AddInt64(a, -1)
	if *a == 0 {
		return true
	}
	return false

}
func (o *OptionEntries) m_remove0(directory, filename string) {
	if o.Stream && o.NoData == false {
		var dir = path.Join(directory, filename)
		log.Infof("Removing file: %s", dir)
		if err := os.Remove(dir); err != nil {
			log.Warnf("Remove failed: %s (%v)", dir, err)
		}
	}
}

func (o *OptionEntries) m_remove(directory, filename string) bool {
	if o.Stream && o.global.no_delete == false {
		o.m_remove0(directory, filename)
	}
	return true
}

func read_data(infile *bufio.Scanner, data *GString, eof *bool, line *int) bool {
	if !infile.Scan() {
		*eof = true
		return false
	}
	data.str = infile.Text()
	data.len = len(data.str)
	*line++
	if infile.Err() != nil {
		return false
	}
	return true
}

func load_config_file(config_file string) *ini.File {
	kf, err := ini.Load(config_file)
	if err != nil {
		log.Warnf("Failed to load config file %s: %v", config_file, err)
		return nil
	}
	return kf
}

func filter_sequence_schemas(create_table string) string {
	re, err := regexp.Compile("`\\w+`\\.(`\\w+`)")
	if err != nil {
		log.Warnf("filter table schema fail:%v", err)
	}
	fss := re.FindAllStringSubmatch(create_table, -1)
	return re.ReplaceAllString(create_table, fss[0][1])
}

func remove_definer_from_gchar(str *GString) {
	definer := " DEFINER="
	// 查找 " DEFINER=" 子串
	indexDefiner := strings.Index(str.str, definer)
	if indexDefiner != -1 {
		// 找到 " DEFINER=" 后的第一个空格的位置
		substrFromDefiner := str.str[indexDefiner+len(definer):]
		indexSpace := strings.Index(substrFromDefiner, " ")
		if indexSpace != -1 {
			// 将 " DEFINER=" 到下一个空格之间的所有字符替换为空格
			before := str.str[:indexDefiner]
			after := substrFromDefiner[indexSpace:]
			// spaces := strings.Repeat(" ", indexSpace+len(definer))
			// return before + spaces + after
			str.str = before + after
			str.len = len(str.str)
		} else {
			// 如果 " DEFINER=" 后没有空格，则清除到末尾
			before := str.str[:indexDefiner]
			str.str = before
			str.len = len(str.str)
		}
	}
	// 如果没有找到 " DEFINER="，返回原始字符串
	return
}

func remove_definer(data *GString) {
	remove_definer_from_gchar(data)
}

func (o *OptionEntries) initialize_common_options(group string) {
	if o.DefaultsFile == "" {
		if g_file_test(DEFAULTS_FILE) {
			o.DefaultsFile = DEFAULTS_FILE
		}
	} else {
		if !g_file_test(o.DefaultsFile) {
			log.Fatalf("Default file %s not found", o.DefaultsFile)
		}
	}

	if o.DefaultsExtraFile != "" {
		if !g_file_test(o.DefaultsExtraFile) {
			log.Fatalf("Default extra file %s not found", o.DefaultsExtraFile)
		}
	} else {
		if o.DefaultsFile == "" {
			log.Infof("Using no configuration file")
			return
		}
	}
	if o.DefaultsFile == "" {
		o.DefaultsFile = o.DefaultsExtraFile
		o.DefaultsExtraFile = ""
	}
	var new_defaults_file string
	if !path.IsAbs(o.DefaultsFile) {
		new_defaults_file = path.Join(g_get_current_dir(), o.DefaultsFile)
		o.DefaultsFile = new_defaults_file
	}
	o.global.key_file = load_config_file(o.DefaultsFile)
	if o.global.key_file != nil && g_key_file_has_group(o.global.key_file, group) {
		parse_key_file_group(o.global.key_file, o, group)
		set_connection_defaults_file_and_group(o, o.DefaultsFile, group)
	} else {
		set_connection_defaults_file_and_group(o, o.DefaultsFile, "")
	}
	if o.DefaultsExtraFile == "" {
		return
	}
	if !path.IsAbs(o.DefaultsExtraFile) {
		new_defaults_file = path.Join(g_get_current_dir(), o.DefaultsExtraFile)
		o.DefaultsExtraFile = new_defaults_file
	}
	var extra_key_file *ini.File = load_config_file(o.DefaultsExtraFile)
	if extra_key_file != nil && g_key_file_has_group(extra_key_file, group) {
		log.Infof("Parsing extra key file")
		parse_key_file_group(extra_key_file, o, group)
		set_connection_defaults_file_and_group(o, o.DefaultsExtraFile, group)
	} else {
		set_connection_defaults_file_and_group(o, o.DefaultsExtraFile, "")
	}
	log.Infof("Merging config files user: ")
	m_key_file_merge(o.global.key_file, extra_key_file)
}

func parse_key_file_group(kf *ini.File, context *OptionEntries, group string) {
	// var keys []string
	// keys = kf.GetKeyList(group)
	section := kf.Section(group)
	if section == nil {
		log.Errorf("Loading configuration on section %s is null", group)
		return
	}
	keys := section.Keys()
	tmpSection := ini.Empty()
	newSectionTmp, _ := tmpSection.NewSection(group)
	for _, key := range keys {
		_, _ = newSectionTmp.NewKey(key.Name(), key.Value())
	}
	err := tmpSection.MapTo(&context)
	if err == nil {
		log.Infof("Config file loaded")
	}
}

func g_key_file_has_group(kf *ini.File, group string) bool {
	return kf.HasSection(group)
}

func g_get_current_dir() string {
	current_dir, _ := os.Getwd()
	return current_dir
}

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

func print_version(program string) {
	fmt.Printf("%s v%s, built against %s %s with SSL support\n", program, VERSION, DB_LIBRARY, MYSQL_VERSION_STR)
	fmt.Printf("Git Commit Hash: %s\n", src.GitHash)
	fmt.Printf("Git Branch: %s\n", src.GitBranch)
	fmt.Printf("Build Time: %s\n", src.BuildTS)
	fmt.Printf("Go Version: %s\n", src.GoVersion)
}

func (o *OptionEntries) initialize_set_names() {
	if strings.ToLower(o.SetNamesStr) != BINARY {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", o.SetNamesStr)
	} else {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", BINARY)
	}
}

func create_backup_dir(new_directory string) {
	if new_directory != "" {
		if g_file_test(new_directory) {
			log.Debugf("directory %s is exist ", new_directory)
			return
		}
		err := os.Mkdir(new_directory, 0750)
		if err != nil {
			log.Fatalf("Unable to create `%s': %v", new_directory, err)
		}
	}
	// create_fifo_dir(new_fifo_directory)
}

func create_fifo_dir(new_fifo_directory string) {
	if new_fifo_directory != "" {
		if g_file_test(new_fifo_directory) {
			log.Debugf("fifo directory %s is exist ", new_fifo_directory)
			return
		}
		err := os.Mkdir(new_fifo_directory, 0750)
		if err != nil {
			log.Fatalf("Unable to create `%s': %v", new_fifo_directory, err)
		}
	}
}

func load_hash_of_all_variables_perproduct_from_key_file(kf *ini.File, context *OptionEntries, set_session_hash map[string]string, str string) map[string]string {
	var s string = str
	set_session_hash = load_hash_from_key_file(kf, set_session_hash, s)
	s += "_"
	s += context.get_product_name()
	set_session_hash = load_hash_from_key_file(kf, set_session_hash, s)
	s += fmt.Sprintf("_%d", context.get_major())
	set_session_hash = load_hash_from_key_file(kf, set_session_hash, s)
	s += fmt.Sprintf("_%d", context.get_secondary())
	set_session_hash = load_hash_from_key_file(kf, set_session_hash, s)
	s += fmt.Sprintf("_%d", context.get_revision())
	set_session_hash = load_hash_from_key_file(kf, set_session_hash, s)
	return set_session_hash
}

func load_hash_from_key_file(kf *ini.File, set_session_hash map[string]string, group_variables string) map[string]string {
	var keys []*ini.Key
	section := kf.Section(group_variables)
	keys = section.Keys()
	if set_session_hash == nil {
		set_session_hash = make(map[string]string)
	}
	for i := 0; i < len(keys); i++ {
		value := keys[i].Value()
		set_session_hash[keys[i].Name()] = value
	}
	return set_session_hash
}

func refresh_set_global_from_hash(ss *GString, sr *GString, set_global_hash map[string]string) {
	set_global_rollback_from_hash(ss, sr, set_global_hash)
	refresh_set_from_hash(ss, "GLOBAL", set_global_hash)
}

func set_global_rollback_from_hash(ss *GString, sr *GString, set_hash map[string]string) {
	var lkey, e string
	if len(set_hash) > 0 {
		var stmp *GString = g_string_new(" INTO")
		g_string_append(ss, "SELECT ")
		for lkey, e = range set_hash {
			_ = e
			break
		}
		g_string_append_printf(stmp, " @%s", lkey)
		g_string_append_printf(sr, "SET GLOBAL %s = @%s ;\n", lkey, lkey)
		g_string_append_printf(ss, " @@%s", lkey)
		var i = 0
		for lkey, e = range set_hash {
			if i == 0 {
				i++
				continue
			}
			g_string_append_printf(stmp, ", @%s", lkey)
			g_string_append_printf(sr, "SET GLOBAL %s = @%s ;\n", lkey, lkey)
			g_string_append_printf(ss, ", @@%s", lkey)
			i++
		}
		g_string_append_printf(ss, "%s ;\n", stmp.str)

	}
}

func refresh_set_from_hash(ss *GString, kind string, set_hash map[string]string) {
	for lkey, e := range set_hash {
		var c = "/*!"
		if strings.HasPrefix(e, c) {
			g_string_append_printf(ss, "/%s SET %s %s = %s */;\n", c, kind, lkey, e)
		} else {
			g_string_append_printf(ss, "SET %s %s = %s ;\n", kind, lkey, e)
		}
	}
}

func refresh_set_session_from_hash(ss *GString, set_session_hash map[string]string) {
	refresh_set_from_hash(ss, "SESSION", set_session_hash)
}

func get_table_list(tables_list string) []string {
	tl := strings.Split(tables_list, ",")
	for _, table := range tl {
		if !strings.Contains(table, ".") {
			log.Fatalf("Table name %s is not in DATABASE.TABLE format", table)
		}
	}
	return tl
}

func pring_help(o *OptionEntries) {
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [OPTION…] multi-threaded MySQL loader\n", MYLOADER)
	pflag.PrintDefaults()
	print_string("host", o.Hostname)
	print_string("user", o.Username)
	print_string("password", o.Password)
	print_bool("ask-password", o.AskPassword)
	print_int("port", o.Port)
	print_string("socket", o.SocketPath)
	print_string("protocol", o.Protocol)
	// print_bool("compress-protocol",compress_protocol);
	print_bool("ssl", o.Ssl)
	// print_string("ssl-mode",ssl_mode);
	print_string("key", o.Key)
	print_string("cert", o.Cert)
	print_string("ca", o.Ca)
	// print_string("capath",capath);
	// print_string("cipher",cipher);
	// print_string("tls-version",tls_version);
	print_list("regex", o.global.regex_list)
	print_string("source-db", o.SourceDb)

	print_bool("skip-triggers", o.SkipTriggers)
	print_bool("skip-constraints", o.SkipConstraints)
	print_bool("skip-indexes", o.SkipIndexes)
	print_bool("skip-post", o.SkipPost)
	print_bool("no-data", o.NoData)

	print_string("omit-from-file", o.TablesSkiplistFile)
	print_string("tables-list", o.TablesList)
	print_string("pmm-path", o.PmmPath)
	print_string("pmm-resolution", o.PmmResolution)

	print_bool("enable-binlog", o.EnableBinlog)
	if o.InnodbOptimizeKeys == "" {
		print_string("innodb-optimize-keys", SKIP)
	} else if o.global.innodb_optimize_keys_per_table {
		print_string("innodb-optimize-keys", AFTER_IMPORT_PER_TABLE)
	} else if o.global.innodb_optimize_keys_all_tables {
		print_string("innodb-optimize-keys", AFTER_IMPORT_ALL_TABLES)
	} else {
		print_string("innodb-optimize-keys", "")
	}

	print_bool("no-schemas", o.NoSchemas)

	print_string("purge-mode", o.PurgeModeStr)
	print_bool("disable-redo-log", o.DisableRedoLog)
	print_string("checksum", o.global.checksum_str)
	print_bool("overwrite-tables", o.OverwriteTables)
	print_bool("overwrite-unsafe", o.OverwriteUnsafe)
	print_uint("retry-count", o.RetryCount)
	print_bool("serialized-table-creation", o.SerialTblCreation)
	print_bool("stream", o.Stream)

	print_uint("max-threads-per-table", o.MaxThreadsPerTable)
	print_uint("max-threads-for-index-creation", o.MaxThreadsForIndexCreation)
	print_uint("max-threads-for-post-actions", o.MaxThreadsForPostCreation)
	print_uint("max-threads-for-schema-creation", o.MaxThreadsForSchemaCreation)
	print_string("exec-per-thread", o.ExecPerThread)
	print_string("exec-per-thread-extension", o.ExecPerThreadExtension)

	print_int("rows", o.Rows)
	print_uint("queries-per-transaction", o.CommitCount)
	print_bool("append-if-not-exist", o.AppendIfNotExist)
	print_string("set-names", o.SetNamesStr)

	print_bool("skip-definer", o.SkipDefiner)
	print_bool("help", o.Help)

	print_string("directory", o.InputDirectory)
	print_string("logfile", o.LogFile)

	print_string("database", o.DB)
	print_string("quote-character", o.global.identifier_quote_character_str)
	print_bool("resume", o.Resume)
	print_uint("threads", o.NumThreads)
	print_bool("version", o.ProgramVersion)
	print_int("verbose", o.Verbose)
	print_bool("debug", o.Debug)
	print_string("defaults-file", o.DefaultsFile)
	print_string("defaults-extra-file", o.DefaultsExtraFile)
	// print_string("fifodir",fifo_directory);
	os.Exit(EXIT_SUCCESS)
}

// check_num_threads 检查并设置线程数量
// 该函数根据选项配置和系统处理器数量，确保线程数量设置为合理值
// 参数:
//
//	o *OptionEntries: 指向选项配置的指针
func (o *OptionEntries) check_num_threads() {
	// 如果配置的线程数量小于等于0，则将其设置为系统处理器数量
	if o.NumThreads <= 0 {
		o.NumThreads = g_get_num_processors()
	}
	// 如果线程数量小于最小线程数量，则记录警告并将其设置为最小线程数量
	if o.NumThreads < MIN_THREAD_COUNT {
		log.Warnf("Invalid number of threads %d, setting to %d", o.NumThreads, MIN_THREAD_COUNT)
		o.NumThreads = MIN_THREAD_COUNT
	}
}
func append_alter_table() {

}

func parse_object_to_export(object_to_export **object_to_export, val string) {

}
func finish_alter_table(alter_table_statement *GString) {
	str := strings.LastIndex(alter_table_statement.str[:alter_table_statement.len], ",")
	if str > alter_table_statement.len-5 {
		alter_table_statement.str = alter_table_statement.str[:str] + ";" + alter_table_statement.str[str+1:]
		alter_table_statement.str += "\n"
	} else {
		alter_table_statement.str += ";\n"
	}
	alter_table_statement.len = len(alter_table_statement.str)
}
func global_process_create_table_statement(statement, create_table_statement, alter_table_statement, alter_table_constraint_statement *GString, real_table string, split_indexes bool) int {
	return 0
}

func (o *OptionEntries) show_warnings_if_possible(c *db_connection) string {
	return ""
}

func g_assert(r bool) {
	if !r {
		panic("Assertion failed")
	}
}

func g_thread_new(thread_name string, thread *sync.WaitGroup, thread_id uint) *GThreadFunc {
	var gtf = new(GThreadFunc)
	gtf.thread = thread
	gtf.name = thread_name
	gtf.thread_id = thread_id
	gtf.thread.Add(1)
	return gtf

}
func mysql_thread_id(dc *db_connection) uint64 {
	res, err := dc.conn.Execute("SELECT connection_id()")
	if err != nil {
		log.Fatalf("Error getting connection_id: %v", err)
		return 0
	}
	for _, v := range res.Values {
		for _, v2 := range v {
			return v2.AsUint64()
		}
	}
	return 0
}
func g_string_append_printf(s *GString, msg string, args ...any) {
	s.str += fmt.Sprintf(msg, args...)
	s.len = len(s.str)
}
func g_string_append(s *GString, str string) {
	s.str += str
	s.len += len(str)
}

func g_string_set_size(s *GString, size int) {
	s.str = s.str[:size]
	s.len = size
}

func g_string_assign(s *GString, str string) {
	s.str = str
	s.len = len(str)
}

func g_string_printf(s *GString, msg string, args ...any) {
	s.str = fmt.Sprintf(msg, args...)
	s.len = len(s.str)

}

func g_string_new(str string, args ...any) *GString {
	var s = new(GString)
	s.str = fmt.Sprintf(str, args...)
	s.len = len(s.str)
	return s

}
func g_string_free(str *GString, free bool) {
	str.str = ""
	str.len = 0
	if free {
		str = nil
	}
}
func (o *OptionEntries) initialize_share_common() {

}

func g_get_num_processors() uint {
	return uint(runtime.NumCPU())
}

func print_int(key string, val int) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}
func print_uint(key string, val uint) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}

func print_string(key string, val string) {
	if val != "" {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), val)
	} else {
		fmt.Printf("# %s=\n", widthCompletion(WIDTH-2, key))
	}

}

func print_bool(key string, val bool) {
	if val {
		fmt.Printf("%s= TRUE\n", widthCompletion(WIDTH, key))
	} else {
		fmt.Printf("# %s= FALSE\n", widthCompletion(WIDTH-2, key))
	}
}

func print_list(key string, val []string) {
	if len(val) != 0 {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), strings.Join(val, ","))
	} else {
		fmt.Printf("# %s= \"\"\n", widthCompletion(WIDTH-2, key))
	}
}

func widthCompletion(width int, key string) string {
	return key + strings.Repeat(" ", width-len(key))
}

func initialize_conf_per_table(cpt *configuration_per_table) {
	cpt.all_anonymized_function = make(map[string]string)
	cpt.all_where_per_table = make(map[string]string)
	cpt.all_limit_per_table = make(map[string]string)
	cpt.all_num_threads_per_table = make(map[string]string)
	cpt.all_columns_on_select_per_table = make(map[string]string)
	cpt.all_columns_on_insert_per_table = make(map[string]string)
	cpt.all_object_to_export = make(map[string]string)
	cpt.all_partition_regex_per_table = make(map[string]string)
	cpt.all_rows_per_table = make(map[string]string)

}

func load_per_table_info_from_key_file(kf *ini.File, cpt *configuration_per_table, init_function_pointer *function_pointer) {

}
