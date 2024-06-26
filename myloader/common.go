package myloader

import (
	"bufio"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"io"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type schema_status int
type thread_states int
type file_type int
type check_sum func(conn *client.Conn, database, table string) (string, error)

const (
	MYLOADER     = "myloader"
	EXIT_SUCCESS = 0
)
const (
	NOT_FOUND schema_status = iota
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
	schema_state      schema_status
	queue             *asyncQueue
	mutex             *sync.Mutex
	schema_checksum   string
	post_checksum     string
	triggers_checksum string
}

type thread_data struct {
	conf             *configuration
	thrconn          *client.Conn
	current_database string
	thread_id        uint
	status           thread_states
	err              error
}

type configuration struct {
	database_queue   *asyncQueue
	table_queue      *asyncQueue
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

type db_table struct {
	database          *database
	table             string
	real_table        string
	rows              uint64
	restore_job_list  []*restore_job
	current_threads   uint
	max_threads       uint
	max_threads_hard  uint
	mutex             *sync.Mutex
	indexes           string
	constraints       string
	count             uint
	schema_state      schema_status
	index_enqueued    bool
	start_data_time   time.Time
	finish_data_time  time.Time
	start_index_time  time.Time
	finish_time       time.Time
	remaining_jobs    int64
	data_checksum     string
	schema_checksum   string
	indexes_checksum  string
	triggers_checksum string
	is_view           bool
}

func g_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

func is_table_in_list(table_name string, tl []string) bool {
	return slices.Contains(tl, table_name)
}

func (o *OptionEntries) initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = make(map[string]string)
	if o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_MARIADB {
		set_session_hash["WAIT_TIMEOUT"] = "2147483"
		set_session_hash["NET_WRITE_TIMEOUT"] = "2147483"
	}
	return set_session_hash
}

func generic_checksum(conn *client.Conn, database, table, query_template string, column_number int) (string, error) {
	var query string
	if table == "" {
		query = fmt.Sprintf(query_template, database)
	} else {
		query = fmt.Sprintf(query_template, database, table)
	}

	result, err := conn.Execute(query)
	if err != nil {
		log.Errorf("Error dumping checksum (%s.%s): %v", database, table, err)
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

func checksum_table(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "CHECKSUM TABLE `%s`.`%s`", 1)
}

func checksum_table_structure(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0)
}

func checksum_process_structure(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0)
}

func checksum_trigger_structure(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';", 0)
}

func checksum_trigger_structure_from_database(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';", 0)
}

func checksum_view_structure(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", 0)
}

func checksum_database_defaults(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;", 0)
}

func checksum_table_indexes(conn *client.Conn, database, table string) (string, error) {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0)
}

func g_file_test(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	return false
}

func m_critical(msg string) {
	log.Fatalf(msg)
}

func m_warning(msg string) {
	log.Warnf(msg)
}

func m_error(msg string) {
	log.Errorf(msg)
}

func g_atomic_int_dec_and_test(a *int64) bool {
	atomic.AddInt64(a, -1)
	if *a == 0 {
		return true
	}
	return false

}

func m_remove(o *OptionEntries, directory, filename string) bool {
	if o.Execution.Stream && o.Filter.NoData == false {
		var dir = path.Join(directory, filename)
		log.Infof("Removing file: %s", dir)
		os.Remove(dir)
	}
	return true
}

func read_data(reader *bufio.Reader, data *string, eof *bool, line *uint) bool {
	var err error
	var buffer string
	for {
		buffer, err = reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				*eof = true
				buffer = ""
				// If there's any content collected before EOF, return it as the last line.
			} else {
				return false // An error occurred during reading
			}
		}
		*data += buffer
		if *data == "" {
			break
		}
		if (*data)[len(*data)-1] == '\n' {
			// Successfully read a line
			*line++
		}
		if buffer == "\n" || buffer == "" || strings.HasSuffix(buffer, ";\n") {
			break
		}
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

func remove_definer_from_gchar(str string) string {
	definer := " DEFINER="
	// 查找 " DEFINER=" 子串
	indexDefiner := strings.Index(str, definer)
	if indexDefiner != -1 {
		// 找到 " DEFINER=" 后的第一个空格的位置
		substrFromDefiner := str[indexDefiner+len(definer):]
		indexSpace := strings.Index(substrFromDefiner, " ")
		if indexSpace != -1 {
			// 将 " DEFINER=" 到下一个空格之间的所有字符替换为空格
			before := str[:indexDefiner]
			after := substrFromDefiner[indexSpace:]
			// spaces := strings.Repeat(" ", indexSpace+len(definer))
			// return before + spaces + after
			return before + after
		} else {
			// 如果 " DEFINER=" 后没有空格，则清除到末尾
			before := str[:indexDefiner]
			return before
		}
	}
	// 如果没有找到 " DEFINER="，返回原始字符串
	return str
}

func remove_definer(data string) string {
	return remove_definer_from_gchar(data)
}

func initialize_common_options(o *OptionEntries, group string) {
	if o.Common.DefaultsFile == "" {
		if g_file_test(DEFAULTS_FILE) {
			o.Common.DefaultsFile = DEFAULTS_FILE
		}
	} else {
		if !g_file_test(o.Common.DefaultsFile) {
			log.Fatalf("Default file %s not found", o.Common.DefaultsFile)
		}
	}

	if o.Common.DefaultsExtraFile != "" {
		if !g_file_test(o.Common.DefaultsExtraFile) {
			log.Fatalf("Default extra file %s not found", o.Common.DefaultsExtraFile)
		}
	} else {
		if o.Common.DefaultsFile == "" {
			log.Infof("Using no configuration file")
			return
		}
	}
	if o.Common.DefaultsFile == "" {
		o.Common.DefaultsFile = o.Common.DefaultsExtraFile
		o.Common.DefaultsExtraFile = ""
	}
	var new_defaults_file string
	if !path.IsAbs(o.Common.DefaultsFile) {
		new_defaults_file = path.Join(g_get_current_dir(), o.Common.DefaultsFile)
		o.Common.DefaultsFile = new_defaults_file
	}
	o.global.key_file = load_config_file(o.Common.DefaultsFile)
	if o.global.key_file != nil && g_key_file_has_group(o.global.key_file, group) {
		parse_key_file_group(o.global.key_file, o, group)
		set_connection_defaults_file_and_group(o, o.Common.DefaultsFile, group)
	} else {
		set_connection_defaults_file_and_group(o, o.Common.DefaultsFile, "")
	}
	if o.Common.DefaultsExtraFile == "" {
		return
	}
	if !path.IsAbs(o.Common.DefaultsExtraFile) {
		new_defaults_file = path.Join(g_get_current_dir(), o.Common.DefaultsExtraFile)
		o.Common.DefaultsExtraFile = new_defaults_file
	}
	var extra_key_file *ini.File = load_config_file(o.Common.DefaultsExtraFile)
	if extra_key_file != nil && g_key_file_has_group(extra_key_file, group) {
		log.Infof("Parsing extra key file")
		parse_key_file_group(extra_key_file, o, group)
		set_connection_defaults_file_and_group(o, o.Common.DefaultsExtraFile, group)
	} else {
		set_connection_defaults_file_and_group(o, o.Common.DefaultsExtraFile, "")
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
	fmt.Printf("%s v%s, built against %s %s  with SSL support\n", program, VERSION, DB_LIBRARY, MYSQL_VERSION_STR)
}

func initialize_set_names(o *OptionEntries) {
	if strings.ToLower(o.Statement.SetNamesStr) != BINARY {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", o.Statement.SetNamesStr)
	} else {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", BINARY)
	}
}

func create_backup_dir(new_directory, new_fifo_directory string) {
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
	create_fifo_dir(new_fifo_directory)
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

func refresh_set_global_from_hash(ss *string, sr *string, set_global_hash map[string]string) {
	set_global_rollback_from_hash(ss, sr, set_global_hash)
	*ss = refresh_set_from_hash(*ss, "GLOBAL", set_global_hash)
}

func set_global_rollback_from_hash(ss *string, sr *string, set_hash map[string]string) {
	if len(set_hash) > 0 {
		for key, value := range set_hash {
			_ = value
			stmp := " INTO"
			*ss += "SELECT "
			stmp += fmt.Sprintf(" @%s", key)
			*sr += fmt.Sprintf("SET GLOBAL %s = @%s ;\n", key, key)
			*ss += fmt.Sprintf(" @@%s", key)
			var i = 0
			for key, value = range set_hash {
				if i == 0 {
					i++
					continue
				}
				stmp += fmt.Sprintf(", @%s", key)
				*sr += fmt.Sprintf("SET GLOBAL %s = @%s ;\n", key, key)
				*ss += fmt.Sprintf(", @@%s", key)
				i++
			}
			*ss += fmt.Sprintf("%s ;\n", stmp)
		}
	}
}

func refresh_set_from_hash(ss string, kind string, set_hash map[string]string) string {
	for key, value := range set_hash {
		index := strings.Index(value, "/*!")
		if index != -1 {
			var e = value[:index]
			var c = value[index:]
			ss += fmt.Sprintf("/%s SET %s %s = %s */;\n", c, kind, key, e)
			if strings.Contains(ss, "//") {
				ss = strings.ReplaceAll(ss, "//", "/")
			}
		} else {
			v, err := strconv.Atoi(value)
			if err != nil {
				ss += fmt.Sprintf("SET %s %s = '%s' ;\n", kind, key, value)
			} else {
				ss += fmt.Sprintf("SET %s %s = %d ;\n", kind, key, v)
			}

		}

	}
	return ss
}

func refresh_set_session_from_hash(ss string, set_session_hash map[string]string) string {
	ss = refresh_set_from_hash(ss, "SESSION", set_session_hash)
	return ss
}

func get_table_list(o *OptionEntries, tables_list string) []string {
	tl := strings.Split(tables_list, ",")
	for _, table := range tl {
		if !strings.Contains(table, ".") {
			log.Fatalf("Table name %s is not in DATABASE.TABLE format", table)
		}
	}
	return tl
}

func pring_help() {
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [OPTION…] multi-threaded MySQL loader\n", MYLOADER)
	pflag.PrintDefaults()

	os.Exit(0)
}
