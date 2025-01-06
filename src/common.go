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
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

var (
	Log_output         *os.File
	Logger             *os.File
	ignore_errors_list []uint16
	show_warnings      bool
	No_delete          bool
	stream             bool
	Ignore_errors_list []uint16
	CheckRowCount      bool
	Stream_queue       *GAsyncQueue
)

const (
	MYLOADER_MODE              = "myloader_mode"
	DEFAULTS_FILE              = "/etc/mydumper.cnf"
	VERSION                    = "0.16.7-5"
	DB_LIBRARY                 = "MySQL"
	MYSQL_VERSION_STR          = "8.0.31"
	EXIT_FAILURE               = 1
	EXIT_SUCCESS               = 0
	WIDTH                      = 40
	MIN_THREAD_COUNT           = 2
	IS_INNODB_TABLE            = 2
	INCLUDE_CONSTRAINT         = 4
	IS_ALTER_TABLE_PRESENT     = 8
	START_SLAVE                = "START SLAVE"
	START_SLAVE_SQL_THREAD     = "START SLAVE SQL_THREAD"
	CALL_START_REPLICATION     = "CALL mysql.rds_start_replication();"
	STOP_SLAVE_SQL_THREAD      = "STOP SLAVE SQL_THREAD"
	STOP_SLAVE                 = "STOP SLAVE"
	CALL_STOP_REPLICATION      = "CALL mysql.rds_stop_replication();"
	RESET_SLAVE                = "RESET SLAVE"
	CALL_RESET_EXTERNAL_MASTER = "CALL mysql.rds_reset_external_master()"
	SHOW_SLAVE_STATUS          = "SHOW SLAVE STATUS"
	SHOW_ALL_SLAVES_STATUS     = "SHOW ALL SLAVES STATUS"
	START_REPLICA              = "START REPLICA"
	START_REPLICA_SQL_THREAD   = "START REPLICA SQL_THREAD"
	STOP_REPLICA               = "STOP REPLICA"
	STOP_REPLICA_SQL_THREAD    = "STOP REPLICA SQL_THREAD"
	RESET_REPLICA              = "RESET REPLICA"
	SHOW_REPLICA_STATUS        = "SHOW REPLICA STATUS"
	SHOW_ALL_REPLICAS_STATUS   = "SHOW ALL REPLICAS STATUS"
	SHOW_MASTER_STATUS         = "SHOW MASTER STATUS"
	SHOW_BINLOG_STATUS         = "SHOW BINLOG STATUS"
	SHOW_BINARY_LOG_STATUS     = "SHOW BINARY LOG STATUS"
	CHANGE_MASTER              = "CHANGE MASTER"
	CHANGE_REPLICATION_SOURCE  = "CHANGE REPLICATION SOURCE"
	ZSTD_EXTENSION             = ".zst"
	GZIP_EXTENSION             = ".gz"
	BZIP2_EXTENSION            = ".bz2"
	LZ4_EXTENSION              = ".lz4"
)

type Function_pointer func(string)
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

func Initialize_share_common() {

}

func Initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = make(map[string]string)
	if Detected_server == SERVER_TYPE_MYSQL || Detected_server == SERVER_TYPE_MARIADB {
		set_session_hash["WAIT_TIMEOUT"] = "2147483"
		set_session_hash["NET_WRITE_TIMEOUT"] = "2147483"
	}
	return set_session_hash
}

func Initialize_set_names() {
	if set_names_str != "" {
		if len(set_names_str) != 0 {
			Set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", set_names_str)
		} else {
			set_names_str = ""
		}
	} else {
		set_names_str = "binary"
		Set_names_statement = "/*!40101 SET NAMES binary*/"
	}
}

func Free_set_names() {
	set_names_str = ""
	Set_names_statement = ""
}

func Show_warnings_if_possible(conn *DBConnection) string {
	if !show_warnings {
		return ""
	}
	var result *mysql.Result
	result = conn.Execute("SHOW WARNINGS")
	if conn.Err != nil {
		log.Errorf("Error on SHOW WARNINGS: %v", conn.Err)
		return ""
	}
	var _error *GString = G_string_new("")
	for _, row := range result.Values {
		G_string_append(_error, string(row[2].AsString()))
		G_string_append(_error, "\n")
	}
	return _error.Str.String()
}

func generic_checksum(conn *DBConnection, database, table, query_template string, column_number int) string {
	var query string = fmt.Sprintf(query_template, database, table)
	result := conn.Execute(query)
	if conn.Err != nil {
		log.Errorf("Error dumping checksum (%s.%s): %v", database, table, conn.Err)
		return ""
	}
	var r string

	for _, row := range result.Values {
		r = fmt.Sprintf("%v", row[column_number].Value())
	}
	return r
}

func Checksum_table(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "CHECKSUM TABLE `%s`.`%s`", 1)
}

func Checksum_table_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(column_name, ordinal_position, data_type)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s';", 0)
}

func Checksum_process_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(replace(ROUTINE_DEFINITION,' ','')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.routines WHERE ROUTINE_SCHEMA='%s' order by ROUTINE_TYPE,ROUTINE_NAME", 0)
}

func Checksum_trigger_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s' AND EVENT_OBJECT_TABLE='%s';", 0)
}

func Checksum_trigger_structure_from_database(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(REPLACE(REPLACE(REPLACE(ACTION_STATEMENT, CHAR(32), ''), CHAR(13), ''), CHAR(10), ''), CHAR(9), '')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA='%s';", 0)
}

func Checksum_view_structure(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(REPLACE(VIEW_DEFINITION,TABLE_SCHEMA,'')) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.views WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s';", 0)
}

func Checksum_database_defaults(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(concat(DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' ;", 0)
}

func Checksum_table_indexes(conn *DBConnection, database, table string) string {
	return generic_checksum(conn, database, table, "SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS(TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME)) AS UNSIGNED)), 10, 16)), 0) AS crc FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME", 0)
}

func Load_config_file(config_file string) *ini.File {
	kf, err := ini.Load(config_file)
	if err != nil {
		log.Warnf("Failed to load config file %s: %v", config_file, err)
		return nil
	}
	return kf
}

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

func Load_per_table_info_from_key_file(kf *ini.File, cpt *Configuration_per_table, init_function_pointer *Function_pointer) {
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
						ht[key.Name()] = init_function_pointer
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
					if strings.Compare(key.Name(), "rows") == 0 {
						cpt.All_rows_per_table[groups[i]] = key.Value()
					}
				}

			}
			cpt.All_anonymized_function[groups[i]] = ht
		}
	}
}

func Load_hash_of_all_variables_perproduct_from_key_file(kf *ini.File, set_session_hash map[string]string, str string) {
	if set_session_hash == nil {
		log.Fatalf("set_session_hash is nil")
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

func free_hash_table(hash map[string]string) {
	for key, _ := range hash {
		delete(hash, key)
	}
}

func refresh_set_from_hash(ss *GString, kind string, set_hash map[string]string) {
	if set_hash == nil {
		log.Fatalf("set_hash is nil")
	}
	for lkey, e := range set_hash {
		if strings.HasPrefix(e, "/*!") {
			var c string
			// TODO c variables
			G_string_append_printf(ss, "/%s SET %s %s = %s */;\n", c, kind, lkey, e)
		} else {
			G_string_append_printf(ss, "SET %s %s = %s ;\n", kind, lkey, e)
		}
	}
}

func set_session_hash_insert(set_session_hash map[string]string, _key string, value string) {
	set_session_hash[_key] = value
}

func Refresh_set_session_from_hash(ss *GString, set_session_hash map[string]string) {
	G_string_set_size(ss, 0)
	if _, ok := set_session_hash["FOREIGN_KEY_CHECKS"]; !ok {
		set_session_hash["FOREIGN_KEY_CHECKS"] = "0"
	}
	refresh_set_from_hash(ss, "SESSION", set_session_hash)
}

func set_global_rollback_from_hash(ss *GString, sr *GString, set_hash map[string]string) {
	var stmp *GString
	if len(set_hash) >= 0 {
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
func Refresh_set_global_from_hash(ss *GString, sr *GString, set_global_hash map[string]string) {
	set_global_rollback_from_hash(ss, sr, set_global_hash)
	refresh_set_from_hash(ss, "GLOBAL", set_global_hash)
}

func free_hash(set_session_hash map[string]string) {
	for key, _ := range set_session_hash {
		delete(set_session_hash, key)
	}
}

func Execute_gstring(conn *DBConnection, ss *GString) {
	if ss != nil {
		var line []string = strings.Split(ss.Str.String(), ";\n")
		var i int
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 3 {
				conn.Execute(line[i])
				if conn.Err != nil {
					log.Warnf("Set session failed: %s", line[i])
				}
			}
		}
	}
}

func write_file(file *file_write, buff string) (int, error) {
	return file.write([]byte(buff))
}

func Replace_escaped_strings(b string) string {
	return mysql.Escape(b)
}

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
func create_fifo_dir(new_fifo_directory string) {
	if new_fifo_directory == "" {
		log.Warnf("Fifo directoy provided was invalid")
		return
	}
	if err := os.MkdirAll(new_fifo_directory, 0660); err != nil {
		log.Fatalf("Unable to create `%s': %v", new_fifo_directory, err)
	}
}

func Create_backup_dir(new_directory, new_fifo_directory string) {
	if new_fifo_directory != "" {
		create_fifo_dir(new_fifo_directory)
	}
	if err := os.MkdirAll(new_directory, 0660); err != nil {
		log.Fatalf("Unable to create `%s': %v", new_directory, err)
	}

}
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
func Remove_new_line(to string) string {
	return strings.ReplaceAll(to, "\n", "")
}

func m_remove0(directory string, filename string) {
	remove_path := filepath.Join(directory, filename)
	log.Infof("Removing file: %s ", remove_path)
	err := os.Remove(remove_path)
	if err != nil {
		log.Warnf("Remove failed: %s (%s)", remove_path, err.Error())
	}
}

func M_remove(directory, filename string) bool {
	if Stream != "" && No_delete == false {
		m_remove0(directory, filename)
	}
	return true
}
func matchText(a string, b string) bool {
	return strings.EqualFold(a, b)
}

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

func Is_mysql_special_tables(database string, table string) bool {
	return strings.Compare(database, "mysql") == 0 &&
		(strings.Compare(table, "general_log") == 0 ||
			strings.Compare(table, "slow_log") == 0 ||
			strings.Compare(table, "innodb_index_stats") == 0 ||
			strings.Compare(table, "innodb_table_stats") == 0)
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

func Initialize_common_options(group string) {
	if DefaultsFile == "" {
		if G_file_test(DEFAULTS_FILE) {
			DefaultsFile = DEFAULTS_FILE
		}
	} else {
		if !G_file_test(DefaultsFile) {
			log.Fatalf("Default file %s not found", DefaultsFile)
		}
	}
	if DefaultsExtraFile != "" {
		if !G_file_test(DefaultsExtraFile) {
			log.Fatalf("Default extra file %s not found", DefaultsExtraFile)
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

func Get_table_list(tables_list string) []string {
	tl := strings.Split(tables_list, ",")
	for _, table := range tl {
		if !strings.Contains(table, ".") {
			log.Fatalf("Table name %s is not in DATABASE.TABLE format", table)
		}
	}
	return tl
}

func Remove_definer_from_gchar(str string) string {
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

func Remove_definer(data *GString) {
	str := data.Str.String()
	data.Str.Reset()
	data.Str.WriteString(Remove_definer_from_gchar(str))
}

func Print_version(program string) {
	// 使用 fmt 包的 Printf 函数按照指定格式输出版本信息。
	fmt.Printf("%s v%s, built against %s %s with SSL support\n", program, VERSION, DB_LIBRARY, MYSQL_VERSION_STR)

	// 输出源代码的 Git Commit Hash。
	fmt.Printf("Git Commit Hash: %s\n", GitHash)

	// 输出源代码的 Git 分支名称。
	fmt.Printf("Git Branch: %s\n", GitBranch)

	// 输出程序的构建时间。
	fmt.Printf("Build Time: %s\n", BuildTS)

	// 输出用于构建程序的 Go 语言版本。
	fmt.Printf("Go Version: %s\n", GoVersion)
}

func Stream_arguments_callback() bool {
	// var Err error
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

func Check_num_threads() {
	// 如果配置的线程数量小于等于0，则将其设置为系统处理器数量
	if NumThreads <= 0 {
		NumThreads = g_get_num_processors()
	}
	// 如果线程数量小于最小线程数量，则记录警告并将其设置为最小线程数量
	if NumThreads < MIN_THREAD_COUNT {
		log.Warnf("Invalid number of threads %d, setting to %d", NumThreads, MIN_THREAD_COUNT)
		NumThreads = MIN_THREAD_COUNT
	}
}

func M_error(msg string, args ...any) {
	Execute_gstring(main_connection, Set_global_back)
	log.Errorf(msg, args...)
}

func M_critical(msg string, args ...any) {
	Execute_gstring(main_connection, Set_global_back)
	log.Fatalf(msg, args...)
}

func M_warning(msg string, args ...any) {
	log.Warnf(msg, args...)
}

func Filter_sequence_schemas(create_table string) string {
	re, err := regexp.Compile("`\\w+`\\.(`\\w+`)")
	if err != nil {
		log.Warnf("filter table schema fail:%v", err)
	}
	fss := re.FindAllStringSubmatch(create_table, -1)
	return re.ReplaceAllString(create_table, fss[0][1])
}

func Read_data(infile *bufio.Scanner, data *GString, eof *bool, line *int) bool {
	if !infile.Scan() {
		*eof = true
		return false
	}
	if data.Str.Len() > 0 {
		data.Str.Reset()
	}
	data.Str.WriteString(infile.Text())
	data.Len = data.Str.Len()
	*line++
	if infile.Err() != nil {
		return false
	}
	return true
}
func M_date_time_new_now_local() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

func Double_quoute_protect(r string) string {
	return strings.ReplaceAll(r, "\"", "\"\"")
}

func Backtick_protect(r string) string {
	return strings.ReplaceAll(r, "`", "``")
}

func Newline_protect(r string) string {
	return strings.ReplaceAll(r, "\n", "\u10000")
}

func newline_unprotect(r string) string {
	return strings.ReplaceAll(r, "\u10000", "\n")
}

func widthCompletion(width int, key string) string {
	return key + strings.Repeat(" ", width-len(key))
}
func Print_int(key string, val int) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}
func Print_uint(key string, val uint) {
	fmt.Printf("%s= %d\n", widthCompletion(WIDTH, key), val)
}

func Print_string(key string, val string) {
	if val != "" {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), val)
	} else {
		fmt.Printf("# %s=\n", widthCompletion(WIDTH-2, key))
	}

}

func Print_bool(key string, val bool) {
	if val {
		fmt.Printf("%s= TRUE\n", widthCompletion(WIDTH, key))
	} else {
		fmt.Printf("# %s= FALSE\n", widthCompletion(WIDTH-2, key))
	}
}

func Print_list(key string, val []string) {
	if len(val) != 0 {
		fmt.Printf("%s= %s\n", widthCompletion(WIDTH, key), strings.Join(val, ","))
	} else {
		fmt.Printf("# %s= \"\"\n", widthCompletion(WIDTH-2, key))
	}
}

func append_alter_table(alter_table_statement *GString, table string) {
	G_string_append(alter_table_statement, "ALTER TABLE `")
	G_string_append(alter_table_statement, table)
	G_string_append(alter_table_statement, "` ")
}

func finish_alter_table(alter_table_statement *GString) {

	lastCommaIndex := strings.LastIndex(alter_table_statement.Str.String(), ",")
	if lastCommaIndex > alter_table_statement.Len-5 {
		alter_table_statement = G_string_new(alter_table_statement.Str.String()[:lastCommaIndex])
		G_string_append(alter_table_statement, ";\n")
	} else {
		G_string_append(alter_table_statement, ";\n")
	}
}

func Global_process_create_table_statement(statement *GString, create_table_statement *GString, alter_table_statement *GString, alter_table_constraint_statement *GString, real_table string, split_indexes bool) int {
	var flag int
	var split_file = strings.Split(statement.Str.String(), "\n")
	var autoinc_column string
	append_alter_table(alter_table_statement, real_table)
	append_alter_table(alter_table_constraint_statement, real_table)
	var fulltext_counter int
	var i int
	for i = 0; i < len(split_file); i++ {
		if split_indexes && (strings.HasPrefix(split_file[i], "  KEY") ||
			strings.HasPrefix(split_file[i], "  UNIQUE") ||
			strings.HasPrefix(split_file[i], "  SPATIAL") ||
			strings.HasPrefix(split_file[i], "  FULLTEXT") ||
			strings.HasPrefix(split_file[i], "  INDEX")) {
			if autoinc_column != "" && split_file[i] == autoinc_column {
				G_string_append(create_table_statement, split_file[i])
				G_string_append(create_table_statement, "\n")
			} else {
				flag |= IS_ALTER_TABLE_PRESENT
				if strings.HasPrefix(split_file[i], "  FULLTEXT") {
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
			if strings.HasPrefix(split_file[i], "  CONSTRAINT") {
				flag |= INCLUDE_CONSTRAINT
				G_string_append(alter_table_constraint_statement, "\n ADD")
				G_string_append(alter_table_constraint_statement, split_file[i])
			} else {
				if strings.HasPrefix(split_file[i], "AUTO_INCREMENT") {
					var autoinc_split = strings.SplitN(split_file[i], "`", 3)
					autoinc_column = fmt.Sprintf("(`%s`", autoinc_split[1])
				}
				G_string_append(create_table_statement, split_file[i])
				G_string_append(create_table_statement, "\n")
			}
		}
		if strings.HasPrefix(split_file[i], "ENGINE=InnoDB") {
			flag |= IS_INNODB_TABLE
		}
	}
	G_string_replace(create_table_statement, ",\n)", "\n)")
	finish_alter_table(alter_table_statement)
	finish_alter_table(alter_table_constraint_statement)
	return flag
}
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

func str_list_has_str(str_list []string, str string) bool {
	return slices.Contains(str_list, str)
}

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

func Build_dbt_key(a, b string) string {
	return fmt.Sprintf("%s%s%s.%s%s%s", Identifier_quote_character, a, Identifier_quote_character, Identifier_quote_character, b, Identifier_quote_character)
}
func Common_arguments_callback() bool {
	if SourceControlCommand != "" {
		if strings.ToUpper(SourceControlCommand) == "TRADITIONAL" {
			Source_control_command = TRADITIONAL
			return true
		}
		if strings.ToUpper(SourceControlCommand) == "AWS" {
			Source_control_command = AWS
			return true
		}
	}
	return false
}

func Discard_mysql_output(conn *DBConnection) {
	_ = conn
}

func M_query(conn *DBConnection, query string, log_fun func(fmt string, a ...any), msg string, args ...any) bool {
	conn.Execute(query)
	if conn.Err != nil {
		if !slices.Contains(ignore_errors_list, conn.Code) {
			var c = fmt.Sprintf(msg, args...)
			log_fun("%s - ERROR %d: %v", c, conn.Code, conn.Err)
			return false
		}
	}
	return true
}
