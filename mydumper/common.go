package mydumper

import (
	"errors"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type gKeyFile *ini.File

type anonymized_function_type func(r mysql.FieldValue, length mysql.FieldValue) string

type configuration_per_table struct {
	all_anonymized_function         map[string]map[string]string
	all_where_per_table             map[string]string
	all_limit_per_table             map[string]string
	all_num_threads_per_table       map[string]uint
	all_columns_on_select_per_table map[string]string
	all_columns_on_insert_per_table map[string]string
}

type function_pointer struct {
	fun_ptr    anonymized_function_type
	memory     map[string]string
	value      []string
	parse      []string
	delimiters []string
}

const (
	VERSION                    = "0.15.1-3"
	DB_LIBRARY                 = "MySQL"
	MYSQL_VERSION_STR          = "8.0.31"
	MIN_THREAD_COUNT           = 2
	DEFAULTS_FILE              = "/etc/mydumper.cnf"
	ZSTD_EXTENSION             = ".zst"
	GZIP_EXTENSION             = ".gz"
	EXIT_FAILURE               = 1
	EXIT_SUCCESS               = 0
	UNLOCK_TABLES              = "UNLOCK TABLES"
	INSERT_IGNORE              = "INSERT IGNORE"
	INSERT                     = "INSERT"
	BINARY                     = "binary"
	REPLACE                    = "REPLACE"
	EMPTY_STRING               = ""
	BACKTICK                   = "`"
	DOUBLE_QUOTE               = `"`
	DIRECTORY                  = "export"
	mydumper_global_variables  = "mydumper_global_variables"
	mydumper_session_variables = "mydumper_session_variables"
)

func g_file_test(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	return false
}

func (o *OptionEntries) initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = make(map[string]string)
	if o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_MARIADB {
		set_session_hash["WAIT_TIMEOUT"] = "2147483"
		set_session_hash["NET_WRITE_TIMEOUT"] = "2147483"
	}
	return set_session_hash
}

func initialize_set_names(o *OptionEntries) {
	if strings.ToLower(o.Statement.SetNamesStr) != BINARY {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", o.Statement.SetNamesStr)
	} else {
		o.global.set_names_statement = fmt.Sprintf("/*!40101 SET NAMES %s*/", BINARY)
	}
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

func load_config_file(config_file string) *ini.File {
	kf, err := ini.Load(config_file)
	if err != nil {
		log.Warnf("Failed to load config file %s: %v", config_file, err)
		return nil
	}
	return kf
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

func load_per_table_info_from_key_file(kf *ini.File, conf_per_table *configuration_per_table) {
	var groups = kf.SectionStrings()
	var i int
	var keys []*ini.Key
	var value string
	for i = 0; i < len(groups); i++ {
		if strings.Contains(groups[i], "`.`") && strings.HasPrefix(groups[i], "`") && strings.HasSuffix(groups[i], "`") {
			keys = kf.Section(groups[i]).Keys()
			for _, key := range keys {
				if strings.Compare(key.Name(), "where") == 0 {
					conf_per_table.all_where_per_table[groups[i]] = key.Value()
				}
				if strings.Compare(key.Name(), "limit") == 0 {
					conf_per_table.all_limit_per_table[groups[i]] = key.Value()
				}
				if strings.Compare(key.Name(), "num_threads") == 0 {
					value = key.Value()
					n, _ := strconv.Atoi(value)
					conf_per_table.all_num_threads_per_table[groups[i]] = uint(n)
				}
				if strings.Compare(key.Name(), "columns_on_select") == 0 {
					conf_per_table.all_columns_on_select_per_table[groups[i]] = key.Value()
				}
				if strings.Compare(key.Name(), "columns_on_insert") == 0 {
					conf_per_table.all_columns_on_insert_per_table[groups[i]] = key.Value()
				}
			}

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

func free_hash_table(hash map[string]string) map[string]string {
	for key, _ := range hash {
		delete(hash, key)
	}
	return hash
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

func refresh_set_global_from_hash(ss *string, sr *string, set_global_hash map[string]string) {
	set_global_rollback_from_hash(ss, sr, set_global_hash)
	*ss = refresh_set_from_hash(*ss, "GLOBAL", set_global_hash)
}

func free_hash(set_session_hash map[string]string) map[string]string {
	for key, _ := range set_session_hash {
		delete(set_session_hash, key)
	}
	return set_session_hash
}

func execute_gstring(conn *client.Conn, ss string) {
	if ss != "" {
		lines := strings.Split(ss, ";\n")
		for _, line := range lines {
			if len(line) <= 3 {
				continue
			}
			_, err := conn.Execute(line)
			if err != nil {
				log.Warnf("Set session failed: %s", line)
			}
		}
	}
}

func write_file(file *file_write, buff string) (int, error) {
	return file.write([]byte(buff))
}

func mysqlRealEscapeString(value string) string {
	var sb strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\', 0, '\n', '\r', '\'', '"', '\t', '\f':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '\032':
			sb.WriteByte('\\')
			sb.WriteByte('Z')
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}

func replace_escaped_strings(s string) string {

	var builder strings.Builder
	builder.Grow(len(s)) // 预分配足够的空间

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				builder.WriteByte('\n')
				i++
			case 't':
				builder.WriteByte('\t')
				i++
			case 'r':
				builder.WriteByte('\r')
				i++
			case 'f':
				builder.WriteByte('\f')
				i++
			default:
				builder.WriteByte(s[i])
			}
		} else {
			builder.WriteByte(s[i])
		}
	}

	return builder.String()
}

func escape_tab_with(to string) string {
	var output []byte
	for _, c := range []byte(to) {
		if c == '\t' {
			output = append(output, '\\')
			output = append(output, 't')
		} else {
			output = append(output, c)
		}
	}
	return string(output)
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

func strcount(text string) int {
	return strings.Count(text, "\n")
}

func remove_new_line(to string) string {
	return strings.ReplaceAll(to, "\n", "")
}

func m_remove(o *OptionEntries, directory, filename string) bool {
	if o.Stream.Stream && o.global.no_delete == false {
		remove_path := filepath.Join(directory, filename)
		log.Infof("Removing file: %s ", remove_path)
		_ = os.Remove(remove_path)
	}
	return true
}

func is_table_in_list(table_name string, tl []string) bool {
	return slices.Contains(tl, table_name)
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
	if o.Connection.Socket != "" {
		o.Connection.Protocol = "socket"
	}
	m_key_file_merge(o.global.key_file, extra_key_file)
}

func g_key_file_has_group(kf *ini.File, group string) bool {
	return kf.HasSection(group)
}

func g_get_current_dir() string {
	current_dir, _ := os.Getwd()
	return current_dir
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

func print_version(program string) {
	fmt.Printf("%s v%s, built against %s %s  with SSL support\n", program, VERSION, DB_LIBRARY, MYSQL_VERSION_STR)
}

func stream_arguments_callback(o *OptionEntries) bool {
	if o.Stream.Stream {
		if strings.ToUpper(o.Stream.StreamOpt) == "TRADITIONAL" || o.Stream.StreamOpt == "" {
			return true
		}
		if strings.ToUpper(o.Stream.StreamOpt) == "NO_DELETE" {
			o.global.no_delete = true
			return true
		}
		if strings.ToUpper(o.Stream.StreamOpt) == "NO_STREAM_AND_NO_DELETE" {
			o.global.no_delete = true
			o.global.no_stream = true
			return true
		}
	}
	return false
}

func check_num_threads(o *OptionEntries) {
	if o.Common.NumThreads < MIN_THREAD_COUNT {
		log.Warnf("Invalid number of threads %d, setting to %d", o.Common.NumThreads, MIN_THREAD_COUNT)
		o.Common.NumThreads = MIN_THREAD_COUNT
	}
}

func filter_sequence_schemas(create_table string) string {
	re, err := regexp.Compile("`\\w+`\\.(`\\w+`)")
	if err != nil {
		log.Warnf("filter table schema fail:%v", err)
	}
	fss := re.FindAllStringSubmatch(create_table, -1)
	return re.ReplaceAllString(create_table, fss[0][1])
}

func g_rec_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

func m_date_time_new_now_local() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

func mysqlError(err error) (myErr *mysql.MyError) {
	errors.As(err, &myErr)
	return
}

func mysql_get_server_version(conn *client.Conn) int {
	res, _ := conn.Execute("select @@version")
	v := string(res.Values[0][0].AsString())
	serverVersion := strings.SplitN(v, ".", 3)
	major_version, _ := strconv.Atoi(serverVersion[0])
	release_level, _ := strconv.Atoi(serverVersion[1])
	sub_version, _ := strconv.Atoi(serverVersion[2])
	return major_version*10000 + release_level*100 + sub_version
}

func (o *OptionEntries) free_set_names() {
	o.global.set_names_statement = ""
	o.Statement.SetNamesStr = ""
}

func intToBool(v int) bool {
	if v == 0 {
		return false
	}
	return true
}
