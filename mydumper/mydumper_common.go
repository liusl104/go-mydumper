package mydumper

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

var (
	Compact         bool
	Compress        string
	headers         *GString
	ref_table_mutex *sync.Mutex
	ref_table       map[string]string
	table_number    uint
	nroutines       uint = 4
	server_version  uint
	routine_type    []string = []string{"FUNCTION", "PROCEDURE", "PACKAGE", "PACKAGE BODY"}
)

type file_write struct {
	write    write_fun
	close    close_fun
	flush    flush_fun
	filename string
	status   int
}

func initialize_common() {
	ref_table_mutex = G_mutex_new()
	ref_table = make(map[string]string)
}

func free_common() {
	ref_table_mutex.Lock()
	ref_table = nil
	ref_table_mutex.Unlock()
	ref_table_mutex = nil
}

func determine_filename(table string) string {
	if !masquerade_filename && Check_filename_regex(table) && !strings.Contains(table, ".") && !strings.HasSuffix(table, "mydumper_") {
		return table
	} else {
		r := fmt.Sprintf("mydumper_%d", table_number)
		table_number++
		return r
	}
}

func get_ref_table(k string) string {
	ref_table_mutex.Lock()
	var val string = ref_table[k]
	if val == "" {
		var t = k
		val = determine_filename(t)
		ref_table[t] = val
	}
	ref_table_mutex.Unlock()
	return val
}

func escape_string(str string) string {
	return mysql.Escape(str)
}

func build_schema_table_filename(database string, table string, suffix string) string {
	var filename string
	filename = fmt.Sprintf("%s.%s-%s.sql", database, table, suffix)
	r := path.Join(dump_directory, filename)
	return r
}

func build_schema_filename(database, suffix string) string {
	var filename string
	filename = fmt.Sprintf("%s-%s.sql", database, suffix)
	r := path.Join(dump_directory, filename)
	return r
}

func build_tablespace_filename() string {
	return path.Join(dump_directory, "all-schema-create-tablespace.sql")
}

func build_meta_filename(dump_directory, database string, table string, suffix string) string {
	var filename string
	if table != "" {
		filename = fmt.Sprintf("%s.%s-%s", database, table, suffix)
	} else {
		filename = fmt.Sprintf("%s-%s", database, suffix)
	}
	r := path.Join(dump_directory, filename)
	return r
}

func set_charset(statement *GString, character_set []byte, collation_connection []byte) {
	G_string_printf(statement, "SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n")
	G_string_append(statement, "SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n")
	G_string_append(statement, "SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n")
	G_string_append_printf(statement, "SET character_set_client = %s;\n", character_set)
	G_string_append_printf(statement, "SET character_set_results = %s;\n", character_set)
	G_string_append_printf(statement, "SET collation_connection = %s;\n", collation_connection)

}

func restore_charset(statement *GString) {
	G_string_append(statement, "SET character_set_client = @PREV_CHARACTER_SET_CLIENT;\n")
	G_string_append(statement, "SET character_set_results = @PREV_CHARACTER_SET_RESULTS;\n")
	G_string_append(statement, "SET collation_connection = @PREV_COLLATION_CONNECTION;\n")
}

func clear_dump_directory(directory string) error {
	dir, err := os.Open(directory)

	if err != nil {
		log.Criticalf("cannot open directory %s, %v", directory, err)
		Errors++
		return err
	}
	defer dir.Close()
	filename, err := dir.Readdirnames(-1)
	if err != nil {
		log.Criticalf("error removing file %s (%v)", directory, err)
		Errors++
		return err
	}
	for _, file := range filename {
		file_path := path.Join(directory, file)
		err = os.Remove(file_path)
		if err != nil {
			log.Criticalf("error removing file %s (%v)", file_path, err)
			Errors++
			return err
		}
	}
	return nil
}

func set_transaction_isolation_level_repeatable_read(conn *DBConnection) {
	M_query_critical(conn, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", "Failed to set isolation level")
}

func build_filename(database string, table string, part uint64, sub_part uint, extension string, second_extension string) string {
	var filename string
	var ext string
	var sec string
	if second_extension != "" {
		ext = "."
		sec = second_extension
	}
	if sub_part == 0 {
		filename = fmt.Sprintf("%s.%s.%05d.%s%s%s", database, table, part, extension, ext, sec)
	} else {
		filename = fmt.Sprintf("%s.%s.%05d.%05d.%s%s%s", database, table, part, sub_part, extension, ext, sec)
	}
	r := path.Join(dump_directory, filename)
	return r
}

func build_sql_filename(database string, table string, part uint64, sub_part uint) string {
	return build_filename(database, table, part, sub_part, SQL, "")
}

func build_rows_filename(database string, table string, part uint64, sub_part uint) string {
	return build_filename(database, table, part, sub_part, rows_file_extension, "")
}

func m_real_escape_string(from string) string {
	var to strings.Builder
	for _, ch := range from {
		var escape string
		switch ch {
		case 0:
			escape = "\\0"
		case '\n':
			escape = "\\n"
		case '\r':
			escape = "\\r"
		case '\\':
			escape = "\\\\"
		case '\'':
			escape = "\\'"
		case '"':
			escape = "\\\""
		case '\032':
			escape = "\\Z"
		default:
			to.WriteRune(ch)
			continue
		}
		to.WriteString(escape)
	}
	return to.String()
}

func m_escape_char_with_char(needle byte, repl byte, to []byte) []byte {
	// 预分配缓冲区：最坏情况下（全是目标字符）长度会翻倍，这里按原长度的1.5倍预分配减少扩容
	buf := bytes.NewBuffer(make([]byte, 0, len(to)*3/2))

	for _, b := range to {
		// 若当前字符是目标字符，先写入替换字符
		if b == needle {
			buf.WriteByte(repl)
		}
		// 写入当前字符（无论是否为目标字符）
		buf.WriteByte(b)
	}

	return buf.Bytes()
}

func m_replace_char_with_char(needle byte, repl byte, from []byte) string {
	for i := 0; i < len(from); i++ {
		if from[i] == needle {
			from[i] = repl
			i++
		}
	}
	return string(from)
}

func determine_show_table_status_columns(result *mysql.Result, ecol *int, ccol *int, collcol *int, rowscol *int) {
	var fields = result.Fields
	var i int
	for i = 0; i < len(fields); i++ {
		if strings.EqualFold(string(fields[i].Name), "Engine") {
			*ecol = i
		} else if strings.EqualFold(string(fields[i].Name), "Comment") {
			*ccol = i
		} else if strings.EqualFold(string(fields[i].Name), "Collation") {
			*collcol = i
		} else if strings.EqualFold(string(fields[i].Name), "Rows") {
			*rowscol = i
		}
	}
	G_assert(*ecol > 0)
	G_assert(*ccol > 0)
	G_assert(*collcol > 0)
}

func determine_explain_columns(result *mysql.Result, rowscol *uint) {
	var fields []*mysql.Field = result.Fields
	var i int
	for i = 0; i < len(fields); i++ {
		if strings.EqualFold(string(fields[i].Name), "rows") {
			*rowscol = uint(i)
		} else if strings.EqualFold(string(fields[i].Name), "estRows") { // TiDB
			*rowscol = uint(i)
		}
	}
}

func determine_charset_and_coll_columns_from_show(result *MYSQL_RES, charcol *uint, collcol *uint) {
	*charcol = 0
	*collcol = 0
	var fields []*mysql.Field = Mysql_fetch_fields(result)
	var i uint
	for i = 0; i < uint(len(fields)); i++ {
		if strings.EqualFold(string(fields[i].Name), "character_set_client") {
			*charcol = i
		} else if strings.EqualFold(string(fields[i].Name), "collation_connection") {
			*collcol = i
		}
	}
	G_assert(*charcol > 0)
	G_assert(*collcol > 0)
}

func initialize_header_in_gstring(_headers *GString, charset string) {
	if Is_mysql_like() {
		if charset != "" {
			G_string_printf(_headers, "/*!40101 SET NAMES %s*/;\n", charset)
		}
		G_string_append(_headers, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")
		if Sql_mode != "" && !Compact {
			G_string_append_printf(_headers, "/*!40101 SET SQL_MODE=%s*/;\n", Sql_mode)
		}
		if !SkipTz {
			G_string_append(_headers, "/*!40103 SET TIME_ZONE='+00:00' */;\n")
		}
	} else if Get_product() == SERVER_TYPE_TIDB {
		if !SkipTz {
			G_string_printf(_headers, "/*!40103 SET TIME_ZONE='+00:00' */;\n")
		}
	} else {
		G_string_printf(_headers, "SET FOREIGN_KEY_CHECKS=0;\n")
		if Sql_mode != "" && !Compact {
			G_string_append_printf(_headers, "SET SQL_MODE=%s;\n", Sql_mode)
		}
	}
}

func initialize_sql_statement(statement *GString) {
	G_string_printf(statement, headers.Str.String())
}

func initialize_headers() {
	headers = G_string_sized_new(100)
	initialize_header_in_gstring(headers, SetNamesInFileByDefault)
}
func set_tidb_snapshot(conn *DBConnection) {
	var query string = fmt.Sprintf("SET SESSION tidb_snapshot = '%s'", TidbSnapshot)
	M_query_critical(conn, query, "Failed to set tidb_snapshot (It could be related to https://github.com/pingcap/tidb/issues/8887)")
}

func my_pow_two_plus_prev(prev uint64, max uint) uint64 {
	var r uint64 = 1
	var i uint
	for i = 0; i < max; i++ {
		r *= 2
	}
	return r + prev
}

func parse_rows_per_chunk(rows_p_chunk string, min *uint64, start *uint64, max *uint64, message string) bool {
	if strings.HasPrefix(rows_p_chunk, "-") {
		return false
	}
	var split = strings.Split(rows_p_chunk, ":")
	var err error
	switch len(split) {
	case 0:
		log.Critical(message)
		return false
	case 1:
		*start, err = strconv.ParseUint(split[0], 10, 64)
		*min = *start
		*max = *start
		break
	case 2:
		*min, err = strconv.ParseUint(split[0], 10, 64)
		*start, err = strconv.ParseUint(split[1], 10, 64)
		*max = *start
		break
	default:
		*min, err = strconv.ParseUint(split[0], 10, 64)
		*start, err = strconv.ParseUint(split[1], 10, 64)
		*max, err = strconv.ParseUint(split[2], 10, 64)
		break
	}
	_ = err
	return true
}
func is_empty_dir(directory string) bool {
	dir, err := os.Stat(directory)
	if err != nil {
		log.Criticalf("cannot open directory %s, %v", directory, err)
		Errors++
		return false
	}
	if dir.IsDir() {
		openDir, _ := os.Open(directory)
		defer openDir.Close()
		filename, _ := openDir.ReadDir(-1)
		if len(filename) > 0 {
			return false
		}
		return true
	}
	log.Errorf("%s is file", directory)
	return false
}
