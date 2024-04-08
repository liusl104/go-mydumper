package mydumper

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"strings"
	"sync"
)

var m_close func(o *OptionEntries, thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error

type file_write struct {
	write write_fun
	close close_fun
	flush flush_fun
}

func g_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

func initialize_common(o *OptionEntries) {
	o.global.available_pids = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	var i uint
	for i = 0; i < (o.Common.NumThreads * 2); i++ {
		release_pid(o)
	}
	o.global.ref_table_mutex = g_mutex_new()
	o.global.ref_table = make(map[string]string)
}

func free_common(o *OptionEntries) {
	o.global.ref_table_mutex = nil
	o.global.ref_table = nil
}

func release_pid(o *OptionEntries) {
	o.global.available_pids.push(1)
}

func execute_file_per_thread(sql_fn string, sql_fn3 string) int {
	// TODO
	/*
	 // 创建一个文件用于保存压缩后的数据
	    outFile, err := os.Create("list.gz")
	    if err != nil {
	        panic(err)
	    }
	    defer outFile.Close()

	    // 创建ls命令
	    lsCmd := exec.Command("ls")

	    // 创建gzip命令
	    gzipCmd := exec.Command("gzip", "-c")

	    // 设置gzip命令的标准输出到文件
	    gzipCmd.Stdout = outFile

	    // 创建管道
	    pipeReader, pipeWriter := io.Pipe()

	    // 设置ls命令的标准输出到管道的写入端
	    lsCmd.Stdout = pipeWriter
	    // 设置gzip命令的标准输入为管道的读取端
	    gzipCmd.Stdin = pipeReader

	    // 开始执行ls命令
	    if err := lsCmd.Start(); err != nil {
	        panic(err)
	    }

	    // 开始执行gzip命令
	    if err := gzipCmd.Start(); err != nil {
	        panic(err)
	    }

	    // 等待ls命令完成，并关闭管道的写入端
	    if err := lsCmd.Wait(); err != nil {
	        panic(err)
	    }
	    pipeWriter.Close()

	    // 等待gzip命令完成
	    if err := gzipCmd.Wait(); err != nil {
	        panic(err)
	    }*/
	return 0
}

func m_open_pipe(o *OptionEntries, filename *string, mode string) (*file_write, error) {
	*filename = fmt.Sprintf("%s%s", *filename, o.Exec.ExecPerThreadExtension)
	var flag int
	if strings.ToLower(mode) == "w" {
		flag = os.O_CREATE | os.O_WRONLY
	} else if strings.ToLower(mode) == "r" {
		flag = os.O_RDONLY
	}
	file, err := os.OpenFile(*filename, flag, 0660)
	if err != nil {
		log.Fatalf("open file %s fail:%v", *filename, err)
	}
	var compressFile *gzip.Writer
	var compressEncode *zstd.Encoder
	var f = new(file_write)
	switch strings.ToUpper(o.Extra.CompressMethod) {
	case GZIP:
		compressFile, err = gzip.NewWriterLevel(file, gzip.DefaultCompression)
		if err != nil {
			return nil, err
		}
		f.flush = compressFile.Flush
		f.write = compressFile.Write
		f.close = compressFile.Close
		return f, err
	case ZSTD:
		compressEncode, err = zstd.NewWriter(file)
		if err != nil {
			return nil, err
		}
		f.flush = compressEncode.Flush
		f.write = compressEncode.Write
		f.close = compressEncode.Close
		return f, err

	default:
		if err != nil {
			return nil, err
		}

		f.flush = file.Sync
		f.write = file.Write
		f.close = file.Close
		return f, err
	}

}

func m_close_pipe(o *OptionEntries, thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	release_pid(o)
	var err error
	err = file.close()
	if size > 0 {
		if o.Stream.Stream {
			stream_queue_push(o, dbt, "")
		}
	} else if !o.Extra.BuildEmptyFiles {
		err = os.Remove(filename)
		if err != nil {
			log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}

	return err
}

// o *OptionEntries, thread_id uint, file *os.File, filename string, size uint, dbt *db_table
func m_close_file(o *OptionEntries, thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	var err error
	err = file.close()
	if size > 0 {
		if o.Stream.Stream {
			stream_queue_push(o, dbt, filename)
		}
	} else if !o.Extra.BuildEmptyFiles {
		err = os.Remove(filename)
		if err != nil {
			log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}
	return err
}

func determine_filename(o *OptionEntries, table string) string {
	if check_filename_regex(o, table) && !strings.Contains(table, ".") && !strings.HasSuffix(table, "mydumper_") {
		return table
	} else {
		r := fmt.Sprintf("mydumper_%d", o.global.table_number)
		o.global.table_number++
		return r
	}
}

func escape_string(str string) string {
	return mysql.Escape(str)
}

func build_schema_table_filename(dump_directory string, database string, table string, suffix string) string {
	var filename string
	filename = fmt.Sprintf("%s.%s-%s.sql", database, table, suffix)
	r := path.Join(dump_directory, filename)
	return r
}

func build_schema_filename(dump_directory, database, suffix string) string {
	var filename string
	filename = fmt.Sprintf("%s-%s.sql", database, suffix)
	r := path.Join(dump_directory, filename)
	return r
}

func build_tablespace_filename(dump_directory string) string {
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

func set_charset(statement *string, character_set []byte, collation_connection []byte) {
	*statement = "SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n"
	*statement += "SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n"
	*statement += "SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n"
	*statement += fmt.Sprintf("SET character_set_client = %s;\n", character_set)
	*statement += fmt.Sprintf("SET character_set_results = %s;\n", character_set)
	*statement += fmt.Sprintf("SET collation_connection = %s;\n", collation_connection)

}

func restore_charset(statement *string) {
	*statement += "SET character_set_client = @PREV_CHARACTER_SET_CLIENT;\n"
	*statement += "SET character_set_results = @PREV_CHARACTER_SET_RESULTS;\n"
	*statement += "SET collation_connection = @PREV_COLLATION_CONNECTION;\n"
}

func clear_dump_directory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		log.Errorf("cannot open directory %s, %v", directory, err)
		return err
	}
	filename, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, file := range filename {
		file_path := path.Join(directory, file)
		err = os.Remove(file_path)
		log.Errorf("error removing file %s (%v)", file_path, err)
		return err
	}
	return nil
}

func set_transaction_isolation_level_repeatable_read(conn *client.Conn) error {
	_, err := conn.Execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		log.Errorf("Failed to set isolation level: %v", err)
	}
	return err
}

func build_filename(dump_directory, database string, table string, part uint64, sub_part uint, extension string, second_extension string) string {
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

func build_data_filename(dump_directory, database, table string, part uint64, sub_part uint) string {
	return build_filename(dump_directory, database, table, part, sub_part, "sql", "")
}

func build_fifo_filename(dump_directory, database, table string, part uint64, sub_part uint, extension string) string {
	return build_filename(dump_directory, database, table, part, sub_part, extension, "fifo")
}

func build_stdout_filename(dump_directory, database, table string, part uint64, sub_part uint, extension string, second_extension string) string {
	return build_filename(dump_directory, database, table, part, sub_part, extension, second_extension)
}

func build_load_data_filename(dump_directory, database, table string, part uint64, sub_part uint) string {
	return build_filename(dump_directory, database, table, part, sub_part, "dat", "")
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

func m_escape_char_with_char(needle, repl []byte, data []byte) []byte {
	var buffer bytes.Buffer // 使用 bytes.Buffer 来高效构建字符串
	for _, b := range data {
		if b == needle[0] {
			buffer.WriteByte(repl[0]) // 替换字符
		} else {
			buffer.WriteByte(b) // 原样输出字符
		}
	}

	return buffer.Bytes() // 返回结果
}

func m_replace_char_with_char(needle rune, repl rune, from []rune) string {
	for i := 0; i < len(from); i++ {
		if from[i] == needle {
			from[i] = repl
			i++
		}
	}
	return string(from)
}

func determine_show_table_status_columns(result []*mysql.Field, ecol *int, ccol *int, collcol *int, rowscol *int) {
	var i int
	for i = 0; i < len(result); i++ {
		if strings.ToLower(string(result[i].Name)) == strings.ToLower("Engine") {
			*ecol = i
		} else if strings.ToLower(string(result[i].Name)) == strings.ToLower("Comment") {
			*ccol = i
		} else if strings.ToLower(string(result[i].Name)) == strings.ToLower("Collation") {
			*collcol = i
		} else if strings.ToLower(string(result[i].Name)) == strings.ToLower("Rows") {
			*rowscol = i
		}
	}
}

func initialize_sql_statement(o *OptionEntries, statement *string) {
	if o.is_mysql_like() {
		if o.global.set_names_statement != "" {
			*statement = fmt.Sprintf("%s;\n", o.global.set_names_statement)
		}
		*statement += "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n"
		if !o.Statement.SkipTz {
			*statement += fmt.Sprintf("/*!40103 SET TIME_ZONE='+00:00' */;\n")
		}
	} else if o.global.detected_server == SERVER_TYPE_TIDB {
		if !o.Statement.SkipTz {
			*statement = fmt.Sprintf("/*!40103 SET TIME_ZONE='+00:00' */;\n")
		}
	} else {
		*statement = "SET FOREIGN_KEY_CHECKS=0;\n"
	}
}

func set_tidb_snapshot(o *OptionEntries, conn *client.Conn) error {
	var query string = fmt.Sprintf("SET SESSION tidb_snapshot = '%s'", o.Lock.TidbSnapshot)
	_, err := conn.Execute(query)
	if err != nil {
		log.Errorf("Failed to set tidb_snapshot: %v.\nThis might be related to https://github.com/pingcap/tidb/issues/8887", err)
		return err
	}
	return nil
}
