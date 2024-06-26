package myloader

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

const (
	IS_INNODB_TABLE        = 2
	INCLUDE_CONSTRAINT     = 4
	IS_ALTER_TABLE_PRESENT = 8
)

func initialize_common(o *OptionEntries) {
	o.global.db_hash_mutex = new(sync.Mutex)
	o.global.tbl_hash = make(map[string]string)
}

func get_value(kf *ini.File, group string, key string) string {
	section := kf.Section(group)
	if !section.HasKey(key) {
		return ""
	}
	value, _ := section.GetKey(key)
	return value.Value()
}

func change_master(kf *ini.File, group string, output_statement *string) {
	var val string
	var i uint
	var length int
	var err error
	var s string
	var group_name = strings.SplitN(group, ".", 2)
	var channel_name string
	if len(group_name) > 1 {
		channel_name = group_name[1]
	}

	var keys = kf.Section(group).Keys()
	length = len(keys)
	var exec_change_master, exec_reset_slave, exec_start_slave int
	s += "CHANGE MASTER TO "
	for i = 0; i < uint(length); i++ {
		if keys[i].Name() == "myloader_exec_reset_slave" {
			exec_reset_slave, err = strconv.Atoi(keys[i].Value())
		} else if keys[i].Name() == "myloader_exec_change_master" {
			exec_change_master, err = strconv.Atoi(keys[i].Value())
		} else if keys[i].Name() == "myloader_exec_start_slave" {
			exec_start_slave, err = strconv.Atoi(keys[i].Value())
		} else if keys[i].Name() == "channel_name" {
			channel_name = keys[i].Value()
		} else {
			val = keys[i].Value()
			if val != "" {
				s += fmt.Sprintf("%s = %s, ", keys[i].Name(), val)
			}
		}
	}
	_ = err
	s += " FOR CHANNEL "
	if channel_name == "" {
		s += "''"
	} else {
		s += channel_name
	}
	s += ";\n"
	if exec_change_master != 0 {
		if exec_reset_slave != 0 {
			*output_statement += "STOP SLAVE ;\nRESET SLAVE "
			if exec_reset_slave > 1 {
				*output_statement += "ALL "
			}
			if channel_name != "" {
				*output_statement += fmt.Sprintf("FOR CHANNEL %s ;\n", channel_name)
			}
			*output_statement += ";\n"
		}
		*output_statement += s
		if exec_start_slave != 0 {
			*output_statement += "START SLAVE;\n"
		}
		if channel_name != "" {
			log.Infof("Change master will be executed for channel: %s", channel_name)
		} else {
			log.Infof("Change master will be executed for channel: default channel")
		}
	}
}

func m_filename_has_suffix(o *OptionEntries, str string, suffix string) bool {
	if has_exec_per_thread_extension(o, str) {
		return strings.ToLower(path.Ext(str)) == o.Threads.ExecPerThreadExtension
	} else if strings.HasSuffix(str, GZIP_EXTENSION) {
		return strings.HasSuffix(path.Ext(str[:len(str)-len(GZIP_EXTENSION)]), suffix)
	} else if strings.HasSuffix(str, ZSTD_EXTENSION) {
		strings.HasSuffix(path.Ext(str[:len(str)-len(ZSTD_EXTENSION)]), suffix)
	}
	return strings.HasSuffix(str, suffix)
}

func new_database(o *OptionEntries, db_name string) *database {
	var d = new(database)
	d.name = db_name
	if o.Common.DB != "" {
		d.real_database = o.Common.DB
	} else {
		d.real_database = db_name
	}
	d.mutex = g_mutex_new()
	d.queue = g_async_queue_new(o.Common.BufferSize)
	d.schema_state = NOT_FOUND
	d.schema_checksum = ""
	d.post_checksum = ""
	d.triggers_checksum = ""
	return d
}

func get_db_hash(o *OptionEntries, k, v string) *database {
	o.global.db_hash_mutex.Lock()
	d, _ := o.global.db_hash[k]
	if d == nil {
		d = new_database(o, v)
		o.global.db_hash[k] = d
	}
	o.global.db_hash_mutex.Unlock()
	return d
}

func eval_table(o *OptionEntries, db_name string, table_name string, mutex *sync.Mutex) bool {
	if table_name == "" {
		log.Errorf("Table name is null on eval_table()")
	}
	mutex.Lock()
	if len(o.global.tables) > 0 {
		if !is_table_in_list(table_name, o.global.tables) {
			mutex.Unlock()
			return false
		}
	}
	if o.Filter.TablesSkiplistFile != "" && check_skiplist(o, db_name, table_name) {
		mutex.Unlock()
		return false
	}
	mutex.Unlock()
	return eval_regex(o, db_name, table_name)
}

func execute_use(td *thread_data) bool {
	var query = fmt.Sprintf("USE `%s`", td.current_database)
	_, err := td.thrconn.Execute(query)
	if err != nil {
		return true
	}
	return false
}

func execute_use_if_needs_to(o *OptionEntries, td *thread_data, database string, msg string) {
	if database != "" && o.Common.DB == "" {
		if td.current_database == "" || strings.Compare(database, td.current_database) != 0 {
			td.current_database = database
			if execute_use(td) {
				log.Fatalf(fmt.Sprintf("Thread %d: Error switching to database `%s` %s", td.thread_id, td.current_database, msg))
			}
		}
	}
	return
}

func m_query(conn *client.Conn, query string, log_fun func(str string), fmt string) bool {
	_, err := conn.Execute(query)
	if err != nil {
		log_fun(fmt)
		return false
	}
	return true
}

func get_file_type(o *OptionEntries, filename string) file_type {
	if strings.Compare(filename, "metadata") == 0 || strings.Contains(filename, "metadata.partial") {
		return METADATA_GLOBAL
	}
	if o.Filter.SourceDb != "" && !strings.HasPrefix(filename, o.Filter.SourceDb) {
		return IGNORED
	}
	if m_filename_has_suffix(o, filename, "-schema.sql") {
		return SCHEMA_TABLE
	}
	if strings.Compare(filename, "all-schema-create-tablespace.sql") == 0 {
		return SCHEMA_TABLESPACE
	}
	if strings.Compare(filename, "resume") == 0 {
		if !o.Common.Resume {
			log.Fatalf("resume file found, but no --resume option passed. Use --resume or remove it and restart process if you consider that it will be safe.")
		}
		return RESUME
	}
	if strings.Compare(filename, "resume.partial") == 0 {
		log.Fatalf("\"resume.partial file found. Remove it and restart process if you consider that it will be safe.")
	}

	if m_filename_has_suffix(o, filename, "-checksum") {
		return CHECKSUM
	}

	if m_filename_has_suffix(o, filename, "-schema-view.sql") {
		return SCHEMA_VIEW
	}

	if m_filename_has_suffix(o, filename, "-schema-sequence.sql") {
		return SCHEMA_SEQUENCE
	}

	if m_filename_has_suffix(o, filename, "-schema-triggers.sql") {
		return SCHEMA_TRIGGER
	}

	if m_filename_has_suffix(o, filename, "-schema-post.sql") {
		return SCHEMA_POST
	}

	if m_filename_has_suffix(o, filename, "-schema-create.sql") {
		return SCHEMA_CREATE
	}

	if m_filename_has_suffix(o, filename, ".sql") {
		return DATA
	}

	if m_filename_has_suffix(o, filename, ".dat") {
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

func build_dbt_key(a, b string) string {
	return fmt.Sprintf("`%s`_`%s`", a, b)
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

func refresh_table_list_without_table_hash_lock(conf *configuration) {
	var table_list []*db_table
	conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range conf.table_hash {
		table_list = append(table_list, dbt)
	}
	conf.table_list = table_list
	conf.table_list_mutex.Unlock()
}

func refresh_table_list(conf *configuration) {
	conf.table_hash_mutex.Lock()
	refresh_table_list_without_table_hash_lock(conf)
	conf.table_hash_mutex.Unlock()
}

func checksum_dbt_template(dbt *db_table, dbt_checksum string, conn *client.Conn, message string, fun check_sum) {
	checksum, _ := fun(conn, dbt.database.name, dbt.real_table)
	if dbt_checksum != checksum {
		log.Warnf("%s mismatch found for `%s`.`%s`. Got '%s', expecting '%s'", message, dbt.database.name, dbt.table, dbt_checksum, checksum)
	} else {
		log.Infof("%s confirmed for `%s`.`%s`", message, dbt.database.name, dbt.table)
	}
}

func checksum_database_template(database string, dbt_checksum string, conn *client.Conn, message string, fun check_sum) {
	checksum, _ := fun(conn, database, "")
	if dbt_checksum != checksum {
		log.Warnf("%s mismatch found for `%s`. Got '%s', expecting '%s'", message, database, dbt_checksum, checksum)
	} else {
		log.Infof("%s confirmed for `%s`", message, database)
	}
}

func checksum_dbt(dbt *db_table, conn *client.Conn) {
	if dbt.schema_checksum != "" {
		if dbt.is_view {
			checksum_dbt_template(dbt, dbt.schema_checksum, conn, "View checksum", checksum_view_structure)
		} else {
			checksum_dbt_template(dbt, dbt.schema_checksum, conn, "Structure checksum", checksum_table_structure)
		}

	}
	if dbt.triggers_checksum != "" {
		checksum_dbt_template(dbt, dbt.triggers_checksum, conn, "Trigger checksum", checksum_trigger_structure)
	}
	if dbt.indexes_checksum != "" {
		checksum_dbt_template(dbt, dbt.indexes_checksum, conn, "Schema index checksum", checksum_table_indexes)
	}
	if dbt.data_checksum != "" {
		checksum_dbt_template(dbt, dbt.data_checksum, conn, "Checksum", checksum_table)
	}
}

func has_exec_per_thread_extension(o *OptionEntries, filename string) bool {
	return o.Threads.ExecPerThreadExtension != "" && strings.HasSuffix(filename, o.Threads.ExecPerThreadExtension)
}

func execute_file_per_thread(sql_fn string, sql_fn3 string, exec string) *osFile {
	var sql_file2 *os.File
	var sql_file3 *os.File
	var outfile *osFile
	var err error
	sql_file2, err = os.Open(sql_fn)
	if err != nil {
		log.Fatalf("fail open file name: %s ", sql_fn)
		return nil
	}
	sql_file3, err = os.OpenFile(sql_fn3, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	if err != nil {
		log.Fatalf("fail open fifo file name: %s ", sql_fn3)
		return nil
	}
	_ = sql_file3
	switch exec {
	case GZIP_EXTENSION:
		var out *gzip.Reader
		out, err = gzip.NewReader(sql_file2)
		outfile = new(osFile)
		outfile.close = out.Close
		outfile.writerTo = out.WriteTo
		outfile.metadata = out.Name
		outfile.read = out.Read

	case ZSTD_EXTENSION:
		var out *zstd.Decoder
		out, err = zstd.NewReader(sql_file2)
		outfile = new(osFile)
		outfile.close = func() error {
			out.Close()
			return nil
		}
		outfile.writerTo = out.WriteTo
		outfile.metadata = ""
		outfile.read = out.Read
	}
	var size int64
	size, err = outfile.writerTo(sql_file3)
	if err != nil {
		log.Fatalf("fail write %s file : %v ", sql_fn3, err)
	}
	log.Debugf("size after decompression file %s size: %d", sql_fn3, size)
	return outfile
}

func get_command_and_basename(o *OptionEntries, filename string, basename *string) bool {
	var length int
	if has_exec_per_thread_extension(o, filename) {
		length = len(o.Threads.ExecPerThreadExtension)
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
