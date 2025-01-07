package myloader

import (
	"bufio"
	"fmt"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type osFile struct {
	file     *os.File
	write    func(w []byte) (int, error)
	read     func(p []byte) (int, error)
	sync     func()
	close    func() error
	metadata string
	writerTo func(o io.Writer) (int64, error)
}

type fifo struct {
	status          *sync.WaitGroup
	pid             *osFile
	filename        string
	stdout_filename string
	mutex           *sync.Mutex
	size            int64
}

var (
	conf                    *configuration
	fifo_hash               map[*os.File](*fifo)
	fifo_table_mutex        *sync.Mutex
	append_if_not_exist     bool
	change_master_statement *GString
	schema_sequence_fix     bool
)

func initialize_process(c *configuration) {
	conf = c
	fifo_hash = make(map[*os.File]*fifo)
	fifo_table_mutex = G_mutex_new()
}

func append_new_db_table(real_db_name *database, table string, number_rows uint64, alter_table_statement *GString) *db_table {
	var lkey = build_dbt_key(real_db_name.filename, table)
	var dbt *db_table = conf.table_hash[lkey]
	if dbt == nil {
		conf.table_hash_mutex.Lock()
		dbt = conf.table_hash[lkey]
		if dbt == nil {
			dbt = new(db_table)
			dbt.database = real_db_name
			dbt.table = table
			dbt.real_table = dbt.table
			dbt.rows = number_rows
			dbt.restore_job_list = nil
			Parse_object_to_export(dbt.object_to_export, conf_per_table.All_object_to_export[lkey])
			dbt.current_threads = 0
			if MaxThreadsPerTable > NumThreads {
				dbt.max_threads = NumThreads
			} else {
				dbt.max_threads = MaxThreadsPerTable
			}
			dbt.max_connections_per_job = 0
			dbt.retry_count = RetryCount
			dbt.mutex = G_mutex_new()
			dbt.indexes = alter_table_statement
			dbt.schema_state = NOT_FOUND
			dbt.index_enqueued = false
			dbt.remaining_jobs = 0
			dbt.constraints = nil
			dbt.count = 0
			conf.table_hash[lkey] = dbt
			refresh_table_list_without_table_hash_lock(conf, false)
			dbt.schema_checksum = ""
			dbt.triggers_checksum = ""
			dbt.indexes_checksum = ""
			dbt.data_checksum = ""
			dbt.is_view = false
			dbt.is_sequence = false
		} else {
			if number_rows > 0 {
				dbt.rows = number_rows
			}
			if alter_table_statement != nil {
				dbt.indexes = alter_table_statement
			}
		}
		conf.table_hash_mutex.Unlock()
	} else {
		if number_rows > 0 {
			dbt.rows = number_rows
		}
		if alter_table_statement != nil {
			dbt.indexes = alter_table_statement
		}
	}
	return dbt
}

func free_dbt(dbt *db_table) {
	dbt.table = ""
	dbt.constraints = nil
	dbt.mutex = nil
}

func free_table_hash(table_hash map[string]*db_table) {
	conf.table_hash_mutex.Lock()
	var lkey string
	if table_hash != nil {
		for lkey, _ = range table_hash {
			delete(table_hash, lkey)
		}
	}
	conf.table_hash_mutex.Unlock()
}

func myl_open(filename string, mode int) (*osFile, error) {
	var file *osFile
	var err error
	mode = os.O_RDONLY
	file, err = execute_file_per_thread(filename, ExecPerThreadExtension)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		return nil, err
	}
	return file, nil
}

func myl_close(filename string, file *osFile, rm bool) {
	fifo_table_mutex.Lock()
	fifo_table_mutex.Unlock()
	_ = filename
	_ = file.close()
}

func load_schema(dbt *db_table, filename string) *control_job {
	var infile *osFile
	var data *GString = new(GString)
	var eof bool
	var line int
	var err error
	var create_table_statement *GString = new(GString)
	infile, err = myl_open(filename, os.O_RDONLY)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		errors++
		return nil
	}
	var reader = bufio.NewScanner(infile.file)
	for eof == false {
		if Read_data(reader, data, &eof, &line) {
			var length int
			if data.Len >= 5 {
				length = data.Len - 5
			}
			if strings.Contains(data.Str.String()[length:], ";\n") {
				if data.Str.String()[:13] == "CREATE TABLE " {
					if !strings.Contains(data.Str.String()[:30], Identifier_quote_character_str) {
						log.Errorf("Identifier quote character (%s) not found on %s. Review file and configure --identifier-quote-character properly", Identifier_quote_character_str, filename)
						return nil
					}
					var expr = fmt.Sprintf("CREATE\\s+TABLE\\s+[^%s]*%s(.+?)%s\\s*\\(", Identifier_quote_character, Identifier_quote_character, Identifier_quote_character)
					var matchInfo *regexp.Regexp
					matchInfo, err = regexp.Compile(expr)
					if err != nil {
						log.Errorf("Cannot parse real table name from CREATE TABLE statement: %v", err)
						return nil
					}
					dbt.real_table = matchInfo.FindString(data.Str.String())
					if dbt.real_table == "" {
						log.Errorf("Cannot parse real table name from CREATE TABLE statement: %s", data.Str.String())
						return nil
					}
					if strings.HasPrefix(dbt.table, "mydumper_") {
						tbl_hash[dbt.table] = dbt.real_table
					} else {
						tbl_hash[dbt.real_table] = dbt.real_table
					}
					if append_if_not_exist {
						if strings.HasPrefix(data.Str.String(), "CREATE TABLE ") && strings.HasPrefix(data.Str.String(), "CREATE TABLE IF") {
							var tmp_data string
							tmp_data += "CREATE TABLE IF NOT EXISTS "
							tmp_data += data.Str.String()[13:]
							data.Str.Reset()
							data.Str.WriteString(tmp_data)

						}
					}
				}
				if InnodbOptimizeKeys != "" || SkipConstraints || SkipIndexes {
					var alter_table_statement, alter_table_constraint_statement *GString = new(GString), new(GString)
					if strings.HasPrefix(data.Str.String(), "/*!40") {
						alter_table_statement = data
						create_table_statement = data
					} else {
						var new_create_table_statement *GString = new(GString)
						var flag = process_create_table_statement(data, new_create_table_statement, alter_table_statement, alter_table_constraint_statement, dbt, dbt.rows == 0 || dbt.rows >= 1000000 || SkipConstraints || SkipIndexes)
						if (flag & IS_INNODB_TABLE) != 0 {
							if (flag & IS_ALTER_TABLE_PRESENT) != 0 {
								log.Infof("Fast index creation will be use for table: %s.%s", dbt.database.real_database, dbt.real_table)
							} else {
								alter_table_statement = nil
							}
							if !SkipIndexes {
								if InnodbOptimizeKeys != "" {
									dbt.indexes = alter_table_statement
								} else if alter_table_statement != nil {
									G_string_append(create_table_statement, alter_table_statement.Str.String())
								}
							}
							if !SkipConstraints && flag&INCLUDE_CONSTRAINT != 0 {
								var rj *restore_job = new_schema_restore_job(filename, JOB_RESTORE_STRING, dbt, dbt.database, alter_table_constraint_statement, CONSTRAINTS)
								G_async_queue_push(conf.post_table_queue, new_control_job(JOB_RESTORE, rj, dbt.database))
								dbt.constraints = alter_table_constraint_statement
							} else {
								alter_table_constraint_statement = nil
							}
							G_string_set_size(data, 0)
						} else {
							alter_table_statement = nil
							alter_table_constraint_statement = nil
							G_string_set_size(create_table_statement, 0)
							G_string_append(create_table_statement, data.Str.String())
						}
					}
				} else {
					G_string_append(create_table_statement, data.Str.String())
				}
				G_string_set_size(data, 0)
			}
		}
	}

	if schema_sequence_fix {
		var statement = Filter_sequence_schemas(create_table_statement.Str.String())
		G_string_assign(create_table_statement, statement)
	}
	var rj *restore_job = new_schema_restore_job(filename, JOB_TO_CREATE_TABLE, dbt, dbt.database, create_table_statement, "")
	var cj *control_job = new_control_job(JOB_RESTORE, rj, dbt.database)
	myl_close(filename, infile, true)
	return cj
}

func get_database_table_part_name_from_filename(filename string, database *string, table *string, part *uint, sub_part *uint) {
	var split_db_tbl = strings.SplitN(filename, ".", 4)
	if len(split_db_tbl) > 2 {
		*database = split_db_tbl[0]
		*table = split_db_tbl[1]
		if len(split_db_tbl) >= 3 {
			t, _ := strconv.Atoi(split_db_tbl[2])
			*part = uint(t)
		} else {
			*part = 0
		}
		if len(split_db_tbl) > 3 {
			t, _ := strconv.Atoi(split_db_tbl[3])
			*sub_part = uint(t)
		}
	} else {
		*database = ""
		*table = ""
		*part = 0
		*sub_part = 0
	}
	return
}

func get_database_name_from_filename(filename string) string {
	var split_file = strings.SplitN(filename, "-schema-create.sql", 2)
	var db_name = split_file[0]
	return db_name
}

func get_database_table_name_from_filename(filename string, suffix string, database *string, table *string) {
	var split_file = strings.SplitN(filename, suffix, 2)
	var split_db_tbl = strings.Split(split_file[0], ".")
	if len(split_db_tbl) == 2 {
		*database = split_db_tbl[0]
		*table = split_db_tbl[1]
	} else {
		*database = ""
		*table = ""
	}

}

func get_database_name_from_content(filename string) string {
	var infile *osFile
	var err error
	infile, err = myl_open(filename, os.O_RDONLY)
	if err != nil {
		log.Criticalf("cannot open database schema file %s (%v)", filename, err)
		errors++
		return ""
	}
	var eof bool
	var data *GString = new(GString)
	var line int
	var real_database string
	var reader = bufio.NewScanner(infile.file)

	for eof == false {
		if Read_data(reader, data, &eof, &line) {
			if data.Len >= 5 {
				if strings.Contains(data.Str.String()[:data.Len-5], ";\n") {
					if strings.HasPrefix(data.Str.String(), "CREATE ") {
						var create = strings.SplitN(data.Str.String(), Identifier_quote_character_str, 3)
						real_database = create[1]
						break
					} else {
						G_string_set_size(data, 0)
					}
				}
			}
		}
	}

	myl_close(filename, infile, false)
	return real_database
}

func process_tablespace_filename(filename string) {
	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, nil, nil, TABLESPACE)
	G_async_queue_push(conf.database_queue, new_control_job(JOB_RESTORE, rj, nil))
}

func process_database_filename(filename string) {
	var db_kname, db_vname string
	db_kname = get_database_name_from_filename(filename)
	db_vname = db_kname

	if db_kname != "" {

		if strings.HasPrefix(db_kname, "mydumper_") {
			db_vname = get_database_name_from_content(path.Join(directory, filename))
			if db_vname == "" {
				log.Criticalf("It was not possible to process db content in file: %s", filename)
			}
		}

	} else {
		log.Criticalf("It was not possible to process db file: %s", filename)
	}

	log.Tracef("Adding database: %s -> %s", db_kname, db_vname)
	var real_db_name *database = get_db_hash(db_kname, db_vname)
	if DB == "" {
		real_db_name.schema_state = NOT_CREATED
		var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, nil, CREATE_DATABASE)
		G_async_queue_push(conf.database_queue, new_control_job(JOB_RESTORE, rj, nil))
	} else {
		real_db_name.schema_state = CREATED
	}
}

func process_table_filename(filename string) bool {
	var db_name, table_name string
	var dbt *db_table
	get_database_table_name_from_filename(filename, "-schema.sql", &db_name, &table_name)
	if db_name == "" || table_name == "" {
		log.Criticalf("It was not possible to process file: %s (1)", filename)
	}
	var real_db_name *database = get_db_hash(db_name, db_name)
	if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
		log.Warnf("Skiping table: `%s`.`%s`", real_db_name.name, table_name)
		return false
	}
	dbt = append_new_db_table(real_db_name, table_name, 0, nil)
	dbt.schema_state = NOT_CREATED
	var cj *control_job = load_schema(dbt, path.Join(directory, filename, ""))
	if cj == nil {
		return false
	}
	real_db_name.mutex.Lock()
	if real_db_name.schema_state != CREATED || sequences_processed < sequences {
		G_async_queue_push(real_db_name.queue, cj)
		real_db_name.mutex.Unlock()
		return false
	} else {
		if cj != nil {
			log.Tracef("table_queue <- %v: %s", cj.data.restore_job.job_type, filename)
			G_async_queue_push(conf.table_queue, cj)
		}
	}
	real_db_name.mutex.Unlock()
	return true
}

func process_metadata_global(file string) {
	var pt = path.Join(directory, file)
	var kf = Load_config_file(pt)
	if kf == nil {
		log.Errorf("Global metadata file processing was not possible")
	}
	log.Infof("Reading metadata: %s", file)
	var j uint
	var err error
	var value string
	var length uint
	var real_table_name string
	// gchar **groups=g_key_file_get_groups(kf, &length);
	var database_table []string
	var groups []string = kf.SectionStrings()
	length = uint(len(groups))
	var dbt *db_table
	change_master_statement = G_string_new("")
	var delim_bt = "`.`"
	var delim_dq = "\".\""
	var delimiter string
	var wrong_quote string
	if Identifier_quote_character == BACKTICK {
		delimiter = delim_bt
		wrong_quote = "\""
	} else {
		delimiter = delim_dq
		wrong_quote = "`"
	}
	for j = 0; j < length; j++ {
		var group = groups[j]
		if strings.HasPrefix(group, "config") {
			if j > 0 {
				log.Critical("Wrong metadata: [config] group must be first")
			}
			value = get_value(kf, "config", "quote_character")
			if value != "" {
				if strings.EqualFold(value, "BACKTICK") {
					Identifier_quote_character = BACKTICK
					Identifier_quote_character_str = "`"
					wrong_quote = "\""
					delimiter = delim_bt
				} else if strings.EqualFold(value, "DOUBLE_QUOTE") {
					Identifier_quote_character = DOUBLE_QUOTE
					Identifier_quote_character_str = "\""
					delimiter = delim_dq
					wrong_quote = "`"
				} else {
					log.Criticalf("Wrong quote_character = %s in metadata", value)
				}
				log.Tracef("metadata: quote character is %v", Identifier_quote_character)
			}
		} else if strings.HasPrefix(group, wrong_quote) {
			log.Errorf("metadata is broken: group %s has wrong quoting: %s; must be: %s", group, wrong_quote, Identifier_quote_character)
		} else if strings.HasPrefix(group, Identifier_quote_character) {
			database_table = strings.SplitN(group, delimiter, 2)
			if database_table[1] != "" {
				// database_table[1][strlen(database_table[1])-1]='\0'
				database_table[1] = ""
				if SourceDb == "" || strings.Compare(database_table[1], SourceDb) == 0 {
					var real_db_name = get_db_hash(database_table[0], database_table[0])
					dbt = append_new_db_table(real_db_name, database_table[1], 0, nil)
					if !dbt.object_to_export.No_data {
						dbt.data_checksum = get_value(kf, group, "data_checksum")
					}
					if !dbt.object_to_export.No_schema {
						dbt.schema_checksum = get_value(kf, group, "schema_checksum")
					}
					if !dbt.object_to_export.No_schema {
						dbt.indexes_checksum = get_value(kf, group, "indexes_checksum")
					}
					if !dbt.object_to_export.No_trigger {
						dbt.triggers_checksum = get_value(kf, group, "triggers_checksum")
					}
					value = get_value(kf, group, "is_view")
					if value != "" && strings.Compare(value, "1") == 0 {
						dbt.is_view = true
					}
					value = get_value(kf, group, "is_sequence")
					if value != "" && strings.Compare(value, "1") == 0 {
						dbt.is_sequence = true
						sequences++
					}
					value = get_value(kf, group, "rows")
					if value != "0" {
						dbt.rows, err = strconv.ParseUint(value, 10, 64)
						if err != nil {
							log.Criticalf("Error parsing rows: %v", err)
						}
					}
					value = get_value(kf, group, "real_table_name")
					if value != "" {
						real_table_name = value
						if strings.Compare(dbt.real_table, real_table_name) != 0 {
							dbt.real_table = real_table_name
						}
					}
				}
			} else {
				database_table[0] = ""
				var database *database = get_db_hash(database_table[0], database_table[0])
				database.schema_checksum = get_value(kf, group, "schema_checksum")
				database.post_checksum = get_value(kf, group, "post_checksum")
				database.triggers_checksum = get_value(kf, group, "triggers_checksum")
			}
		} else if strings.HasPrefix(group, "replication") {
			change_master(kf, group, change_master_statement)
		} else if strings.HasPrefix(group, "master") || strings.HasPrefix(group, "source") {
			change_master(kf, group, change_master_statement)
		} else if strings.HasPrefix(group, "myloader_session_variables") {
			Load_hash_of_all_variables_perproduct_from_key_file(kf, set_session_hash, "myloader_session_variables")
			Refresh_set_session_from_hash(Set_session, set_session_hash)
		} else {
			log.Tracef("metadata: skipping group %s", group)
		}
	}

	M_remove(directory, file)
}

func process_schema_view_filename(filename string) bool {
	var db_name, table_name string
	var real_db_name *database
	get_database_table_from_file(filename, "-schema", &db_name, &table_name)
	if db_name == "" {
		log.Criticalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(db_name, db_name)
	if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
		log.Warnf("File %s has been filter out(1)", filename)
		return false
	}
	var dbt = append_new_db_table(real_db_name, table_name, 0, nil)
	dbt.is_view = true
	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, nil, VIEW)
	G_async_queue_push(conf.view_queue, new_control_job(JOB_RESTORE, rj, real_db_name))
	return true
}

func process_schema_sequence_filename(filename string) bool {
	var db_name, table_name string
	var real_db_name *database
	var dbt *db_table
	get_database_table_from_file(filename, "-schema-sequence", &db_name, &table_name)
	if db_name == "" {
		log.Criticalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(db_name, db_name)
	if real_db_name == nil {
		log.Warnf("It was not possible to process file: %s (3) because real_db_name isn't found. We might renqueue it, take into account that restores without schema-create files are not supported", filename)
		return false
	}
	if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
		log.Warnf("File %s has been filter out", filename)
		return true
	}
	dbt = append_new_db_table(real_db_name, table_name, 0, nil)
	dbt.is_sequence = true
	dbt.schema_state = NOT_CREATED
	var rj *restore_job = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, dbt, real_db_name, nil, SEQUENCE)
	var cj *control_job = new_control_job(JOB_RESTORE, rj, real_db_name)
	real_db_name.mutex.Lock()
	if real_db_name.schema_state != CREATED {
		log.Tracef("%s.sequence_queue <- %v: %s", db_name, cj.data.restore_job.job_type, filename)
		log.Tracef("real_db_name: %v; sequence_queue: %v", real_db_name, real_db_name.sequence_queue)
		G_async_queue_push(real_db_name.sequence_queue, cj)
		real_db_name.mutex.Unlock()
		return false
	} else {
		if cj != nil {
			log.Tracef("table_queue <- %v: %s", cj.data.restore_job.job_type, filename)
			G_async_queue_push(conf.table_queue, cj)
		}
	}
	real_db_name.mutex.Unlock()
	return true

}

func process_schema_filename(filename string, object string) bool {
	var db_name, table_name string
	var real_db_name *database
	var dbt *db_table
	get_database_table_from_file(filename, "-schema", &db_name, &table_name)
	if db_name == "" {
		log.Criticalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(db_name, db_name)
	if table_name != "" {
		if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
			log.Warnf("File %s has been filter out(1)", filename)
			return false
		}
		dbt = append_new_db_table(real_db_name, table_name, 0, nil)
	}
	if object == TRIGGER || dbt == nil || !dbt.object_to_export.No_trigger {
		var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, nil, object)
		G_async_queue_push(conf.post_queue, new_control_job(JOB_RESTORE, rj, real_db_name))
	}

	return true
}

func cmp_restore_job(rj1 *restore_job, rj2 *restore_job) int {
	if rj1.data.drj.part != rj2.data.drj.part {
		var a = rj1.data.drj.part
		var b = rj2.data.drj.part
		for a%2 == b%2 {
			a = a >> 1
			b = b >> 1
		}
		if a%2 > b%2 {
			return 1
		} else {
			return 0
		}
	}
	if rj1.data.drj.sub_part > rj2.data.drj.sub_part {
		return 1
	} else {
		return 0
	}

}

func process_data_filename(filename string) bool {
	var db_name, table_name string
	var part, sub_part uint
	get_database_table_part_name_from_filename(filename, &db_name, &table_name, &part, &sub_part)
	if db_name == "" || table_name == "" {
		log.Criticalf("It was not possible to process file: %s (3)", filename)
	}
	var real_db_name *database = get_db_hash(db_name, db_name)
	if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
		log.Warnf("Skiping table: `%s`.`%s`", real_db_name.name, table_name)
		return false
	}
	var dbt *db_table = append_new_db_table(real_db_name, table_name, 0, nil)
	if !dbt.object_to_export.No_data {
		var rj = new_data_restore_job(filename, JOB_RESTORE_FILENAME, dbt, part, sub_part)
		dbt.mutex.Lock()
		atomic.AddInt64(&dbt.remaining_jobs, 1)
		dbt.count++
		dbt.restore_job_list.PushBack(rj)
		//  dbt.restore_job_list=g_list_append(dbt.restore_job_list,rj);
		dbt.mutex.Unlock()
	} else {
		log.Warnf("Ignoring file %s on `%s`.`%s`", filename, dbt.database.name, dbt.table)
	}

	return true
}

func process_checksum_filename(filename string) bool {
	var db_name, table_name string
	get_database_table_from_file(filename, "-", &db_name, &table_name)
	if db_name == "" {
		log.Criticalf("It was not possible to process file: %s (4)", filename)
	}
	if table_name == "" {
		var real_db_name = get_db_hash(db_name, db_name)
		if !eval_table(real_db_name.name, table_name, conf.table_list_mutex) {
			return false
		}
	}
	return true
}
