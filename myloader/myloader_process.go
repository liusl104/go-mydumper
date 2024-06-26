package myloader

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type osFile struct {
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

func initialize_process(o *OptionEntries, c *configuration) {
	o.global.conf = c
	o.global.fifo_hash = make(map[*os.File]*fifo)
	o.global.fifo_table_mutex = g_mutex_new()
}

func append_new_db_table(o *OptionEntries, filename string, database string, table string, number_rows uint64, alter_table_statement string) *db_table {
	if database == "" || table == "" {
		log.Fatalf("It was not possible to process file: %s, database: %s table: %s", filename, database, table)
	}
	var real_db_name = get_db_hash(o, database, database)
	if real_db_name == nil {
		log.Errorf("It was not possible to process file: %s. %s was not found and real_db_name is null. Restore without schema-create files is not supported", filename, database)
	}
	var lkey = build_dbt_key(database, table)
	var dbt *db_table
	dbt, _ = o.global.conf.table_hash[lkey]
	if dbt == nil {
		o.global.conf.table_hash_mutex.Lock()
		dbt, _ = o.global.conf.table_hash[lkey]
		if dbt == nil {
			dbt = new(db_table)
			dbt.database = real_db_name
			dbt.table = table
			dbt.real_table = dbt.table
			dbt.rows = number_rows
			dbt.restore_job_list = nil
			dbt.current_threads = 0
			if o.Threads.MaxThreadsPerTable > o.Common.NumThreads {
				dbt.max_threads = o.Common.NumThreads
			} else {
				dbt.max_threads = o.Threads.MaxThreadsPerTable
			}
			if o.Threads.MaxThreadsPerTableHard > o.Common.NumThreads {
				dbt.max_threads_hard = o.Common.NumThreads
			} else {
				dbt.max_threads_hard = o.Threads.MaxThreadsPerTableHard
			}
			dbt.mutex = g_mutex_new()
			dbt.indexes = alter_table_statement
			// dbt.start_index_time
			dbt.schema_state = NOT_FOUND
			dbt.index_enqueued = false
			dbt.remaining_jobs = 0
			dbt.constraints = ""
			dbt.count = 0
			o.global.conf.table_hash[lkey] = dbt
			refresh_table_list_without_table_hash_lock(o.global.conf)
			dbt.schema_checksum = ""
			dbt.triggers_checksum = ""
			dbt.indexes_checksum = ""
			dbt.data_checksum = ""
			dbt.is_view = false
		} else {
			if number_rows > 0 {
				dbt.rows = number_rows
			}
			if alter_table_statement != "" {
				dbt.indexes = alter_table_statement
			}
		}
		o.global.conf.table_hash_mutex.Unlock()
	} else {
		if number_rows > 0 {
			dbt.rows = number_rows
		}
		if alter_table_statement != "" {
			dbt.indexes = alter_table_statement
		}
	}
	return dbt
}

func free_dbt(dbt *db_table) {
	dbt.table = ""
	dbt.constraints = ""
}

func free_table_hash(o *OptionEntries, table_hash map[string]*db_table) {
	o.global.conf.table_hash_mutex.Lock()
	var lkey string
	if table_hash != nil {
		for lkey, _ = range table_hash {
			delete(table_hash, lkey)
		}
	}
	o.global.conf.table_hash_mutex.Unlock()
}

func myl_open(o *OptionEntries, filename string, mode int) (*os.File, error) {
	var file *os.File
	var child_proc *osFile
	var basename string
	var fifoname string
	var err error
	if get_command_and_basename(o, fifoname, &basename) {
		fifoname = basename
		if o.Common.FifoDirectory != "" {
			var basefilename = path.Base(basename)
			fifoname = fmt.Sprintf("%s/%s", o.Common.FifoDirectory, basefilename)
		}
		fileInfo, _ := os.Lstat(fifoname)
		if fileInfo.Mode()&os.ModeSymlink == 0 {
			log.Warnf("FIFO file found %s, removing and continuing", fifoname)
			os.Remove(basename)
		}
		child_proc = execute_file_per_thread(filename, fifoname, o.Threads.ExecPerThreadExtension)
		file, err = os.OpenFile(fifoname, mode, 0660)
		o.global.fifo_table_mutex.Lock()
		f, _ := o.global.fifo_hash[file]
		if f != nil {
			f.mutex.Lock()
			o.global.fifo_table_mutex.Unlock()
			f.pid = child_proc
			f.filename = filename
			f.stdout_filename = fifoname
			f.status = new(sync.WaitGroup)
			f.status.Add(1)
		} else {
			f = new(fifo)
			f.mutex = g_mutex_new()
			f.mutex.Lock()
			f.pid = child_proc
			f.filename = filename
			f.stdout_filename = fifoname
			f.status = new(sync.WaitGroup)
			f.status.Add(1)
			o.global.fifo_hash[file] = f
			o.global.fifo_table_mutex.Unlock()
		}
	} else {
		/*fileInfo, _ := os.Lstat(filename)
		if fileInfo.Mode()&os.ModeSymlink == 0 {
			log.Warnf("FIFO file found %s. Skipping", filename)
			file = nil
		} else {
			file, err = os.OpenFile(filename, mode, 0660)
			if err != nil {
				log.Errorf("open file %s fail:%v", filename, err)
				return nil, err
			}
		}*/
		file, err = os.OpenFile(filename, mode, 0660)
		if err != nil {
			log.Errorf("open file %s fail:%v", filename, err)
			return nil, err
		}

	}
	return file, nil
}

func myl_close(o *OptionEntries, filename string, file *os.File) {
	o.global.fifo_table_mutex.Lock()
	f, _ := o.global.fifo_hash[file]
	o.global.fifo_table_mutex.Unlock()
	_ = file.Close()
	if f != nil {
		f.status.Wait()
		o.global.fifo_table_mutex.Lock()
		_ = f.pid.close()
		f.mutex.Unlock()
		o.global.fifo_table_mutex.Unlock()
		_ = os.Remove(f.stdout_filename)
	}
	m_remove(o, "", filename)
}

func load_schema(o *OptionEntries, dbt *db_table, filename string) *control_job {
	var infile *os.File
	var data string
	var eof bool
	var line uint
	var err error
	var create_table_statement string
	infile, err = myl_open(o, filename, os.O_RDONLY)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		return nil
	}
	var reader = bufio.NewReader(infile)
	for eof == false {
		if read_data(reader, &data, &eof, &line) {
			var length int
			if len(data) >= 5 {
				length = len(data) - 5
			} else {
				length = len(data)
			}
			if strings.Contains(data[length:], ";\n") {
				if data[:13] == "CREATE TABLE " {
					if !strings.Contains(data[:30], o.Common.IdentifierQuoteCharacter) {
						log.Fatalf("Identifier quote character (%s) not found on %s. Review file and configure --identifier-quote-character properly", o.Common.IdentifierQuoteCharacter, filename)
					}
					var create_table = strings.SplitN(data, o.Common.IdentifierQuoteCharacter, 3)
					dbt.real_table = create_table[1]
					if strings.HasPrefix(dbt.table, "mydumper_") {
						o.global.tbl_hash[dbt.table] = dbt.real_table
					} else {
						o.global.tbl_hash[dbt.real_table] = dbt.real_table
					}
					if o.Statement.AppendIfNotExist {
						if data[:13] == "CREATE TABLE " && data[:15] != "CREATE TABLE IF" {
							var tmp_data = ""
							tmp_data += "CREATE TABLE IF NOT EXISTS "
							tmp_data += fmt.Sprintf("%c", data[13])
							data = tmp_data
						}
					}
				}
				if o.Execution.InnodbOptimizeKeys != "" {
					var alter_table_statement, alter_table_constraint_statement string
					if strings.HasPrefix(data, "/*!40") {
						alter_table_statement = data
						create_table_statement = data
					} else {
						var new_create_table_statement string
						var flag = process_create_table_statement(data, &new_create_table_statement, &alter_table_statement, &alter_table_constraint_statement, dbt, dbt.rows == 0 || dbt.rows >= 1000000)
						if (flag & IS_INNODB_TABLE) != 0 {
							if (flag & IS_ALTER_TABLE_PRESENT) != 0 {
								alter_table_statement = finish_alter_table(alter_table_statement)
								log.Infof("Fast index creation will be use for table: %s.%s", dbt.database.real_database, dbt.real_table)
							} else {
								alter_table_statement = ""
							}
							create_table_statement += strings.Join(strings.Split(new_create_table_statement, ",\n)"), "\n)")
							dbt.indexes = alter_table_statement
							if (flag & INCLUDE_CONSTRAINT) != 0 {
								var rj = new_schema_restore_job(filename, JOB_RESTORE_STRING, dbt, dbt.database, alter_table_constraint_statement, CONSTRAINTS)
								o.global.conf.post_table_queue.push(new_job(JOB_RESTORE, rj, dbt.database.real_database))
								dbt.constraints = alter_table_constraint_statement
							} else {
								alter_table_constraint_statement = ""
							}

						} else {
							alter_table_statement = ""
							alter_table_constraint_statement = ""
							create_table_statement += data
						}
					}
				} else {
					create_table_statement += data
				}
			}
		}
	}

	if o.global.schema_sequence_fix {
		var statement = filter_sequence_schemas(create_table_statement)
		create_table_statement = statement
	}
	var rj = new_schema_restore_job(filename, JOB_TO_CREATE_TABLE, dbt, dbt.database, create_table_statement, "")
	var cj = new_job(JOB_RESTORE, rj, dbt.database.real_database)
	myl_close(o, filename, infile)
	return cj
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

func get_database_name_from_filename(filename string) string {
	var split_file = strings.SplitN(filename, "-schema-create.sql", 2)
	var db_name = split_file[0]
	return db_name
}

func get_database_table_part_name_from_filename(o *OptionEntries, filename string, database *string, table *string, part *uint, sub_part *uint) {
	var l = len(filename) - 4
	if o.Threads.ExecPerThreadExtension != "" && strings.HasSuffix(filename, o.Threads.ExecPerThreadExtension) {
		l -= len(o.Threads.ExecPerThreadExtension)
	}
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

func get_database_name_from_content(o *OptionEntries, filename string) string {
	var infile *os.File
	var err error
	infile, err = myl_open(o, filename, os.O_RDONLY)
	if infile == nil {
		log.Fatalf("cannot open database schema file %s (%v)", filename, err)
	}
	var eof bool
	var data string
	var line uint
	var real_database string
	var reader = bufio.NewReader(infile)

	for eof == false {
		if read_data(reader, &data, &eof, &line) {
			var length int
			if len(data) > 5 {
				length = strings.Index(data[len(data)-5:], ";\n")
			} else {
				length = strings.Index(data, ";\n")
			}
			if length > 0 {
				if strings.HasPrefix(data, "CREATE ") {
					var create = strings.SplitN(data, o.Common.IdentifierQuoteCharacter, 3)
					real_database = create[1]
					break
				}
			}
		}
	}

	myl_close(o, filename, infile)
	return real_database
}

func process_tablespace_filename(o *OptionEntries, filename string) {
	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, nil, "", TABLESPACE)
	o.global.conf.database_queue.push(new_job(JOB_RESTORE, rj, ""))
}

func process_database_filename(o *OptionEntries, filename string) {
	var db_kname, db_vname string
	db_kname = get_database_name_from_filename(filename)
	db_vname = db_kname

	if db_kname != "" {
		if o.Common.DB != "" {
			db_vname = o.Common.DB
		} else {
			if strings.HasPrefix(db_kname, "mydumper_") {
				db_vname = get_database_name_from_content(o, path.Join(o.global.directory, filename))
			}
		}
	} else {
		log.Fatalf("It was not possible to process db file: %s", filename)
	}

	log.Debugf("Adding database: %s -> %s", db_kname, db_vname)
	var real_db_name = get_db_hash(o, db_kname, db_vname)
	if o.Common.DB == "" {
		real_db_name.schema_state = NOT_CREATED
		var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, "", CREATE_DATABASE)
		o.global.conf.database_queue.push(new_job(JOB_RESTORE, rj, ""))
	} else {
		real_db_name.schema_state = CREATED
	}
}

func process_table_filename(o *OptionEntries, filename string) bool {
	var db_name, table_name string
	var dbt *db_table
	get_database_table_name_from_filename(filename, "-schema.sql", &db_name, &table_name)
	if db_name == "" || table_name == "" {
		log.Fatalf("It was not possible to process file: %s (1)", filename)
	}
	var real_db_name = get_db_hash(o, db_name, db_name)
	if !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
		log.Warnf("Skiping table: `%s`.`%s`", real_db_name.name, table_name)
		return false
	}
	dbt = append_new_db_table(o, "", db_name, table_name, 0, "")
	dbt.schema_state = NOT_CREATED
	var cj = load_schema(o, dbt, path.Join(o.global.directory, filename, ""))
	real_db_name.mutex.Lock()
	if real_db_name.schema_state != CREATED {
		real_db_name.queue.push(cj)
		real_db_name.mutex.Unlock()
		return false
	} else {
		if cj != nil {
			o.global.conf.table_queue.push(cj)
		}
	}
	real_db_name.mutex.Unlock()
	return true
}

func process_metadata_global(o *OptionEntries, file string) bool {
	var pt = path.Join(o.global.directory, file)
	var kf = load_config_file(pt)
	if kf == nil {
		log.Errorf("Global metadata file processing was not possible")
	}
	log.Infof("Reading metadata: %s", file)
	var j uint
	var err error
	var value string
	var length uint
	// gchar **groups=g_key_file_get_groups(kf, &length);
	var database_table []string
	var groups []string = kf.SectionStrings()
	length = uint(len(groups))
	var dbt *db_table
	o.global.change_master_statement = ""
	var delimiter = fmt.Sprintf("%s.%s", o.Common.IdentifierQuoteCharacter, o.Common.IdentifierQuoteCharacter)
	for j = 0; j < length; j++ {
		if strings.HasPrefix(groups[j], "`") {
			database_table = strings.SplitN(groups[j], delimiter, 2)
			if database_table[1] != "" {
				db_name := strings.ReplaceAll(database_table[0], o.Common.IdentifierQuoteCharacter, "")
				table_name := strings.ReplaceAll(database_table[1], o.Common.IdentifierQuoteCharacter, "")
				dbt = append_new_db_table(o, "", db_name, table_name, 0, "")
				err = nil
				var keys = kf.Section(groups[j]).Keys()
				dbt.data_checksum = get_value(kf, groups[j], "data_checksum")
				dbt.schema_checksum = get_value(kf, groups[j], "schema_checksum")
				dbt.indexes_checksum = get_value(kf, groups[j], "indexes_checksum")
				dbt.triggers_checksum = get_value(kf, groups[j], "triggers_checksum")
				value = get_value(kf, groups[j], "is_view")
				if value != "" && strings.Compare(value, "1") == 0 {
					dbt.is_view = true
				}
				var v int
				v, err = strconv.Atoi(get_value(kf, groups[j], "rows"))
				_ = err
				if v != 0 {
					dbt.rows = uint64(v)
				}
				value = get_value(kf, groups[j], "real_table_name")
				if value != "" {
					if strings.Compare(dbt.real_table, value) != 0 {
						dbt.real_table = value
					}
				}
				_ = keys
			} else {
				database_table[0] = database_table[0][:len(database_table[0])-1]
				var db = get_db_hash(o, database_table[0], database_table[0])
				db.schema_checksum = get_value(kf, groups[j], "schema_checksum")
				db.post_checksum = get_value(kf, groups[j], "post_checksum")
				db.triggers_checksum = get_value(kf, groups[j], "triggers_checksum")
			}
		} else if strings.HasPrefix(groups[j], "replication") {
			change_master(kf, groups[j], &o.global.change_master_statement)
		} else if strings.HasPrefix(groups[j], "master") {
			change_master(kf, groups[j], &o.global.change_master_statement)
		}
	}

	return true
}

func process_schema_view_filename(o *OptionEntries, filename string) bool {
	var db_name, table_name string
	var real_db_name *database
	get_database_table_from_file(filename, "-schema", &db_name, &table_name)
	if db_name == "" {
		log.Fatalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(o, db_name, db_name)
	if !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
		log.Warnf("File %s has been filter out(1)", filename)
		return false
	}
	var dbt = append_new_db_table(o, "", db_name, table_name, 0, "")
	dbt.is_view = true
	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, "", VIEW)
	o.global.conf.view_queue.push(new_job(JOB_RESTORE, rj, real_db_name.name))
	return true
}

func process_schema_sequence_filename(o *OptionEntries, filename string) bool {
	var db_name, table_name string
	var real_db_name *database
	get_database_table_from_file(filename, "-schema-sequence", &db_name, &table_name)
	if db_name == "" {
		log.Fatalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(o, db_name, db_name)
	if real_db_name == nil {
		log.Warnf("It was not possible to process file: %s (3) because real_db_name isn't found. We might renqueue it, take into account that restores without schema-create files are not supported", filename)
		return false
	}
	if !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
		log.Warnf("File %s has been filter out", filename)
		return true
	}

	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, "", SEQUENCE)
	o.global.conf.view_queue.push(new_job(JOB_RESTORE, rj, real_db_name.name))
	return true

}

func process_schema_filename(o *OptionEntries, filename string, object string) bool {
	var db_name, table_name string
	var real_db_name *database
	get_database_table_from_file(filename, "-schema", &db_name, &table_name)
	if db_name == "" {
		log.Fatalf("Database is null on: %s", filename)
	}
	real_db_name = get_db_hash(o, db_name, db_name)
	if table_name != "" && !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
		log.Warnf("File %s has been filter out(1)", filename)
		return false
	}
	var rj = new_schema_restore_job(filename, JOB_RESTORE_SCHEMA_FILENAME, nil, real_db_name, "", object)
	o.global.conf.post_queue.push(new_job(JOB_RESTORE, rj, real_db_name.name))
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

func process_data_filename(o *OptionEntries, filename string) bool {
	var db_name, table_name string
	var part, sub_part uint
	get_database_table_part_name_from_filename(o, filename, &db_name, &table_name, &part, &sub_part)
	if db_name == "" || table_name == "" {
		log.Fatalf("It was not possible to process file: %s (3)", filename)
	}
	var real_db_name = get_db_hash(o, db_name, db_name)
	if !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
		log.Warnf("Skiping table: `%s`.`%s`", real_db_name.name, table_name)
		return false
	}
	var dbt = append_new_db_table(o, filename, db_name, table_name, 0, "")
	var rj = new_data_restore_job(filename, JOB_RESTORE_FILENAME, dbt, part, sub_part)
	dbt.mutex.Lock()
	atomic.AddInt64(&dbt.remaining_jobs, 1)
	dbt.count++
	dbt.restore_job_list = append(dbt.restore_job_list, rj)
	slices.SortFunc(dbt.restore_job_list, cmp_restore_job)
	//  dbt.restore_job_list=g_list_append(dbt.restore_job_list,rj);
	dbt.mutex.Unlock()

	return true
}

func process_checksum_filename(o *OptionEntries, filename string) bool {
	var db_name, table_name string
	get_database_table_from_file(filename, "-", &db_name, &table_name)
	if db_name == "" {
		log.Fatalf("It was not possible to process file: %s (4)", filename)
	}
	if table_name == "" {
		var real_db_name = get_db_hash(o, db_name, db_name)
		if !eval_table(o, real_db_name.name, table_name, o.global.conf.table_list_mutex) {
			return false
		}
	}
	return true
}
