package myloader

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type restore_job_type int
type purge_mode int

const (
	JOB_RESTORE_SCHEMA_FILENAME restore_job_type = iota
	JOB_RESTORE_FILENAME
	JOB_TO_CREATE_TABLE
	JOB_RESTORE_STRING
)
const (
	FAIL purge_mode = iota
	NONE
	DROP
	TRUNCATE
	DELETE
)

type restore_job struct {
	job_type restore_job_type
	data     *restore_job_data
	filename string
	dbt      *db_table
}

type restore_job_data struct {
	drj *data_restore_job
	srj *schema_restore_job
}

type data_restore_job struct {
	index    uint
	part     uint
	sub_part uint
}

type schema_restore_job struct {
	database  *database
	statement *GString
	object    string
}

func (o *OptionEntries) initialize_restore_job(pm_str string) {
	o.global.file_list_to_do = g_async_queue_new(o.BufferSize)
	o.global.single_threaded_create_table = g_mutex_new()
	o.global.progress_mutex = g_mutex_new()
	o.global.shutdown_triggered_mutex = g_mutex_new()
	o.global.detailed_errors = new(restore_errors)
	if pm_str != "" {
		if strings.EqualFold(pm_str, "TRUNCATE") {
			o.global.purge_mode = TRUNCATE
		} else if strings.EqualFold(pm_str, "DROP") {
			o.global.purge_mode = DROP
		} else if strings.EqualFold(pm_str, "NONE") {
			o.global.purge_mode = NONE
		} else if strings.EqualFold(pm_str, "FAIL") {
			o.global.purge_mode = FAIL
		} else if strings.EqualFold(pm_str, "DELETE") {
			o.global.purge_mode = DELETE
		} else {
			log.Errorf("Purge mode unknown")
		}
	} else if o.OverwriteTables {
		o.global.purge_mode = DROP
	} else {
		o.global.purge_mode = FAIL
	}
}

func new_data_restore_job_internal(index uint, part uint, sub_part uint) *data_restore_job {
	var drj = new(data_restore_job)
	drj.index = index
	drj.part = part
	drj.sub_part = sub_part
	return drj
}

func new_schema_restore_job_internal(database *database, statement *GString, object string) *schema_restore_job {
	var srj = new(schema_restore_job)
	srj.database = database
	srj.statement = statement
	srj.object = object
	return srj
}

func new_restore_job(filename string, dbt *db_table, job_type restore_job_type) *restore_job {
	var rj = new(restore_job)
	rj.data = new(restore_job_data)
	rj.data.srj = new(schema_restore_job)
	rj.data.drj = new(data_restore_job)
	rj.filename = filename
	rj.dbt = dbt
	rj.job_type = job_type
	return rj
}

func new_data_restore_job(filename string, job_type restore_job_type, dbt *db_table, part uint, sub_part uint) *restore_job {
	var rj = new_restore_job(filename, dbt, job_type)
	rj.data.drj = new_data_restore_job_internal(dbt.count+1, part, sub_part)
	return rj
}

func new_schema_restore_job(filename string, job_type restore_job_type, dbt *db_table, database *database, statement *GString, object string) *restore_job {
	var rj = new_restore_job(filename, dbt, job_type)
	rj.data.srj = new_schema_restore_job_internal(database, statement, object)
	return rj
}

func free_restore_job(rj *restore_job) {
	rj.filename = ""
}

func free_schema_restore_job(srj *schema_restore_job) {
	srj = nil
}

func (o *OptionEntries) overwrite_table_message(m string, a ...any) {
	if o.OverwriteUnsafe {
		log.Fatalf(m, a...)
	} else {
		log.Warnf(m, a...)
	}
}

func (o *OptionEntries) overwrite_table(td *thread_data, dbt *db_table) bool {
	var truncate_or_delete_failed bool
	var data *GString = new(GString)
	var q = o.global.identifier_quote_character
	if o.global.purge_mode == DROP {
		log.Infof("Dropping table or view (if exists) %s.%s", dbt.database.real_database, dbt.real_table)
		g_string_printf(data, "DROP TABLE IF EXISTS %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if o.restore_data_in_gstring_extended(td, data, true, dbt.database, o.overwrite_table_message, "Drop table %s.%s failed", dbt.database.real_database, dbt.real_table) != 0 {
			truncate_or_delete_failed = true
		}
	} else if o.global.purge_mode == TRUNCATE {
		log.Infof("Truncating table %s.%s", dbt.database.real_database, dbt.real_table)
		g_string_printf(data, "TRUNCATE TABLE %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if o.restore_data_in_gstring(td, data, true, dbt.database) != 0 {
			truncate_or_delete_failed = false
		} else {
			truncate_or_delete_failed = true
		}
		if truncate_or_delete_failed {
			log.Warnf("Truncate failed, we are going to try to create table or view")
		}
	} else if o.global.purge_mode == DELETE {
		log.Infof("Deleting content of table %s.%s", dbt.database.real_database, dbt.real_table)
		g_string_printf(data, "DELETE FROM %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if o.restore_data_in_gstring(td, data, true, dbt.database) != 0 {
			truncate_or_delete_failed = false
		} else {
			truncate_or_delete_failed = true
		}
		if truncate_or_delete_failed {
			log.Warnf("Delete failed, we are going to try to create table or view")
		}
	}
	return truncate_or_delete_failed
}

func (o *OptionEntries) increse_object_error(object string) {
	switch strings.ToLower(object) {
	case SEQUENCE:
		atomic.AddUint64(&o.global.detailed_errors.sequence_errors, 1)
	case TRIGGER:
		atomic.AddUint64(&o.global.detailed_errors.trigger_errors, 1)
	case TABLESPACE:
		atomic.AddUint64(&o.global.detailed_errors.tablespace_errors, 1)
	case CREATE_DATABASE:
		atomic.AddUint64(&o.global.detailed_errors.schema_errors, 1)
	case VIEW:
		atomic.AddUint64(&o.global.detailed_errors.view_errors, 1)
	case POST:
		atomic.AddUint64(&o.global.detailed_errors.post_errors, 1)
	case INDEXES:
		atomic.AddUint64(&o.global.detailed_errors.index_errors, 1)
	case CONSTRAINTS:
		atomic.AddUint64(&o.global.detailed_errors.constraints_errors, 1)
	default:
		log.Infof("Failed object %s no place to save", object)
	}
}

func schema_state_increment(key string, dbt *db_table, total *uint, schema_state schema_status) {
	_ = key
	if dbt.schema_state >= schema_state {
		*total = *total + 1
	}
}

func is_all_done(key string, dbt *db_table, total *uint) {
	_ = key
	schema_state_increment(key, dbt, total, ALL_DONE)
}

func is_created(key string, dbt *db_table, total *uint) {
	_ = key
	schema_state_increment(key, dbt, total, CREATED)
}

func g_hash_table_foreach(hash_table map[string]*db_table, f func(string, *db_table, *uint), total *uint) {
	for key, dbt := range hash_table {
		f(key, dbt, total)
	}
}

func get_total_done(conf *configuration, total *uint) {
	conf.table_hash_mutex.Lock()
	g_hash_table_foreach(conf.table_hash, is_all_done, total)
	conf.table_hash_mutex.Unlock()
}

func get_total_created(conf *configuration, total *uint) {
	conf.table_hash_mutex.Lock()
	g_hash_table_foreach(conf.table_hash, is_created, total)
	conf.table_hash_mutex.Unlock()
}

func (o *OptionEntries) process_restore_job(td *thread_data, rj *restore_job) bool {
	if td.conf.pause_resume != nil {
		task := td.conf.pause_resume.try_pop()
		var resume_mutex *sync.Mutex
		if task != nil {
			resume_mutex = task.(*sync.Mutex)
			log.Infof("Thread %d: Stop", td.thread_id)
			resume_mutex.Lock()
			resume_mutex.Unlock()
			log.Infof("Thread %d: Resumming", td.thread_id)
			resume_mutex = nil
		}
	}
	if o.global.shutdown_triggered {
		o.global.file_list_to_do.push(rj.filename)
		td.status = COMPLETED
		return false
	}
	var dbt = rj.dbt
	var total uint
	td.status = STARTED
	switch rj.job_type {
	case JOB_RESTORE_STRING:
		if o.SourceDb == "" || strings.Compare(dbt.database.name, o.SourceDb) == 0 {
			log.Infof("Thread %d: restoring %s %s.%s from %s. Tables %d of %d completed", td.thread_id,
				rj.data.srj.object, dbt.database.real_database, dbt.real_table, rj.filename, total, len(td.conf.table_hash))
			if o.restore_data_in_gstring(td, rj.data.srj.statement, false, rj.data.srj.database) != 0 {
				o.increse_object_error(rj.data.srj.object)
				log.Infof("Failed %s: %s", rj.data.srj.object, rj.data.srj.statement)
			}
		}
		free_schema_restore_job(rj.data.srj)
		break
	case JOB_TO_CREATE_TABLE:
		dbt.schema_state = CREATING
		if (o.SourceDb != "" || strings.Compare(dbt.database.name, o.SourceDb) == 0) && o.NoSchemas && !dbt.object_to_export.no_schema {
			if o.SerialTblCreation {
				o.global.single_threaded_create_table.Lock()
			}
			log.Infof("Thread %d: restoring table %s.%s from %s", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename)
			var overwrite_error bool
			if o.OverwriteTables {
				overwrite_error = o.overwrite_table(td, dbt)
				if overwrite_error {
					if dbt.retry_count != 0 {
						dbt.retry_count--
						dbt.schema_state = NOT_CREATED
						m_warning("Drop table %s.%s failed: retry %d of %d", dbt.database.real_database, dbt.real_table, o.global.retry_count-dbt.retry_count, o.global.retry_count)
						return true
					} else {
						m_critical("Drop table %s.%s failed: exiting", dbt.database.real_database, dbt.real_table)
					}
				} else if dbt.retry_count < o.global.retry_count {
					m_warning("Drop table %s.%s succeeded!", dbt.database.real_database, dbt.real_table)
				}
			}
			if (o.global.purge_mode == TRUNCATE || o.global.purge_mode == DELETE) && overwrite_error {
				log.Infof("Skipping table creation %s.%s from %s", dbt.database.real_database, dbt.real_table, rj.filename)
			} else {
				log.Infof("Thread %d: Creating table %s.%s from content in %s. On db: %s", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename, dbt.database.name)
				if o.restore_data_in_gstring(td, rj.data.srj.statement, true, rj.data.srj.database) != 0 {
					atomic.AddUint64(&o.global.detailed_errors.schema_errors, 1)
					if o.global.purge_mode == FAIL {
						log.Errorf("Thread %d: issue restoring %s", td.thread_id, rj.filename)
					} else {
						log.Fatalf("Thread %d: issue restoring %s", td.thread_id, rj.filename)
					}
				} else {
					get_total_created(td.conf, &total)
					log.Infof("Thread %d: Table %s.%s created. Tables that pass created stage: %d of %d", td.thread_id, dbt.database.real_database, dbt.real_table, total, len(td.conf.table_hash))
				}
			}
			if o.SerialTblCreation {
				o.global.single_threaded_create_table.Unlock()
			}
		}
		dbt.schema_state = CREATED
		free_schema_restore_job(rj.data.srj)
		break
	case JOB_RESTORE_FILENAME:
		if o.SourceDb == "" || strings.Compare(dbt.database.name, o.SourceDb) == 0 {
			o.global.progress_mutex.Lock()
			log.Infof("Thread %d: restoring %s.%s part %d of %d from %s | Progress %d of %d. Tables %d of %d completed", td.thread_id,
				dbt.database.real_database, dbt.real_table, rj.data.drj.index, dbt.count, rj.filename, o.global.progress, o.global.total_data_sql_files, total, len(td.conf.table_hash))
			o.global.progress_mutex.Unlock()
			if o.restore_data_from_file(td, rj.filename, false, dbt.database) > 0 {
				atomic.AddUint64(&o.global.detailed_errors.data_errors, 1)
				log.Fatalf("Thread : issue restoring %s", rj.filename)
			}
		}
		g_atomic_int_dec_and_test(&(dbt.remaining_jobs))
		break
	case JOB_RESTORE_SCHEMA_FILENAME:
		if o.SourceDb == "" || strings.Compare(rj.data.srj.database.name, o.SourceDb) == 0 {
			if strings.EqualFold(rj.data.srj.object, VIEW) || !o.NoSchemas {
				get_total_done(td.conf, &total)
				log.Infof("Thread %d: restoring %s on `%s` from %s. Tables %d of %d completed", td.thread_id, rj.data.srj.object,
					rj.data.srj.database.real_database, rj.filename, total, len(td.conf.table_hash))
				if dbt != nil {
					dbt.schema_state = CREATING
				}
				var db *database
				if strings.EqualFold(rj.data.srj.object, CREATE_DATABASE) {
					db = rj.data.srj.database
				}
				if o.restore_data_from_file(td, rj.filename, true, db) > 0 {
					o.increse_object_error(rj.data.srj.object)
					if dbt != nil {
						dbt.schema_state = NOT_CREATED
					}
				} else if dbt != nil {
					dbt.schema_state = CREATED
				}
			}
		}
		free_schema_restore_job(rj.data.srj)
		break

	default:
		log.Fatalf("Something very bad happened!")
	}
	td.status = COMPLETED
	return false
}

func (o *OptionEntries) signal_thread(data any) {
	if o.global.signal_thread == nil {
		o.global.signal_thread = new(sync.WaitGroup)
	}
	o.global.signal_thread.Add(1)
	defer o.global.signal_thread.Done()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	o.sig_triggered(data, sig)
	log.Infof("Ending signal thread")
}

func (o *OptionEntries) sig_triggered(user_data any, signal os.Signal) bool {
	var conf *configuration = user_data.(*configuration)
	var i uint
	var queue *asyncQueue
	o.global.shutdown_triggered_mutex.Lock()
	if signal == syscall.SIGTERM {
		o.global.shutdown_triggered = true
	} else {
		if o.global.pause_mutex_per_thread == nil {
			o.global.pause_mutex_per_thread = make([]*sync.Mutex, o.NumThreads)
			for i = 0; i < o.NumThreads; i++ {
				o.global.pause_mutex_per_thread[i] = g_mutex_new()
			}
		}
		if conf.pause_resume == nil {
			conf.pause_resume = g_async_queue_new(o.BufferSize)
		}
		queue = conf.pause_resume
		for i = 0; i < o.NumThreads; i++ {
			o.global.pause_mutex_per_thread[i].Lock()
			queue.push(o.global.pause_mutex_per_thread[i])
		}
		fmt.Printf("Ctrl+c detected! Are you sure you want to cancel(Y/N)?")
		var c string
		for {
			_, _ = fmt.Scanln(&c)
			if c == "N" || c == "n" {
				for i = 0; i < o.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				o.global.shutdown_triggered_mutex.Unlock()
				return true
			}
			if c == "Y" || c == "y" {
				o.global.shutdown_triggered = true
				for i = 0; i < o.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				break
			}
		}

	}
	o.inform_restore_job_running()
	o.create_index_shutdown_job(conf)
	log.Infof("Writing resume.partial file")
	var filename string
	var p = "resume.partial"
	var p2 = "resume"
	outfile, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0660)
	filename = o.global.file_list_to_do.pop().(string)
	for strings.Compare(filename, "NO_MORE_FILES") != 0 {
		log.Debugf("Adding %s to resume file", filename)
		outfile.WriteString(fmt.Sprintf("%s\n", filename))
		filename = o.global.file_list_to_do.pop().(string)
	}
	outfile.Close()
	err = os.Rename(p, p2)
	if err != nil {
		log.Fatalf("Error renaming resume.partial to resume")
	}
	log.Infof("Shutting down gracefully completed.")
	o.global.shutdown_triggered_mutex.Unlock()
	return false
}

func stop_signal_thread(o *OptionEntries) {
	o.global.shutdown_triggered_mutex.Lock()
	o.global.shutdown_triggered_mutex.Unlock()
}

func (o *OptionEntries) restore_job_finish() {
	if o.global.shutdown_triggered {
		o.global.file_list_to_do.push("NO_MORE_FILES")
	}
}
