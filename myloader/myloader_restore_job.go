package myloader

import (
	"fmt"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

var (
	shutdown_triggered           bool
	file_list_to_do              *GAsyncQueue
	progress_mutex               *sync.Mutex
	single_threaded_create_table *sync.Mutex
	shutdown_triggered_mutex     *sync.Mutex
	progress                     int
	purge_mode                   purgeMode = FAIL
	total_data_sql_files         int
	signal_threads               *sync.WaitGroup
	pause_mutex_per_thread       []*sync.Mutex
)

type restore_job_type int
type purgeMode int

const (
	JOB_RESTORE_SCHEMA_FILENAME restore_job_type = iota
	JOB_RESTORE_FILENAME
	JOB_TO_CREATE_TABLE
	JOB_RESTORE_STRING
)
const (
	FAIL purgeMode = iota
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

func initialize_restore_job(pm_str string) {
	file_list_to_do = G_async_queue_new(BufferSize)
	single_threaded_create_table = G_mutex_new()
	progress_mutex = G_mutex_new()
	shutdown_triggered_mutex = G_mutex_new()
	detailed_errors = new(restore_errors)
	if pm_str != "" {
		if strings.EqualFold(pm_str, "TRUNCATE") {
			purge_mode = TRUNCATE
		} else if strings.EqualFold(pm_str, "DROP") {
			purge_mode = DROP
		} else if strings.EqualFold(pm_str, "NONE") {
			purge_mode = NONE
		} else if strings.EqualFold(pm_str, "FAIL") {
			purge_mode = FAIL
		} else if strings.EqualFold(pm_str, "DELETE") {
			purge_mode = DELETE
		} else {
			log.Errorf("Purge mode unknown")
		}
	} else if OverwriteTables {
		purge_mode = DROP
	} else {
		purge_mode = FAIL
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

func overwrite_table_message(m string, a ...any) {
	if OverwriteUnsafe {
		log.Criticalf(m, a...)
	} else {
		log.Warnf(m, a...)
	}
}

func overwrite_table(td *thread_data, dbt *db_table) bool {
	var truncate_or_delete_failed bool
	var data *GString = G_string_new("")
	var q = Identifier_quote_character
	if purge_mode == DROP {
		log.Infof("Dropping table or view (if exists) %s.%s", dbt.database.real_database, dbt.real_table)
		G_string_printf(data, "DROP TABLE IF EXISTS %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if restore_data_in_gstring_extended(td, data, true, dbt.database, overwrite_table_message, "Drop table %s.%s failed", dbt.database.real_database, dbt.real_table) != 0 {
			truncate_or_delete_failed = true
		}
		G_string_printf(data, "DROP VIEW IF EXISTS %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if restore_data_in_gstring(td, data, true, dbt.database) != 0 {
			log.Critical("Drop view failed")
		}
	} else if purge_mode == TRUNCATE {
		log.Infof("Truncating table %s.%s", dbt.database.real_database, dbt.real_table)
		G_string_printf(data, "TRUNCATE TABLE %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if restore_data_in_gstring(td, data, true, dbt.database) != 0 {
			truncate_or_delete_failed = false
		} else {
			truncate_or_delete_failed = true
		}
		if truncate_or_delete_failed {
			log.Warnf("Truncate failed, we are going to try to create table or view")
		}
	} else if purge_mode == DELETE {
		log.Infof("Deleting content of table %s.%s", dbt.database.real_database, dbt.real_table)
		G_string_printf(data, "DELETE FROM %s%s%s.%s%s%s", q, dbt.database.real_database, q, q, dbt.real_table, q)
		if restore_data_in_gstring(td, data, true, dbt.database) != 0 {
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

func increse_object_error(object string) {
	switch strings.ToLower(object) {
	case SEQUENCE:
		atomic.AddUint64(&detailed_errors.sequence_errors, 1)
	case TRIGGER:
		atomic.AddUint64(&detailed_errors.trigger_errors, 1)
	case TABLESPACE:
		atomic.AddUint64(&detailed_errors.tablespace_errors, 1)
	case CREATE_DATABASE:
		atomic.AddUint64(&detailed_errors.schema_errors, 1)
	case VIEW:
		atomic.AddUint64(&detailed_errors.view_errors, 1)
	case POST:
		atomic.AddUint64(&detailed_errors.post_errors, 1)
	case INDEXES:
		atomic.AddUint64(&detailed_errors.index_errors, 1)
	case CONSTRAINTS:
		atomic.AddUint64(&detailed_errors.constraints_errors, 1)
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

func process_restore_job(td *thread_data, rj *restore_job) bool {
	if td.conf.pause_resume != nil {
		task := G_async_queue_try_pop(td.conf.pause_resume)
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
	if shutdown_triggered {
		G_async_queue_push(file_list_to_do, rj.filename)
		td.status = COMPLETED
		return false
	}
	var dbt = rj.dbt
	var total uint
	td.status = STARTED
	switch rj.job_type {
	case JOB_RESTORE_STRING:
		if SourceDb == "" || strings.Compare(dbt.database.name, SourceDb) == 0 {
			log.Infof("Thread %d: restoring %s %s.%s from %s. Tables %d of %d completed", td.thread_id,
				rj.data.srj.object, dbt.database.real_database, dbt.real_table, rj.filename, total, len(td.conf.table_hash))
			if restore_data_in_gstring(td, rj.data.srj.statement, false, rj.data.srj.database) != 0 {
				increse_object_error(rj.data.srj.object)
				log.Infof("Failed %s: %s", rj.data.srj.object, rj.data.srj.statement)
			}
		}
		free_schema_restore_job(rj.data.srj)
		break
	case JOB_TO_CREATE_TABLE:
		dbt.schema_state = CREATING
		if (SourceDb != "" || strings.Compare(dbt.database.name, SourceDb) == 0) && NoSchemas && !dbt.object_to_export.No_schema {
			if SerialTblCreation {
				single_threaded_create_table.Lock()
			}
			log.Infof("Thread %d: restoring table %s.%s from %s", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename)
			var overwrite_error bool
			if OverwriteTables {
				overwrite_error = overwrite_table(td, dbt)
				if overwrite_error {
					if dbt.retry_count != 0 {
						dbt.retry_count--
						dbt.schema_state = NOT_CREATED
						M_warning("Drop table %s.%s failed: retry %d of %d", dbt.database.real_database, dbt.real_table, retry_count-dbt.retry_count, retry_count)
						return true
					} else {
						M_critical("Drop table %s.%s failed: exiting", dbt.database.real_database, dbt.real_table)
					}
				} else if dbt.retry_count < retry_count {
					M_warning("Drop table %s.%s succeeded!", dbt.database.real_database, dbt.real_table)
				}
			}
			if (purge_mode == TRUNCATE || purge_mode == DELETE) && overwrite_error {
				log.Infof("Skipping table creation %s.%s from %s", dbt.database.real_database, dbt.real_table, rj.filename)
			} else {
				log.Infof("Thread %d: Creating table %s.%s from content in %s. On db: %s", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename, dbt.database.name)
				if restore_data_in_gstring(td, rj.data.srj.statement, true, rj.data.srj.database) != 0 {
					atomic.AddUint64(&detailed_errors.schema_errors, 1)
					if purge_mode == FAIL {
						log.Errorf("Thread %d: issue restoring %s", td.thread_id, rj.filename)
					} else {
						log.Criticalf("Thread %d: issue restoring %s", td.thread_id, rj.filename)
					}
				} else {
					get_total_created(td.conf, &total)
					log.Infof("Thread %d: Table %s.%s created. Tables that pass created stage: %d of %d", td.thread_id, dbt.database.real_database, dbt.real_table, total, len(td.conf.table_hash))
				}
			}
			if SerialTblCreation {
				single_threaded_create_table.Unlock()
			}
		}
		dbt.schema_state = CREATED
		free_schema_restore_job(rj.data.srj)
		break
	case JOB_RESTORE_FILENAME:
		if SourceDb == "" || strings.Compare(dbt.database.name, SourceDb) == 0 {
			progress_mutex.Lock()
			log.Infof("Thread %d: restoring %s.%s part %d of %d from %s | Progress %d of %d. Tables %d of %d completed", td.thread_id,
				dbt.database.real_database, dbt.real_table, rj.data.drj.index, dbt.count, rj.filename, progress, total_data_sql_files, total, len(td.conf.table_hash))
			progress_mutex.Unlock()
			if restore_data_from_file(td, rj.filename, false, dbt.database) > 0 {
				atomic.AddUint64(&detailed_errors.data_errors, 1)
				log.Criticalf("Thread : issue restoring %s", rj.filename)
			}
		}
		G_atomic_int_dec_and_test(&(dbt.remaining_jobs))
		break
	case JOB_RESTORE_SCHEMA_FILENAME:
		if SourceDb == "" || strings.Compare(rj.data.srj.database.name, SourceDb) == 0 {
			if strings.EqualFold(rj.data.srj.object, VIEW) || !NoSchemas {
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
				if restore_data_from_file(td, rj.filename, true, db) > 0 {
					increse_object_error(rj.data.srj.object)
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
		log.Critical("Something very bad happened!")
	}
	td.status = COMPLETED
	return false
}

func signal_thread(data any) {
	defer signal_threads.Done()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	sig_triggered(data, sig)
	log.Infof("Ending signal thread")
}

func sig_triggered(user_data any, signal os.Signal) bool {
	var conf *configuration = user_data.(*configuration)
	var i uint
	var queue *GAsyncQueue
	shutdown_triggered_mutex.Lock()
	if signal == syscall.SIGTERM {
		shutdown_triggered = true
	} else {
		if pause_mutex_per_thread == nil {
			pause_mutex_per_thread = make([]*sync.Mutex, NumThreads)
			for i = 0; i < NumThreads; i++ {
				pause_mutex_per_thread[i] = G_mutex_new()
			}
		}
		if conf.pause_resume == nil {
			conf.pause_resume = G_async_queue_new(BufferSize)
		}
		queue = conf.pause_resume
		for i = 0; i < NumThreads; i++ {
			pause_mutex_per_thread[i].Lock()
			G_async_queue_push(queue, pause_mutex_per_thread[i])
		}
		fmt.Printf("Ctrl+c detected! Are you sure you want to cancel(Y/N)?")
		var c string
		for {
			_, _ = fmt.Scanln(&c)
			if c == "N" || c == "n" {
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Unlock()
				}
				shutdown_triggered_mutex.Unlock()
				return true
			}
			if c == "Y" || c == "y" {
				shutdown_triggered = true
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Unlock()
				}
				break
			}
		}

	}
	inform_restore_job_running()
	create_index_shutdown_job(conf)
	log.Infof("Writing resume.partial file")
	var filename string
	var p = "resume.partial"
	var p2 = "resume"
	outfile, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0660)
	filename = G_async_queue_pop(file_list_to_do).(string)
	for strings.Compare(filename, "NO_MORE_FILES") != 0 {
		log.Debugf("Adding %s to resume file", filename)
		outfile.WriteString(fmt.Sprintf("%s\n", filename))
		filename = G_async_queue_pop(file_list_to_do).(string)
	}
	outfile.Close()
	err = os.Rename(p, p2)
	if err != nil {
		log.Criticalf("Error renaming resume.partial to resume")
	}
	log.Infof("Shutting down gracefully completed.")
	shutdown_triggered_mutex.Unlock()
	return false
}

func stop_signal_thread() {
	shutdown_triggered_mutex.Lock()
	shutdown_triggered_mutex.Unlock()
}

func restore_job_finish() {
	if shutdown_triggered {
		G_async_queue_push(file_list_to_do, "NO_MORE_FILES")
	}
}
