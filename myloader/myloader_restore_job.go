package myloader

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
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
	statement string
	object    string
}

func initialize_restore_job(o *OptionEntries, pm_str string) {
	o.global.file_list_to_do = g_async_queue_new(o.Common.BufferSize)
	o.global.single_threaded_create_table = g_mutex_new()
	o.global.progress_mutex = g_mutex_new()
	o.global.shutdown_triggered_mutex = g_mutex_new()
	o.global.detailed_errors = new(restore_errors)
	if pm_str != "" {
		switch strings.ToUpper(pm_str) {
		case "TRUNCATE":
			o.global.purge_mode = TRUNCATE
		case "DROP":
			o.global.purge_mode = DROP
		case "DELETE":
			o.global.purge_mode = DELETE
		case "NONE":
			o.global.purge_mode = NONE
		case "FAIL":
			o.global.purge_mode = FAIL
		default:
			log.Errorf("Purge mode unknown")
		}
	} else if o.Execution.OverwriteTables {
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

func new_schema_restore_job_internal(database *database, statement string, object string) *schema_restore_job {
	var rj = new(schema_restore_job)
	rj.database = database
	rj.statement = statement
	rj.object = object
	return rj
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

func new_schema_restore_job(filename string, job_type restore_job_type, dbt *db_table, database *database, statement string, object string) *restore_job {
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

func overwrite_table(o *OptionEntries, conn *client.Conn, database string, table string) bool {
	var truncate_or_delete_failed bool
	var query string
	if o.global.purge_mode == DROP {
		log.Infof("Dropping table or view (if exists) `%s`.`%s`", database, table)
		query = fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", database, table)
		m_query(conn, query, m_critical, "Drop table failed")
		query = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s`", database, table)
		m_query(conn, query, m_critical, "Drop view failed")
	} else if o.global.purge_mode == TRUNCATE {
		log.Infof("Truncating table `%s`.`%s`", database, table)
		query = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", database, table)
		truncate_or_delete_failed = m_query(conn, query, m_warning, "TRUNCATE TABLE failed")
		if truncate_or_delete_failed {
			log.Warnf("Truncate failed, we are going to try to create table or view")
		}
	} else if o.global.purge_mode == DELETE {
		log.Infof("Deleting content of table `%s`.`%s`", database, table)
		query = fmt.Sprintf("DELETE FROM `%s`.`%s`", database, table)
		truncate_or_delete_failed = m_query(conn, query, m_warning, "DELETE failed")
		if truncate_or_delete_failed {
			log.Warnf("Delete failed, we are going to try to create table or view")
		}
	}
	return truncate_or_delete_failed
}

func is_all_done(key any, dbt *db_table, total *uint) {
	_ = key
	if dbt.schema_state >= ALL_DONE {
		*total = *total + 1
	}
}

func increse_object_error(o *OptionEntries, object string) {
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

func get_total_done(conf *configuration, total *uint) {
	conf.table_hash_mutex.Lock()
	var dbt *db_table
	var key string
	for key, dbt = range conf.table_hash {
		is_all_done(key, dbt, total)
	}
	conf.table_hash_mutex.Unlock()
}

func process_restore_job(o *OptionEntries, td *thread_data, rj *restore_job) {
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
		return
	}
	var dbt = rj.dbt
	var query_counter uint
	var total uint
	td.status = STARTED
	switch rj.job_type {
	case JOB_RESTORE_STRING:
		get_total_done(td.conf, &total)
		log.Infof("Thread %d: restoring %s `%s`.`%s` from %s. Tables %d of %d completed", td.thread_id, rj.data.srj.object,
			dbt.database.real_database, dbt.real_table, rj.filename, total, len(td.conf.table_hash))
		if restore_data_in_gstring(o, td, rj.data.srj.statement, false, &query_counter) != 0 {
			increse_object_error(o, rj.data.srj.object)
			log.Infof("Failed %s: %s", rj.data.srj.object, rj.data.srj.statement)
		}
		free_schema_restore_job(rj.data.srj)
	case JOB_TO_CREATE_TABLE:
		dbt.schema_state = CREATING
		if o.Execution.SerialTblCreation {
			o.global.single_threaded_create_table.Lock()
		}
		log.Infof("Thread %d: restoring table `%s`.`%s` from %s", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename)
		var truncate_or_delete_failed bool
		if o.Execution.OverwriteTables {
			truncate_or_delete_failed = overwrite_table(o, td.thrconn, dbt.database.real_database, dbt.real_table)
		}
		if (o.global.purge_mode == TRUNCATE || o.global.purge_mode == DELETE) && !truncate_or_delete_failed {
			log.Infof("Skipping table creation `%s`.`%s` from %s", dbt.database.real_database, dbt.real_table, rj.filename)
		} else {
			log.Infof("Thread %d: Creating table `%s`.`%s` from content in %s.", td.thread_id, dbt.database.real_database, dbt.real_table, rj.filename)
			if restore_data_in_gstring(o, td, rj.data.srj.statement, false, &query_counter) != 0 {
				atomic.AddUint64(&o.global.detailed_errors.schema_errors, 1)
				if o.global.purge_mode == FAIL {
					log.Errorf("Thread %d: issue restoring %s: %v", td.thread_id, rj.filename, td.err)
				} else {
					log.Fatalf("Thread %d: issue restoring %s: %v", td.thread_id, rj.filename, td.err)
				}
			} else {
				get_total_done(td.conf, &total)
				log.Infof("Thread %d: Table `%s`.`%s` created. Tables %d of %d completed", td.thread_id, dbt.database.real_database, dbt.real_table, total, len(td.conf.table_hash))
			}
		}
		dbt.schema_state = CREATED
		if o.Execution.SerialTblCreation {
			o.global.single_threaded_create_table.Unlock()
		}
		free_schema_restore_job(rj.data.srj)
	case JOB_RESTORE_FILENAME:
		o.global.progress_mutex.Lock()
		o.global.progress++
		get_total_done(td.conf, &total)
		log.Infof("Thread %d: restoring `%s`.`%s` part %d of %d from %s. Progress %d of %d. Tables %d of %d completed", td.thread_id,
			dbt.database.real_database, dbt.real_table, rj.data.drj.index, dbt.count, rj.filename, o.global.progress, o.global.total_data_sql_files, total, len(td.conf.table_hash))
		o.global.progress_mutex.Unlock()

		if restore_data_from_file(o, td, dbt.database.real_database, dbt.real_table, rj.filename, false) > 0 {
			atomic.AddUint64(&o.global.detailed_errors.data_errors, 1)
			log.Fatalf("Thread %d: issue restoring %s: %s", td.thread_id, rj.filename, td.err)
		}
		g_atomic_int_dec_and_test(&dbt.remaining_jobs)
	case JOB_RESTORE_SCHEMA_FILENAME:
		get_total_done(td.conf, &total)
		log.Infof("Thread %d: restoring %s on `%s` from %s. Tables %d of %d completed", td.thread_id, rj.data.srj.object,
			rj.data.srj.database.real_database, rj.filename, total, len(td.conf.table_hash))
		if restore_data_from_file(o, td, rj.data.srj.database.real_database, "", rj.filename, true) > 0 {
			increse_object_error(o, rj.data.srj.object)
		}
		free_schema_restore_job(rj.data.srj)
	default:
		log.Fatalf("Something very bad happened!")
	}
}

func signal_thread(o *OptionEntries, data any) {
	if o.global.signal_thread == nil {
		o.global.signal_thread = new(sync.WaitGroup)
	}
	o.global.signal_thread.Add(1)
	defer o.global.signal_thread.Done()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	sig_triggered(o, data, sig)
	log.Infof("Ending signal thread")
}

func sig_triggered(o *OptionEntries, user_data any, signal os.Signal) bool {
	var i uint
	var queue *asyncQueue
	o.global.shutdown_triggered_mutex.Lock()
	if signal == syscall.SIGTERM {
		o.global.shutdown_triggered = true
	} else {
		if o.global.pause_mutex_per_thread == nil || len(o.global.pause_mutex_per_thread) == 0 {
			o.global.pause_mutex_per_thread = make([]*sync.Mutex, o.Common.NumThreads)
			for i = 0; i < o.Common.NumThreads; i++ {
				o.global.pause_mutex_per_thread[i] = g_mutex_new()
			}
		}
		if user_data.(*configuration).pause_resume == nil {
			user_data.(*configuration).pause_resume = g_async_queue_new(o.Common.BufferSize)
		}
		queue = user_data.(*configuration).pause_resume
		for i = 0; i < o.Common.NumThreads; i++ {
			o.global.pause_mutex_per_thread[i].Lock()
			queue.push(o.global.pause_mutex_per_thread[i])
		}
		fmt.Printf("Ctrl+c detected! Are you sure you want to cancel(Y/N)?")
		var c string
		for {
			_, _ = fmt.Scanln(&c)
			if c == "N" || c == "n" {
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				o.global.shutdown_triggered_mutex.Unlock()
				return true
			}
			if c == "Y" || c == "y" {
				o.global.shutdown_triggered = true
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				break
			}
		}

	}
	inform_restore_job_running(o)
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

func restore_job_finish(o *OptionEntries) {
	if o.global.shutdown_triggered {
		o.global.file_list_to_do.push("NO_MORE_FILES")
	}
}
