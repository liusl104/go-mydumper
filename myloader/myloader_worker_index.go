package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"sync"
	"time"
)

var (
	innodb_optimize_keys_all_tables_queue *GAsyncQueue
	index_threads                         []*GThread
	index_td                              []*thread_data
	init_connection_mutex                 *sync.Mutex
)

// initialize_worker_index creates MaxThreadsForIndexCreation index worker threads (worker_index_thread) and innodb_optimize_keys_all_tables_queue.
func initialize_worker_index(conf *configuration) {
	var n uint = 0
	init_connection_mutex = G_mutex_new()
	index_threads = make([]*GThread, MaxThreadsForIndexCreation)
	index_td = make([]*thread_data, MaxThreadsForIndexCreation)
	innodb_optimize_keys_all_tables_queue = G_async_queue_new("innodb_optimize_keys_all_tables_queue")
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		index_td[n] = new(thread_data)
		initialize_thread_data(index_td[n], conf, WAITING, n+1+NumThreads+MaxThreadsForSchemaCreation, nil)
		index_threads[n] = M_thread_new("myloader_index", worker_index_thread, index_td[n], "Index thread could not be created")
	}
}

// process_index pops a control job from index_queue; if JOB_SHUTDOWN returns false; otherwise runs process_job and sets table schema_state to ALL_DONE.
func process_index(td *thread_data) bool {
	var job = G_async_queue_pop(td.conf.index_queue).(*control_job)
	if job.job_type == JOB_SHUTDOWN {
		log.Debugf("index_queue -> %s", jtype2str(job.job_type))
		return false
	}
	G_assert(job.job_type == JOB_RESTORE)
	var dbt = job.data.restore_job.dbt
	log.Debugf("index_queue -> %s: %s.%s", rjtype2str(job.data.restore_job.job_type), dbt.database.real_database, dbt.table)
	dbt.start_index_time = time.Now()
	log.Infof("restoring index: %s.%s", dbt.database.name, dbt.table)
	process_job(td, job, nil)
	dbt.finish_time = time.Now()
	dbt.mutex.Lock()
	dbt.schema_state = ALL_DONE
	dbt.mutex.Unlock()
	return true
}

// worker_index_thread signals ready, then loops processing index jobs and pushing REQUEST_DATA_JOB until shutdown.
func worker_index_thread(c any) {
	td := c.(*thread_data)
	var cnf = td.conf
	init_connection_mutex.Lock()
	init_connection_mutex.Unlock()
	G_async_queue_push(cnf.ready, 1)
	if optimize_keys_all_tables {
		G_async_queue_pop(innodb_optimize_keys_all_tables_queue)
	}
	log.Debugf("I-Thread %d: Starting import", td.thread_id)
	var cont = true
	for cont {
		cont = process_index(td)
		enroute_into_the_right_queue_based_on_file_type(REQUEST_DATA_JOB)
	}
	log.Debugf("I-Thread %d: ending", td.thread_id)

}

// create_index_shutdown_job pushes JOB_SHUTDOWN to conf.index_queue for each index worker.
func create_index_shutdown_job(conf *configuration) {
	var n uint
	log.Debugf("Sending SHUTDOWN to index threads")
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		G_async_queue_push(conf.index_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}

// wait_index_worker_to_finish joins all index worker threads.
func wait_index_worker_to_finish() {
	var n uint
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		G_thread_join(index_threads[n])
	}
}

// start_optimize_keys_all_tables pushes one item to innodb_optimize_keys_all_tables_queue per index thread (unblocks them).
func start_optimize_keys_all_tables() {
	var n uint
	log.Debugf("optimize_keys_all_tables_queue <- 1 (%d times)", MaxThreadsForIndexCreation)
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		G_async_queue_push(innodb_optimize_keys_all_tables_queue, 1)
	}
}

// create_index_job creates a restore job for the table's indexes and pushes it to conf.index_queue; sets dbt.schema_state to INDEX_ENQUEUED.
func create_index_job(conf *configuration, dbt *db_table, tdid uint) bool {
	log.Infof("Thread %d: Enqueuing index for table: %s.%s", tdid, dbt.database.real_database, dbt.table)
	var rj *restore_job = new_schema_restore_job("index", JOB_RESTORE_STRING, dbt, dbt.database, dbt.indexes, INDEXES)
	log.Debugf("index_queue <- %s: %s.%s", rjtype2str(rj.job_type), dbt.database.real_database, dbt.table)
	G_async_queue_push(conf.index_queue, new_control_job(JOB_RESTORE, rj, dbt.database))
	dbt.schema_state = INDEX_ENQUEUED
	return true
}

// enqueue_index_for_dbt_if_possible if dbt.schema_state is DATA_DONE, either sets ALL_DONE (no indexes) or creates an index job via create_index_job.
func enqueue_index_for_dbt_if_possible(conf *configuration, dbt *db_table) {
	if dbt.schema_state == DATA_DONE {
		if dbt.indexes == nil {
			dbt.schema_state = ALL_DONE
		} else {
			create_index_job(conf, dbt, 0)
		}
	}
	// return dbt.schema_state != ALL_DONE
}

// enqueue_indexes_if_possible iterates conf.table_list and calls enqueue_index_for_dbt_if_possible for each table.
func enqueue_indexes_if_possible(conf *configuration) {
	conf.table_list_mutex.Lock()
	for _, dbt := range conf.table_list {
		dbt.mutex.Lock()
		enqueue_index_for_dbt_if_possible(conf, dbt)
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

// free_index_worker_threads clears index_td and index_threads.
func free_index_worker_threads() {
	index_td = nil
	index_threads = nil
}
