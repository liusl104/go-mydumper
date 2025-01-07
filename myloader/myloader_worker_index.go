package myloader

import (
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"sync"
	"time"
)

var (
	innodb_optimize_keys_all_tables_queue *GAsyncQueue
	index_threads                         []*GThreadFunc
	index_td                              []*thread_data
)

func initialize_worker_index(conf *configuration) {
	var n uint = 0
	//  index_mutex = g_mutex_new();
	init_connection_mutex = G_mutex_new()

	index_threads = make([]*GThreadFunc, 0)
	index_td = make([]*thread_data, MaxThreadsForIndexCreation)
	innodb_optimize_keys_all_tables_queue = G_async_queue_new(BufferSize)
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		index_td[n] = new(thread_data)
		index_threads[n] = G_thread_new("myloader_index", new(sync.WaitGroup), n)
		initialize_thread_data(index_td[n], conf, WAITING, n+1+NumThreads+MaxThreadsForSchemaCreation, nil)
		// g_thread_new("myloader_index_thread", worker_index_thread()) {
		go worker_index_thread(index_td[n], n)

	}
}

func process_index(td *thread_data) bool {
	var job = G_async_queue_pop(td.conf.index_queue).(*control_job)
	if job.job_type == JOB_SHUTDOWN {
		log.Tracef("index_queue -> %v", job.job_type)
		return false
	}
	G_assert(job.job_type == JOB_RESTORE)
	var dbt = job.data.restore_job.dbt
	log.Tracef("index_queue -> %v: %s.%s", job.data.restore_job.job_type, dbt.database.real_database, dbt.table)
	dbt.start_index_time = time.Now()
	log.Infof("restoring index: %s.%s", dbt.database.name, dbt.table)
	process_job(td, job, nil)
	dbt.finish_time = time.Now()
	dbt.mutex.Lock()
	dbt.schema_state = ALL_DONE
	dbt.mutex.Unlock()
	return true
}

func worker_index_thread(td *thread_data, thread_id uint) {
	defer index_threads[thread_id].Thread.Done()
	var conf = td.conf
	init_connection_mutex.Lock()
	init_connection_mutex.Unlock()
	G_async_queue_push(conf.ready, 1)
	if innodb_optimize_keys_all_tables {
		G_async_queue_pop(innodb_optimize_keys_all_tables_queue)
	}
	log.Tracef("I-Thread %d: Starting import", td.thread_id)
	var cont = true
	for cont {
		cont = process_index(td)
	}
	log.Tracef("I-Thread %d: ending", td.thread_id)

}

func create_index_shutdown_job(conf *configuration) {
	var n uint
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		G_async_queue_push(conf.index_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}

func wait_index_worker_to_finish() {
	var n uint
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		index_threads[n].Thread.Wait()
	}
}

func start_innodb_optimize_keys_all_tables() {
	var n uint
	for n = 0; n < MaxThreadsForIndexCreation; n++ {
		G_async_queue_push(innodb_optimize_keys_all_tables_queue, 1)
	}
}

func free_index_worker_threads() {
	index_td = nil
	index_threads = nil
}
