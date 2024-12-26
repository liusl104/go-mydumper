package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func (o *OptionEntries) initialize_worker_index(conf *configuration) {
	var n uint = 0
	//  index_mutex = g_mutex_new();
	o.global.init_connection_mutex = g_mutex_new()

	o.global.index_threads = make([]*GThreadFunc, 0)
	o.global.index_td = make([]*thread_data, o.MaxThreadsForIndexCreation)
	o.global.innodb_optimize_keys_all_tables_queue = g_async_queue_new(o.BufferSize)
	for n = 0; n < o.MaxThreadsForIndexCreation; n++ {
		o.global.index_td[n] = new(thread_data)
		o.global.index_threads[n] = g_thread_new("myloader_index", new(sync.WaitGroup), n)
		initialize_thread_data(o.global.index_td[n], conf, WAITING, n+1+o.NumThreads+o.MaxThreadsForSchemaCreation, nil)
		// g_thread_new("myloader_index_thread", worker_index_thread()) {
		go o.worker_index_thread(o.global.index_td[n], n)

	}
}

func (o *OptionEntries) process_index(td *thread_data) bool {
	var job = td.conf.index_queue.pop().(*control_job)
	if job.job_type == JOB_SHUTDOWN {
		log.Tracef("index_queue -> %v", job.job_type)
		return false
	}
	g_assert(job.job_type == JOB_RESTORE)
	var dbt = job.data.restore_job.dbt
	log.Tracef("index_queue -> %v: %s.%s", job.data.restore_job.job_type, dbt.database.real_database, dbt.table)
	dbt.start_index_time = time.Now()
	log.Infof("restoring index: %s.%s", dbt.database.name, dbt.table)
	o.process_job(td, job, nil)
	dbt.finish_time = time.Now()
	dbt.mutex.Lock()
	dbt.schema_state = ALL_DONE
	dbt.mutex.Unlock()
	return true
}

func (o *OptionEntries) worker_index_thread(td *thread_data, thread_id uint) {
	defer o.global.index_threads[thread_id].thread.Done()
	var conf = td.conf
	o.global.init_connection_mutex.Lock()
	o.global.init_connection_mutex.Unlock()
	g_async_queue_push(conf.ready, 1)
	if o.global.innodb_optimize_keys_all_tables {
		g_async_queue_pop(o.global.innodb_optimize_keys_all_tables_queue)
	}
	log.Tracef("I-Thread %d: Starting import", td.thread_id)
	var cont = true
	for cont {
		cont = o.process_index(td)
	}
	log.Tracef("I-Thread %d: ending", td.thread_id)

}

func (o *OptionEntries) create_index_shutdown_job(conf *configuration) {
	var n uint
	for n = 0; n < o.MaxThreadsForIndexCreation; n++ {
		g_async_queue_push(conf.index_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}

func (o *OptionEntries) wait_index_worker_to_finish() {
	var n uint
	for n = 0; n < o.MaxThreadsForIndexCreation; n++ {
		o.global.index_threads[n].thread.Wait()
	}
}

func (o *OptionEntries) start_innodb_optimize_keys_all_tables() {
	var n uint
	for n = 0; n < o.MaxThreadsForIndexCreation; n++ {
		g_async_queue_push(o.global.innodb_optimize_keys_all_tables_queue, 1)
	}
}

func (o *OptionEntries) free_index_worker_threads() {
	o.global.index_td = nil
	o.global.index_threads = nil
}
