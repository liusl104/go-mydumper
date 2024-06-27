package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func initialize_worker_index(o *OptionEntries, conf *configuration) {
	var n uint = 0
	//  index_mutex = g_mutex_new();
	o.global.init_connection_mutex = g_mutex_new()
	o.global.index_threads = new(sync.WaitGroup)
	o.global.index_td = make([]*thread_data, o.Threads.MaxThreadsForIndexCreation)
	o.global.innodb_optimize_keys_all_tables_queue = g_async_queue_new(o.Common.BufferSize)
	for n = 0; n < o.Threads.MaxThreadsForIndexCreation; n++ {
		o.global.index_td[n] = new(thread_data)
		o.global.index_td[n].conf = conf
		o.global.index_td[n].thread_id = n + 1 + o.Common.NumThreads + o.Threads.MaxThreadsForSchemaCreation
		o.global.index_td[n].status = WAITING
		o.global.index_threads.Add(1)
		go worker_index_thread(o, o.global.index_td[n])
	}
}

func process_index(o *OptionEntries, td *thread_data) bool {
	var job = td.conf.index_queue.pop().(*control_job)
	if job.job_type == JOB_SHUTDOWN {
		return false
	}
	var dbt = job.data.restore_job.dbt
	execute_use_if_needs_to(o, td, job.use_database, "Restoring index")
	dbt.start_index_time = time.Now()
	log.Infof("restoring index: %s.%s", dbt.database.name, dbt.table)
	process_job(o, td, job)
	dbt.finish_time = time.Now()
	dbt.mutex.Lock()
	dbt.schema_state = ALL_DONE
	dbt.mutex.Unlock()
	return true
}

func worker_index_thread(o *OptionEntries, td *thread_data) {
	defer o.global.index_threads.Done()
	var err error
	var conf = td.conf
	o.global.init_connection_mutex.Lock()
	td.thrconn = nil
	o.global.init_connection_mutex.Unlock()
	td.current_database = ""

	td.thrconn, err = m_connect(o)

	execute_gstring(td.thrconn, o.global.set_session)
	conf.ready.push(1)

	if o.Common.DB != "" {
		td.current_database = o.Common.DB
		if execute_use(td) {
			log.Fatalf("I-Thread %d: Error switching to database `%s` when initializing", td.thread_id, td.current_database)
		}
	}
	if o.global.innodb_optimize_keys_all_tables {
		o.global.innodb_optimize_keys_all_tables_queue.pop()
	}

	log.Debugf("I-Thread %d: Starting import", td.thread_id)
	var cont = true
	for cont {
		cont = process_index(o, td)
	}

	if td.thrconn != nil {
		err = td.thrconn.Close()
	}
	_ = err

	log.Debugf("I-Thread %d: ending", td.thread_id)

}

func create_index_shutdown_job(o *OptionEntries, conf *configuration) {
	var n uint
	for n = 0; n < o.Threads.MaxThreadsForIndexCreation; n++ {
		conf.index_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
	}
}

func wait_index_worker_to_finish(o *OptionEntries) {
	var n uint
	for n = 0; n < o.Threads.MaxThreadsForIndexCreation; n++ {
		o.global.innodb_optimize_keys_all_tables_queue.push(1)
	}
}

func start_innodb_optimize_keys_all_tables(o *OptionEntries) {
	var n uint
	for n = 0; n < o.Threads.MaxThreadsForIndexCreation; n++ {
		o.global.innodb_optimize_keys_all_tables_queue.push(1)
	}
}

func free_index_worker_threads(o *OptionEntries) {
	o.global.index_td = nil
	o.global.index_threads = nil
}
