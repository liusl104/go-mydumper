package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

func initialize_post_loding_threads(o *OptionEntries, conf *configuration) {
	var n uint
	o.global.init_connection_mutex = g_mutex_new()
	o.global.post_threads = make([]*sync.WaitGroup, o.Threads.MaxThreadsForPostCreation)
	o.global.post_td = make([]*thread_data, o.Threads.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining = int64(o.Threads.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining1 = int64(o.Threads.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining2 = int64(o.Threads.MaxThreadsForPostCreation)
	o.global.sync_mutex = g_mutex_new()
	o.global.sync_mutex1 = g_mutex_new()
	o.global.sync_mutex2 = g_mutex_new()
	o.global.sync_mutex.Lock()
	o.global.sync_mutex1.Lock()
	o.global.sync_mutex2.Lock()

	for n = 0; n < o.Threads.MaxThreadsForPostCreation; n++ {
		o.global.post_td[n].conf = conf
		o.global.post_td[n].thread_id = n + 1 + o.Common.NumThreads + o.Threads.MaxThreadsForSchemaCreation + o.Threads.MaxThreadsForIndexCreation
		o.global.post_td[n].status = WAITING
		o.global.post_threads[n] = new(sync.WaitGroup)
		go worker_post_thread(o, o.global.post_td[n], o.global.post_threads[n])
	}
}

func sync_threads(counter *int64, mutex *sync.Mutex) {
	if g_atomic_int_dec_and_test(counter) {
		mutex.Unlock()
	} else {
		mutex.Lock()
		mutex.Unlock()
	}
}

func worker_post_thread(o *OptionEntries, td *thread_data, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
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
			log.Fatalf("Thread %d: Error switching to database `%s` when initializing", td.thread_id, td.current_database)
		}
	}
	var cont = true
	var job *control_job
	log.Infof("Thread %d: Starting post import task over table", td.thread_id)
	cont = true
	for cont {
		job = conf.post_table_queue.pop().(*control_job)
		execute_use_if_needs_to(o, td, job.use_database, "Restoring post table")
		cont = process_job(o, td, job)
	}
	cont = true
	for cont {
		job = conf.post_queue.pop().(*control_job)
		execute_use_if_needs_to(o, td, job.use_database, "Restoring post tasks")
		cont = process_job(o, td, job)
	}
	sync_threads(&o.global.sync_threads_remaining2, o.global.sync_mutex2)
	cont = true
	for cont {
		job = conf.view_queue.pop().(*control_job)
		execute_use_if_needs_to(o, td, job.use_database, "Restoring view tasks")
		cont = process_job(o, td, job)
	}

	if td.thrconn != nil {
		err = td.thrconn.Close()
	}
	_ = err
	log.Debugf("Thread %d: ending", td.thread_id)
}

func create_post_shutdown_job(o *OptionEntries, conf *configuration) {
	var n uint
	for n = 0; n < o.Threads.MaxThreadsForPostCreation; n++ {
		conf.post_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
		conf.post_table_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
		conf.view_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
	}
}

func wait_post_worker_to_finish(o *OptionEntries) {
	var n uint
	for n = 0; n < o.Threads.MaxThreadsForPostCreation; n++ {
		o.global.post_threads[n].Wait()
	}

}

func free_post_worker_threads(o *OptionEntries) {
	o.global.post_td = nil
	o.global.post_threads = nil
}
