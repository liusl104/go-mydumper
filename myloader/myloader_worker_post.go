package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

func (o *OptionEntries) initialize_post_loding_threads(conf *configuration) {
	var n uint
	o.global.init_connection_mutex = g_mutex_new()
	// o.global.post_threads = make([]*sync.WaitGroup, o.Threads.MaxThreadsForPostCreation)
	o.global.post_td = make([]*thread_data, o.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining = int64(o.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining1 = int64(o.MaxThreadsForPostCreation)
	o.global.sync_threads_remaining2 = int64(o.MaxThreadsForPostCreation)
	o.global.sync_mutex = g_mutex_new()
	o.global.sync_mutex1 = g_mutex_new()
	o.global.sync_mutex2 = g_mutex_new()
	o.global.sync_mutex.Lock()
	o.global.sync_mutex1.Lock()
	o.global.sync_mutex2.Lock()
	o.global.post_threads = make([]*GThreadFunc, 0)
	for n = 0; n < o.MaxThreadsForPostCreation; n++ {
		o.global.post_threads[n] = g_thread_new("myloader_post", new(sync.WaitGroup), n)
		o.global.post_td[n] = new(thread_data)
		initialize_thread_data(o.global.post_td[n], conf, WAITING, n+1+o.NumThreads+o.MaxThreadsForSchemaCreation+o.MaxThreadsForIndexCreation, nil)
		go o.worker_post_thread(o.global.post_td[n], n)
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

func (o *OptionEntries) worker_post_thread(td *thread_data, thread_id uint) {
	defer o.global.post_threads[thread_id].thread.Done()
	var conf *configuration = td.conf

	g_async_queue_push(conf.ready, 1)
	var cont bool = true
	var job *control_job

	log.Infof("Thread %d: Starting post import task over table", td.thread_id)
	cont = true
	for cont {
		job = g_async_queue_pop(conf.post_table_queue).(*control_job)
		cont = o.process_job(td, job, nil)
	}

	cont = true
	for cont {
		job = g_async_queue_pop(conf.post_queue).(*control_job)
		cont = o.process_job(td, job, nil)
	}
	sync_threads(&o.global.sync_threads_remaining2, o.global.sync_mutex2)
	cont = true
	for cont {
		job = g_async_queue_pop(conf.view_queue).(*control_job)
		cont = o.process_job(td, job, nil)
	}

	log.Tracef("Thread %d: ending", td.thread_id)
}

func (o *OptionEntries) create_post_shutdown_job(conf *configuration) {
	var n uint
	for n = 0; n < o.MaxThreadsForPostCreation; n++ {
		g_async_queue_push(conf.post_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		g_async_queue_push(conf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		g_async_queue_push(conf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}

func (o *OptionEntries) wait_post_worker_to_finish() {
	var n uint
	for n = 0; n < o.MaxThreadsForPostCreation; n++ {
		o.global.post_threads[n].thread.Wait()
	}
}

func (o *OptionEntries) free_post_worker_threads() {
	o.global.post_td = nil
	o.global.post_threads = nil
}
