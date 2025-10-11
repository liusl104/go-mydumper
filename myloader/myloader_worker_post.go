package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"sync"
)

var (
	sync_mutex              *sync.Mutex
	sync_mutex1             *sync.Mutex
	sync_mutex2             *sync.Mutex
	sync_threads_remaining  int64
	sync_threads_remaining1 int64
	sync_threads_remaining2 int64
	post_td                 []*thread_data
	post_threads            []*GThread
)

func initialize_post_loding_threads(conf *configuration) {
	var n uint
	// post_threads = make([]*sync.WaitGroup, Threads.MaxThreadsForPostCreation)
	post_threads = make([]*GThread, MaxThreadsForPostCreation)
	post_td = make([]*thread_data, MaxThreadsForPostCreation)
	sync_threads_remaining = int64(MaxThreadsForPostCreation)
	sync_threads_remaining1 = int64(MaxThreadsForPostCreation)
	sync_threads_remaining2 = int64(MaxThreadsForPostCreation)
	sync_mutex = G_mutex_new()
	sync_mutex1 = G_mutex_new()
	sync_mutex2 = G_mutex_new()
	sync_mutex.Lock()
	sync_mutex1.Lock()
	sync_mutex2.Lock()
	for n = 0; n < MaxThreadsForPostCreation; n++ {
		post_td[n] = new(thread_data)
		initialize_thread_data(post_td[n], conf, WAITING, n+1+NumThreads+MaxThreadsForSchemaCreation+MaxThreadsForIndexCreation, nil)
		post_threads[n] = M_thread_new("myloader_post", worker_post_thread, post_td[n], "Post thread could not be created")
	}
}

func sync_threads(counter *int64, mutex *sync.Mutex) {
	if G_atomic_int_dec_and_test(counter) {
		mutex.Unlock()
	} else {
		mutex.Lock()
		mutex.Unlock()
	}
}

func worker_post_thread(c any) {
	td := c.(*thread_data)
	var cnf *configuration = td.conf

	G_async_queue_push(conf.ready, 1)
	var cont bool = true
	var job *control_job

	log.Infof("Thread %d: Starting post import task over table", td.thread_id)
	cont = true
	for cont {
		job = G_async_queue_pop(cnf.post_table_queue).(*control_job)
		cont = process_job(td, job, nil)
	}

	cont = true
	for cont {
		job = G_async_queue_pop(cnf.post_queue).(*control_job)
		cont = process_job(td, job, nil)
	}
	sync_threads(&sync_threads_remaining2, sync_mutex2)
	cont = true
	for cont {
		job = G_async_queue_pop(cnf.view_queue).(*control_job)
		cont = process_job(td, job, nil)
	}

	log.Tracef("Thread %d: ending", td.thread_id)
}

func create_post_shutdown_job(conf *configuration) {
	var n uint
	for n = 0; n < MaxThreadsForPostCreation; n++ {
		G_async_queue_push(conf.post_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(conf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(conf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}

func wait_post_worker_to_finish() {
	var n uint
	for n = 0; n < MaxThreadsForPostCreation; n++ {
		G_thread_join(post_threads[n])
	}
}

func free_post_worker_threads() {
	post_td = nil
	post_threads = nil
}
