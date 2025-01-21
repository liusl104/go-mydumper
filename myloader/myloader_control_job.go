package myloader

import (
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"sync"
	"sync/atomic"
)

type control_job_type int

const (
	JOB_RESTORE control_job_type = iota
	JOB_WAIT
	JOB_SHUTDOWN
)

type control_job_data struct {
	restore_job *restore_job
	queue       *GAsyncQueue
}

type control_job struct {
	job_type     control_job_type
	data         *control_job_data
	use_database *database
}

var (
	cjt_mutex             *sync.Mutex
	cjt_cond              *sync.WaitGroup
	control_job_t         *sync.WaitGroup
	cjt_paused            bool
	all_jobs_are_enqueued bool
	last_wait             int64
)

func cjt_resume() {
	cjt_mutex.Lock()
	cjt_paused = false
	cjt_cond.Add(1)
	cjt_mutex.Unlock()
}

func initialize_control_job(conf *configuration) {
	refresh_db_queue = G_async_queue_new(BufferSize)
	here_is_your_job = G_async_queue_new(BufferSize)
	last_wait = int64(NumThreads)
	data_queue = G_async_queue_new(BufferSize)
	cjt_mutex = G_mutex_new()
	cjt_cond = new(sync.WaitGroup)
	control_job_t = new(sync.WaitGroup)
	go control_job_thread(conf)

}
func wait_control_job() {
	control_job_t.Wait()
	cjt_mutex = nil
	cjt_cond = nil
}

func new_control_job(job_type control_job_type, job_data any, use_database *database) *control_job {
	var j = new(control_job)
	j.job_type = job_type
	j.use_database = use_database
	switch job_type {
	case JOB_WAIT:
		j.data.queue = job_data.(*GAsyncQueue)
	case JOB_SHUTDOWN:
		break
	default:
		j.data.restore_job = job_data.(*restore_job)
	}
	return j
}

func process_job(td *thread_data, job *control_job, retry *bool) bool {
	switch job.job_type {
	case JOB_RESTORE:
		var res bool = process_restore_job(td, job.data.restore_job)
		if *retry {
			*retry = res
		}
		return true
	case JOB_WAIT:
		G_async_queue_push(td.conf.ready, 1)
		G_async_queue_pop(job.data.queue)
		break
	case JOB_SHUTDOWN:
		return false
	default:
		log.Error("Something very bad happened!(1)")
	}
	return true
}

func schema_file_missed_lets_continue(td *thread_data) {
	td.conf.table_list_mutex.Lock()
	var i int
	var dbt *db_table
	for _, dbt = range td.conf.table_list {
		dbt.mutex.Lock()
		dbt.schema_state = CREATED
		for i = 0; i < dbt.restore_job_list.Len(); i++ {
			G_async_queue_push(td.conf.stream_queue, dbt)
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
}

func are_we_waiting_for_schema_jobs_to_complete(td *thread_data) bool {
	if G_async_queue_length(td.conf.database_queue) > 0 ||
		G_async_queue_length(td.conf.table_queue) > 0 ||
		G_async_queue_length(td.conf.retry_queue) > 0 {
		return true
	}
	td.conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range td.conf.table_list {
		dbt.mutex.Lock()
		if dbt.schema_state == CREATING {
			dbt.mutex.Unlock()
			td.conf.table_list_mutex.Unlock()
			return true
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
	return false
}

func are_we_waiting_for_create_schema_jobs_to_complete(td *thread_data) bool {
	if G_async_queue_length(td.conf.database_queue) > 0 {
		return true
	}
	td.conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range td.conf.table_list {
		dbt.mutex.Lock()
		if dbt.schema_state == CREATING {
			dbt.mutex.Unlock()
			td.conf.table_list_mutex.Unlock()
			return true
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
	return false
}

func are_available_jobs(td *thread_data) bool {
	td.conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range td.conf.table_list {
		dbt.mutex.Lock()
		if dbt.schema_state != CREATED || dbt.restore_job_list.Len() > 0 {
			dbt.mutex.Unlock()
			td.conf.table_list_mutex.Unlock()
			return true
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
	return false
}

func give_me_next_data_job_conf(conf *configuration, rj **restore_job) bool {
	var giveup = true
	conf.table_list_mutex.Lock()
	var dbt *db_table
	var job *restore_job
	for _, dbt = range conf.table_list {
		if dbt.database.schema_state == NOT_FOUND {
			log.Criticalf("%s.%s: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			continue
		}
		if dbt.schema_state >= DATA_DONE || (dbt.schema_state == CREATED && (dbt.is_view || dbt.is_sequence)) {
			log.Tracef("%s.%s done: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			dbt.mutex.Unlock()
			continue
		}
		dbt.mutex.Lock()
		if !Resume && dbt.schema_state < CREATED {
			giveup = false
			log.Tracef("%s.%s not yet created: %s, waiting", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			dbt.mutex.Unlock()
			continue
		}
		if dbt.schema_state >= DATA_DONE || (dbt.schema_state == CREATED && (dbt.is_view || dbt.is_sequence)) {
			log.Tracef("%s.%s done just now: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			dbt.mutex.Unlock()
			continue
		}
		if dbt.schema_state == CREATED && dbt.restore_job_list.Len() > 0 {
			if dbt.current_threads >= dbt.max_threads {
				giveup = false
				dbt.mutex.Unlock()
				continue
			}
			job = dbt.restore_job_list.Front().Value.(*restore_job)
			var current = dbt.restore_job_list.Front()
			dbt.restore_job_list.Remove(current)
			dbt.restore_job_list.Front().Next()
			dbt.current_threads++
			dbt.mutex.Unlock()
			giveup = false
			log.Tracef("%s.%s sending %v: %s, threads: %d, prohibiting finish", dbt.database.real_database, dbt.real_table,
				job.job_type, job.filename, dbt.current_threads)
			break
		} else {
			log.Tracef("No remaining jobs on %s.%s", dbt.database.real_database, dbt.real_table)
			if all_jobs_are_enqueued && dbt.current_threads == 0 && atomic.LoadInt64(&dbt.remaining_jobs) == 0 {
				dbt.schema_state = DATA_DONE
				var res bool = enqueue_index_for_dbt_if_possible(conf, dbt)
				if res {
					giveup = false
					log.Tracef("%s.%s queuing indexes, prohibiting finish", dbt.database.real_database, dbt.real_table)
				} else {
					log.Tracef("%s.%s skipping indexes, voting for finish", dbt.database.real_database, dbt.real_table)
				}
			}
		}
		dbt.mutex.Unlock()
	}

	conf.table_list_mutex.Unlock()
	*rj = job
	return giveup
}

func refresh_db_and_jobs(current_ft file_type) {
	switch current_ft {
	case SCHEMA_CREATE, SCHEMA_TABLE, SCHEMA_SEQUENCE:
		schema_queue_push(current_ft)
		break
	case DATA:
		log.Tracef("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
		break
	case INTERMEDIATE_ENDED:
		schema_queue_push(current_ft)
		log.Tracef("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
		break
	case SHUTDOWN:
		log.Tracef("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
	default:
		break
	}
}

func maybe_shutdown_control_job() {
	if G_atomic_int_dec_and_test(&last_wait) {
		log.Tracef("SHUTDOWN maybe_shutdown_control_job")
		refresh_db_and_jobs(SHUTDOWN)
	}
}

func wake_threads_waiting(threads_waiting *uint) {
	for *threads_waiting > 0 {
		log.Tracef("refresh_db_queue <- %v", THREAD)
		G_async_queue_push(refresh_db_queue, THREAD)
		*threads_waiting = *threads_waiting - 1
	}
}

func control_job_thread(conf *configuration) {
	defer control_job_t.Done()
	var ft file_type
	var rj *restore_job
	var _num_threads uint = NumThreads
	var threads_waiting uint
	var giveup bool
	var cont = true
	if OverwriteTables && !OverwriteUnsafe && cjt_paused {
		log.Tracef("Thread control_job_thread paused")
		cjt_mutex.Lock()
		for cjt_paused {
			cjt_cond.Wait()
		}
		cjt_mutex.Unlock()
	}
	log.Tracef("Thread control_job_thread started")
	for cont {
		task := G_async_queue_timeout_pop(refresh_db_queue, 10000000)
		if task == nil {
			ft = THREAD
		} else {
			ft = task.(file_type)
		}
		log.Tracef("refresh_db_queue -> %v (%d loaders waiting)", ft, threads_waiting)
		switch ft {
		case DATA:
			wake_threads_waiting(&threads_waiting)
			break
		case THREAD:
			giveup = give_me_next_data_job_conf(conf, &rj)
			if rj != nil {
				log.Tracef("job available in give_me_next_data_job_conf")
				if rj.dbt != nil {
					log.Tracef("data_queue <- %v: %s", rj.job_type, rj.dbt.table)
				} else {
					log.Tracef("data_queue <- %v: %s", rj.job_type, rj.filename)
				}
				G_async_queue_push(data_queue, rj)
				log.Tracef("here_is_your_job <- %v", DATA)
				G_async_queue_push(here_is_your_job, DATA)
			} else {
				log.Tracef("No job available")
				if all_jobs_are_enqueued && giveup {
					log.Tracef("Giving up...")
					control_job_ended = true
					var i uint
					for i = 0; i < _num_threads; i++ {
						log.Tracef("here_is_your_job <- %v", SHUTDOWN)
						G_async_queue_push(here_is_your_job, SHUTDOWN)
					}
				} else {
					log.Tracef("Thread will be waiting")
					for threads_waiting < _num_threads {
						threads_waiting++
					}
				}
			}
			break
		case INTERMEDIATE_ENDED:
			enqueue_indexes_if_possible(conf)
			all_jobs_are_enqueued = true
			wake_threads_waiting(&threads_waiting)
			break
		case SHUTDOWN:
			cont = false
			break
		default:
			log.Tracef("Thread control_job_thread: received Default: %v", ft)
			break
		}
	}
	start_innodb_optimize_keys_all_tables()
	log.Tracef("Thread control_job_thread finished")
	return
}
