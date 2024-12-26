package myloader

import (
	log "github.com/sirupsen/logrus"
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
	queue       *asyncQueue
}

type control_job struct {
	job_type     control_job_type
	data         *control_job_data
	use_database *database
}

func (o *OptionEntries) cjt_resume() {
	o.global.cjt_mutex.Lock()
	o.global.cjt_paused = false
	o.global.cjt_cond.Add(1)
	o.global.cjt_mutex.Unlock()
}

func (o *OptionEntries) initialize_control_job(conf *configuration) {
	o.global.refresh_db_queue = g_async_queue_new(o.BufferSize)
	o.global.here_is_your_job = g_async_queue_new(o.BufferSize)
	o.global.last_wait = int64(o.NumThreads)
	o.global.data_queue = g_async_queue_new(o.BufferSize)
	o.global.cjt_mutex = g_mutex_new()
	o.global.cjt_cond = g_cond_new()
	o.global.control_job_t = new(sync.WaitGroup)
	go o.control_job_thread(conf)

}
func (o *OptionEntries) wait_control_job() {
	o.global.control_job_t.Wait()
	o.global.cjt_mutex = nil
	o.global.cjt_cond = nil
}

func new_control_job(job_type control_job_type, job_data any, use_database *database) *control_job {
	var j = new(control_job)
	j.job_type = job_type
	j.use_database = use_database
	switch job_type {
	case JOB_WAIT:
		j.data.queue = job_data.(*asyncQueue)
	case JOB_SHUTDOWN:
		break
	default:
		j.data.restore_job = job_data.(*restore_job)
	}
	return j
}

func (o *OptionEntries) process_job(td *thread_data, job *control_job, retry *bool) bool {
	switch job.job_type {
	case JOB_RESTORE:
		var res bool = o.process_restore_job(td, job.data.restore_job)
		if *retry {
			*retry = res
		}
		return true
	case JOB_WAIT:
		g_async_queue_push(td.conf.ready, 1)
		g_async_queue_pop(job.data.queue)
		break
	case JOB_SHUTDOWN:
		return false
	default:
		log.Fatalf("Something very bad happened!(1)")
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
			g_async_queue_push(td.conf.stream_queue, dbt)
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
}

func are_we_waiting_for_schema_jobs_to_complete(td *thread_data) bool {
	if g_async_queue_length(td.conf.database_queue) > 0 ||
		g_async_queue_length(td.conf.table_queue) > 0 ||
		g_async_queue_length(td.conf.retry_queue) > 0 {
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
	if g_async_queue_length(td.conf.database_queue) > 0 {
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

func (o *OptionEntries) give_me_next_data_job_conf(conf *configuration, rj **restore_job) bool {
	var giveup = true
	conf.table_list_mutex.Lock()
	var dbt *db_table
	var job *restore_job
	for _, dbt = range conf.table_list {
		if dbt.database.schema_state == NOT_FOUND {
			log.Tracef("%s.%s: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			continue
		}
		if dbt.schema_state >= DATA_DONE || (dbt.schema_state == CREATED && (dbt.is_view || dbt.is_sequence)) {
			log.Tracef("%s.%s done: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			dbt.mutex.Unlock()
			continue
		}
		dbt.mutex.Lock()
		if !o.Resume && dbt.schema_state < CREATED {
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
			if o.global.all_jobs_are_enqueued && dbt.current_threads == 0 && atomic.LoadInt64(&dbt.remaining_jobs) == 0 {
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

func (o *OptionEntries) refresh_db_and_jobs(current_ft file_type) {
	switch current_ft {
	case SCHEMA_CREATE, SCHEMA_TABLE, SCHEMA_SEQUENCE:
		o.schema_queue_push(current_ft)
		break
	case DATA:
		log.Tracef("refresh_db_queue <- %v", current_ft)
		g_async_queue_push(o.global.refresh_db_queue, current_ft)
		break
	case INTERMEDIATE_ENDED:
		o.schema_queue_push(current_ft)
		log.Tracef("refresh_db_queue <- %v", current_ft)
		g_async_queue_push(o.global.refresh_db_queue, current_ft)
		break
	case SHUTDOWN:
		log.Tracef("refresh_db_queue <- %v", current_ft)
		g_async_queue_push(o.global.refresh_db_queue, current_ft)
	default:
		break
	}
}

func (o *OptionEntries) maybe_shutdown_control_job() {
	if g_atomic_int_dec_and_test(&o.global.last_wait) {
		log.Tracef("SHUTDOWN maybe_shutdown_control_job")
		o.refresh_db_and_jobs(SHUTDOWN)
	}
}

func (o *OptionEntries) wake_threads_waiting(threads_waiting *uint) {
	for *threads_waiting > 0 {
		log.Tracef("refresh_db_queue <- %v", THREAD)
		o.global.refresh_db_queue.push(THREAD)
		*threads_waiting = *threads_waiting - 1
	}
}

func (o *OptionEntries) control_job_thread(conf *configuration) {
	o.global.control_job_t.Add(1)
	defer o.global.control_job_t.Done()
	var ft file_type
	var rj *restore_job
	var _num_threads uint = o.NumThreads
	var threads_waiting uint
	var giveup bool
	var cont = true
	if o.OverwriteTables && !o.OverwriteUnsafe && o.global.cjt_paused {
		log.Tracef("Thread control_job_thread paused")
		o.global.cjt_mutex.Lock()
		for o.global.cjt_paused {
			o.global.cjt_cond.Wait()
		}
		o.global.cjt_mutex.Unlock()
	}
	log.Tracef("Thread control_job_thread started")
	for cont {
		task := o.global.refresh_db_queue.timeout_pop(10000000)
		if task == nil {
			ft = THREAD
		} else {
			ft = task.(file_type)
		}
		log.Tracef("refresh_db_queue -> %v (%d loaders waiting)", ft, threads_waiting)
		switch ft {
		case DATA:
			o.wake_threads_waiting(&threads_waiting)
			break
		case THREAD:
			giveup = o.give_me_next_data_job_conf(conf, &rj)
			if rj != nil {
				log.Tracef("job available in give_me_next_data_job_conf")
				if rj.dbt != nil {
					log.Tracef("data_queue <- %v: %s", rj.job_type, rj.dbt.table)
				} else {
					log.Tracef("data_queue <- %v: %s", rj.job_type, rj.filename)
				}
				g_async_queue_push(o.global.data_queue, rj)
				log.Tracef("here_is_your_job <- %v", DATA)
				g_async_queue_push(o.global.here_is_your_job, DATA)
			} else {
				log.Tracef("No job available")
				if o.global.all_jobs_are_enqueued && giveup {
					log.Tracef("Giving up...")
					o.global.control_job_ended = true
					var i uint
					for i = 0; i < _num_threads; i++ {
						log.Tracef("here_is_your_job <- %v", SHUTDOWN)
						g_async_queue_push(o.global.here_is_your_job, SHUTDOWN)
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
			o.enqueue_indexes_if_possible(conf)
			o.global.all_jobs_are_enqueued = true
			o.wake_threads_waiting(&threads_waiting)
			break
		case SHUTDOWN:
			cont = false
			break
		default:
			log.Tracef("Thread control_job_thread: received Default: %v", ft)
			break
		}
	}
	o.start_innodb_optimize_keys_all_tables()
	log.Tracef("Thread control_job_thread finished")
	return
}
