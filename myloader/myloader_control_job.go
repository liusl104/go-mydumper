package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
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
	cjt_cond              *sync.Cond
	control_job_queue     *GAsyncQueue
	data_job_queue        *GAsyncQueue
	data_queue            *GAsyncQueue
	control_job_t         *GThread
	cjt_paused            bool = true
	all_jobs_are_enqueued bool
	last_wait             int64
)

// jtype2str returns the string name of the control_job_type (JOB_RESTORE, JOB_SHUTDOWN).
func jtype2str(jtype control_job_type) string {
	switch jtype {
	case JOB_RESTORE:
		return "JOB_RESTORE"
	case JOB_SHUTDOWN:
		return "JOB_SHUTDOWN"
	}
	return ""
}

// cjt_resume sets cjt_paused to false and signals the control job thread to continue.
func cjt_resume() {
	cjt_mutex.Lock()
	cjt_paused = false
	cjt_cond.Signal()
	cjt_mutex.Unlock()
}

// initialize_control_job creates control_job_queue, data_job_queue, data_queue, cjt mutex/cond, and starts control_job_thread.
func initialize_control_job(conf *configuration) {
	control_job_queue = G_async_queue_new("control_job_queue")
	data_job_queue = G_async_queue_new("data_job_queue")
	last_wait = int64(NumThreads)
	data_queue = G_async_queue_new("data_queue")
	cjt_mutex = G_mutex_new()
	cjt_cond = sync.NewCond(cjt_mutex)
	control_job_t = M_thread_new("myloader_ctr", control_job_thread, conf, "Control job thread could not be created")

}

// wait_control_job joins the control job thread and clears cjt_mutex/cond.
func wait_control_job() {
	log.Debugf("Waiting control job to finish")
	G_thread_join(control_job_t)
	cjt_mutex = nil
	cjt_cond = nil
	log.Debugf("Control job to finished")
}

// new_control_job allocates a control_job with the given type, job_data (restore_job for JOB_RESTORE), and use_database.
func new_control_job(job_type control_job_type, job_data any, use_database *database) *control_job {
	var j = new(control_job)
	j.job_type = job_type
	j.use_database = use_database
	j.data = new(control_job_data)
	switch job_type {
	case JOB_SHUTDOWN:
		break
	default:
		j.data.restore_job = job_data.(*restore_job)
	}
	return j
}

// control_job_queue_push pushes the file_type onto control_job_queue (for control_job_thread).
func control_job_queue_push(current_ft file_type) {
	log.Debugf("control_job_queue <- %s", ft2str(current_ft))
	G_async_queue_push(control_job_queue, current_ft)
}

// request_restore_data_job pushes REQUEST_DATA_JOB and blocks until a file_type is returned on data_job_queue (DATA or SHUTDOWN).
func request_restore_data_job() file_type {
	control_job_queue_push(REQUEST_DATA_JOB)
	var ft file_type = G_async_queue_pop(data_job_queue).(file_type)
	log.Debugf("data_job_queue -> %s", ft2str(ft))
	return ft
}

// rjtype2str returns the string name of the restore_job_type.
func rjtype2str(rjtype restore_job_type) string {
	switch rjtype {
	case JOB_RESTORE_SCHEMA_FILENAME:
		return "JOB_RESTORE_SCHEMA_FILENAME"
	case JOB_RESTORE_FILENAME:
		return "JOB_RESTORE_FILENAME"
	case JOB_TO_CREATE_TABLE:
		return "JOB_TO_CREATE_TABLE"
	case JOB_RESTORE_STRING:
		return "JOB_RESTORE_STRING"
	}

	return "0"
}

// request_next_data_job pops and returns the next restore_job from data_queue.
func request_next_data_job() *restore_job {
	var rj *restore_job = G_async_queue_pop(data_queue).(*restore_job)
	log.Debugf("data_queue -> %s: %s.%s, threads %d", rjtype2str(rj.job_type), rj.dbt.database.real_database, rj.dbt.real_table, rj.dbt.current_threads)
	return rj
}

// give_me_next_data_job_conf finds the next available data restore job from conf.table_list and assigns it to *rj; returns true (giveup) when no more work.
func give_me_next_data_job_conf(conf *configuration, rj **restore_job) bool {
	var giveup = true
	conf.table_list_mutex.Lock()
	var dbt *db_table
	var job *restore_job
	for _, dbt = range conf.table_list {
		if dbt.database.schema_state == NOT_FOUND {
			log.Debugf("%s.%s: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			continue
		}
		if dbt.schema_state >= DATA_DONE || (dbt.schema_state == CREATED && (dbt.is_view || dbt.is_sequence)) {
			log.Debugf("%s.%s done: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			continue
		}
		dbt.mutex.Lock()
		if !Resume && dbt.schema_state < CREATED {
			giveup = false
			log.Debugf("%s.%s not yet created: %s, waiting", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
			dbt.mutex.Unlock()
			continue
		}
		if dbt.schema_state >= DATA_DONE || (dbt.schema_state == CREATED && (dbt.is_view || dbt.is_sequence)) {
			log.Debugf("%s.%s done just now: %s, voting for finish", dbt.database.real_database, dbt.real_table, status2str(dbt.schema_state))
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
			if dbt.restore_job_list.Len() > 0 {
				dbt.restore_job_list.Front().Next()
			}
			dbt.current_threads++
			dbt.mutex.Unlock()
			giveup = false
			log.Debugf("%s.%s sending %v: %s, threads: %d, prohibiting finish", dbt.database.real_database, dbt.real_table,
				rjtype2str(job.job_type), job.filename, dbt.current_threads)
			break
		} else {
			log.Debugf("No remaining jobs on %s.%s", dbt.database.real_database, dbt.real_table)
			if all_jobs_are_enqueued && dbt.current_threads == 0 && atomic.LoadInt64(&dbt.remaining_jobs) == 0 {
				dbt.schema_state = DATA_DONE
				enqueue_index_for_dbt_if_possible(conf, dbt)
				log.Debugf("%s.%s queuing indexes, voting for finish", dbt.database.real_database, dbt.real_table)
			}
		}
		dbt.mutex.Unlock()
	}

	conf.table_list_mutex.Unlock()
	*rj = job
	return giveup
}

// enroute_into_the_right_queue_based_on_file_type pushes the file_type to the appropriate queue (schema_queue_push or control_job_queue_push).
func enroute_into_the_right_queue_based_on_file_type(current_ft file_type) {
	switch current_ft {
	case SCHEMA_CREATE, SCHEMA_TABLE, SCHEMA_SEQUENCE:
		schema_queue_push(current_ft, "")
		break
	case INTERMEDIATE_ENDED:
		schema_queue_push(current_ft, "")
		control_job_queue_push(current_ft)
		break
	case REQUEST_DATA_JOB, DATA, SHUTDOWN:
		control_job_queue_push(current_ft)
	default:
		break
	}
}

// maybe_shutdown_control_job decrements last_wait; when it reaches zero, pushes SHUTDOWN to unblock loader threads.
func maybe_shutdown_control_job() {
	if G_atomic_int_dec_and_test(&last_wait) {
		log.Debugf("SHUTDOWN maybe_shutdown_control_job")
		enroute_into_the_right_queue_based_on_file_type(SHUTDOWN)
	}
}

// wake_threads_waiting pushes REQUEST_DATA_JOB for each waiting thread and zeros threads_waiting.
func wake_threads_waiting(threads_waiting *uint) {
	for *threads_waiting > 0 {
		control_job_queue_push(REQUEST_DATA_JOB)
		*threads_waiting = *threads_waiting - 1
	}
}

// control_job_thread is the main loop: pops file_types from control_job_queue, handles REQUEST_DATA_JOB (give_me_next_data_job_conf), DATA, INTERMEDIATE_ENDED, SHUTDOWN; then starts optimize keys.
func control_job_thread(c any) {
	cnf := c.(*configuration)
	var ft file_type
	var rj *restore_job
	var _num_threads uint = NumThreads
	var threads_waiting uint = 0
	var giveup bool
	var cont = true
	if OverwriteTables && !OverwriteUnsafe && cjt_paused {
		log.Debugf("Thread control_job_thread paused")
		cjt_mutex.Lock()
		for cjt_paused {
			cjt_cond.Wait()
		}
		cjt_mutex.Unlock()
	}
	log.Debugf("Thread control_job_thread started")
	for cont {
		task := G_async_queue_pop(control_job_queue)
		ft = task.(file_type)
		log.Debugf("control_job_queue -> %s (%d loaders waiting)", ft2str(ft), threads_waiting)
		switch ft {
		case DATA:
			wake_threads_waiting(&threads_waiting)
			break
		case REQUEST_DATA_JOB:
			giveup = give_me_next_data_job_conf(cnf, &rj)
			if rj != nil {
				log.Debugf("job available in give_me_next_data_job_conf")
				if rj.dbt != nil {
					log.Debugf("data_queue <- %s: %s", rjtype2str(rj.job_type), rj.dbt.table)
				} else {
					log.Debugf("data_queue <- %s: %s", rjtype2str(rj.job_type), rj.filename)
				}
				G_async_queue_push(data_queue, rj)
				log.Debugf("data_job_queue <- %s", ft2str(DATA))
				G_async_queue_push(data_job_queue, DATA)
			} else {
				log.Debugf("No job available")
				if all_jobs_are_enqueued && giveup {
					log.Debugf("Giving up...")
					control_job_ended = true
					var i uint
					for i = 0; i < _num_threads; i++ {
						log.Debugf("data_job_queue <- %s", ft2str(SHUTDOWN))
						G_async_queue_push(data_job_queue, SHUTDOWN)
					}
				} else {
					log.Debugf("Thread will be waiting | all_jobs_are_enqueued: %v | giveup: %v", all_jobs_are_enqueued, giveup)
					// Consistent with C: increment only once, not loop up to _num_threads
					if threads_waiting < _num_threads {
						threads_waiting++
					}
				}
			}
			break
		case INTERMEDIATE_ENDED:
			enqueue_indexes_if_possible(cnf)
			all_jobs_are_enqueued = true
			wake_threads_waiting(&threads_waiting)
			break
		case SHUTDOWN:
			cont = false
			break
		default:
			log.Debugf("Thread control_job_thread: received Default: %v", ft)
			break
		}
	}
	start_optimize_keys_all_tables()
	log.Debugf("Thread control_job_thread finished")
	return
}

// process_job dispatches the control job: JOB_RESTORE runs process_restore_job (sets retry if needed), JOB_SHUTDOWN returns false to stop.
func process_job(td *thread_data, job *control_job, retry *bool) bool {
	switch job.job_type {
	case JOB_RESTORE:
		log.Debugf("Thread %d: Restoring Job", td.thread_id)
		var res bool = process_restore_job(td, job.data.restore_job)
		if retry != nil {
			*retry = res
		}
		return true
	case JOB_SHUTDOWN:
		log.Debugf("Thread %d: Shutting down", td.thread_id)
		return false
	default:
		log.Criticalf("Something very bad happened!(1)")
	}
	return true
}

/*
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

// are_we_waiting_for_schema_jobs_to_complete returns true if database/table/retry queues have jobs or any table is in CREATING state.
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

// are_we_waiting_for_create_schema_jobs_to_complete returns true if database_queue has jobs or any table is in CREATING state.
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

// are_available_jobs returns true if any table is not CREATED or has pending restore jobs in its list.
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

// refresh_db_and_jobs pushes the file_type to the appropriate queue (schema_queue or refresh_db_queue) for the given type.
func refresh_db_and_jobs(current_ft file_type) {
	switch current_ft {
	case SCHEMA_CREATE, SCHEMA_TABLE, SCHEMA_SEQUENCE:
		schema_queue_push(current_ft, "")
		break
	case DATA:
		log.Debugf("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
		break
	case INTERMEDIATE_ENDED:
		schema_queue_push(current_ft, "")
		log.Debugf("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
		break
	case SHUTDOWN:
		log.Debugf("refresh_db_queue <- %v", current_ft)
		G_async_queue_push(refresh_db_queue, current_ft)
	default:
		break
	}
}
*/
