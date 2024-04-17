package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
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
	use_database string
}

func initialize_control_job(o *OptionEntries, conf *configuration) {
	o.global.refresh_db_queue = g_async_queue_new(o.Common.BufferSize)
	o.global.here_is_your_job = g_async_queue_new(o.Common.BufferSize)
	o.global.data_queue = g_async_queue_new(o.Common.BufferSize)
	o.global.last_wait = int64(o.Common.NumThreads)
	o.global.control_job_t = new(sync.WaitGroup)
	go control_job_thread(o, conf)

}

func new_job(job_type control_job_type, job_data any, use_database string) *control_job {
	var j = new(control_job)
	j.job_type = job_type
	j.use_database = use_database
	switch job_type {
	case JOB_WAIT:
		j.data.queue = job_data.(*asyncQueue)
	case JOB_SHUTDOWN:
	default:
		j.data.restore_job = job_data.(*restore_job)
	}
	return j
}

func process_job(o *OptionEntries, td *thread_data, job *control_job) bool {
	switch job.job_type {
	case JOB_RESTORE:
		process_restore_job(o, td, job.data.restore_job)
	case JOB_WAIT:
		td.conf.ready.push(1)
		job.data.queue.pop()
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
		for i = 0; i < len(dbt.restore_job_list); i++ {
			td.conf.stream_queue.push(1)
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
}

func are_we_waiting_for_schema_jobs_to_complete(td *thread_data) bool {
	if td.conf.database_queue.length > 0 || td.conf.table_queue.length > 0 {
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
	if td.conf.database_queue.length > 0 {
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
		if dbt.schema_state != CREATED || len(dbt.restore_job_list) > 0 {
			dbt.mutex.Unlock()
			td.conf.table_list_mutex.Unlock()
			return true
		}
		dbt.mutex.Unlock()
	}
	td.conf.table_list_mutex.Unlock()
	return false
}

func create_index_job(conf *configuration, dbt *db_table, tdid uint) bool {
	log.Infof("Thread %d: Enqueuing index for table: `%s`.`%s`", tdid, dbt.database.real_database, dbt.table)
	var rj = new_schema_restore_job("index", JOB_RESTORE_STRING, dbt, dbt.database, dbt.indexes, INDEXES)
	conf.index_queue.push(new_job(JOB_RESTORE, rj, dbt.database.real_database))
	dbt.schema_state = INDEX_ENQUEUED
	return true
}

func give_me_next_data_job_conf(o *OptionEntries, conf *configuration, test_condition bool, rj **restore_job) bool {
	var giveup = true
	conf.table_list_mutex.Lock()
	var dbt *db_table
	var job *restore_job
	var second bool
	var i int
	for i < 2 && job == nil {
		i++
		for _, dbt = range conf.table_list {
			if dbt.schema_state >= DATA_DONE {
				continue
			}
			dbt.mutex.Lock()
			var td uint
			if second {
				td = dbt.max_threads_hard
			} else {
				td = dbt.max_threads
			}
			if !test_condition || (dbt.schema_state == CREATED && dbt.current_threads < td) {
				if !o.Common.Resume && dbt.schema_state < CREATED {
					giveup = false
					dbt.mutex.Unlock()
					continue
				}
				if dbt.schema_state >= DATA_DONE {
					dbt.mutex.Unlock()
					continue
				}
				if len(dbt.restore_job_list) > 0 {
					job = dbt.restore_job_list[0]
					// dbt->restore_job_list=g_list_remove_link(dbt->restore_job_list,dbt->restore_job_list);
					dbt.current_threads++
					dbt.mutex.Unlock()
					giveup = false
					break
				} else {
					if o.global.intermediate_queue_ended_local && dbt.current_threads == 0 {
						dbt.schema_state = DATA_DONE
						enqueue_index_for_dbt_if_possible(conf, dbt)
						giveup = false
					}
				}
			}
			dbt.mutex.Unlock()
		}
		second = true
	}
	conf.table_list_mutex.Unlock()
	*rj = job
	return giveup
}

func give_me_next_data_job(o *OptionEntries, td *thread_data, test_condition bool, rj **restore_job) bool {
	return give_me_next_data_job_conf(o, td.conf, test_condition, rj)
}

func give_any_data_job_conf(o *OptionEntries, conf *configuration, rj **restore_job) bool {
	return give_me_next_data_job_conf(o, conf, false, rj)
}

func give_any_data_job(o *OptionEntries, td *thread_data, rj **restore_job) bool {
	return give_me_next_data_job_conf(o, td.conf, false, rj)
}

func enqueue_index_for_dbt_if_possible(conf *configuration, dbt *db_table) {
	if dbt.schema_state == DATA_DONE {
		if dbt.indexes == "" {
			dbt.schema_state = ALL_DONE
		} else {
			create_index_job(conf, dbt, 0)
		}
	}
}

func enqueue_indexes_if_possible(conf *configuration) {
	conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range conf.table_list {
		dbt.mutex.Lock()
		enqueue_index_for_dbt_if_possible(conf, dbt)
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

func refresh_db_and_jobs(o *OptionEntries, current_ft file_type) {
	switch current_ft {
	case SCHEMA_CREATE, SCHEMA_TABLE:
		schema_queue_push(o, current_ft)
	case DATA:
		o.global.refresh_db_queue.push(current_ft)
	case INTERMEDIATE_ENDED:
		schema_queue_push(o, current_ft)
		o.global.refresh_db_queue.push(current_ft)
	case SHUTDOWN:
		o.global.refresh_db_queue.push(current_ft)
	default:
		return
	}
}

func last_wait_control_job_to_shutdown(o *OptionEntries) {
	if g_atomic_int_dec_and_test(&o.global.last_wait) {
		log.Infof("SHUTDOWN last_wait_control_job_to_shutdown")
		refresh_db_and_jobs(o, SHUTDOWN)
	}
}

func wake_threads_waiting(o *OptionEntries, conf *configuration, threads_waiting *uint) {
	var rj *restore_job
	if *threads_waiting > 0 {
		var giveup bool
		for 0 < *threads_waiting && !giveup {
			giveup = give_me_next_data_job_conf(o, conf, true, &rj)
		}
		if rj != nil {
			*threads_waiting = *threads_waiting - 1
			o.global.data_queue.push(rj)
			o.global.here_is_your_job.push(DATA)
		}
		if o.global.intermediate_queue_ended_local && giveup {
			o.global.refresh_db_queue.push(THREAD)
		}
	}

}

func control_job_thread(o *OptionEntries, conf *configuration) {
	o.global.control_job_t.Add(1)
	defer o.global.control_job_t.Done()
	var ft file_type
	var rj *restore_job
	var threads_waiting uint
	var giveup bool
	var lkey string
	var real_db_name *database
	var cont = true
	for cont {
		ft = o.global.refresh_db_queue.pop().(file_type)
		switch ft {
		case SCHEMA_CREATE:
			log.Errorf("Thread control job: This should not happen")
		case SCHEMA_TABLE:
			log.Errorf("Thread control job: This should not happen")
		case DATA:
			wake_threads_waiting(o, conf, &threads_waiting)
		case THREAD:
			giveup = give_me_next_data_job_conf(o, conf, true, &rj)
			if rj != nil {
				o.global.data_queue.push(rj)
				o.global.here_is_your_job.push(DATA)
			} else {
				giveup = give_any_data_job_conf(o, conf, &rj)
				if rj != nil {
					o.global.data_queue.push(rj)
					o.global.here_is_your_job.push(DATA)
				} else {
					if o.global.intermediate_queue_ended_local {
						if giveup {
							o.global.here_is_your_job.push(SHUTDOWN)
							for ; 0 < threads_waiting; threads_waiting-- {
								o.global.here_is_your_job.push(SHUTDOWN)
							}
						} else {
							if threads_waiting < o.Common.NumThreads {
								threads_waiting = threads_waiting + 1
							} else {
								threads_waiting = o.Common.NumThreads
							}
						}
					} else {
						if threads_waiting < o.Common.NumThreads {
							threads_waiting = threads_waiting + 1
						} else {
							threads_waiting = o.Common.NumThreads
						}
					}
				}
			}
		case INTERMEDIATE_ENDED:
			for lkey, real_db_name = range o.global.db_hash {
				real_db_name.mutex.Lock()
				real_db_name.mutex.Unlock()
				_ = lkey
			}
			enqueue_indexes_if_possible(conf)
			wake_threads_waiting(o, conf, &threads_waiting)
			o.global.intermediate_queue_ended_local = true
		case SHUTDOWN:
			cont = false
		default:
			return

		}
	}
	log.Infof("Sending start_innodb_optimize_keys_all_tables")
	start_innodb_optimize_keys_all_tables(o)
	return
}

func process_stream_queue(o *OptionEntries, td *thread_data) {
	var job *control_job
	var cont = true
	var ft file_type = -1
	var rj *restore_job
	var dbt *db_table
	for cont {
		o.global.refresh_db_queue.push(THREAD)
		ft = o.global.here_is_your_job.pop().(file_type)
		switch ft {
		case SCHEMA_CREATE:
			log.Errorf("Thread %d: This should not happen", td.thread_id)
		case SCHEMA_TABLE:
			log.Errorf("Thread %d: This should not happen", td.thread_id)
		case DATA:
			rj = o.global.data_queue.pop().(*restore_job)
			dbt = rj.dbt
			job = new_job(JOB_RESTORE, rj, dbt.database.real_database)
			execute_use_if_needs_to(o, td, job.use_database, "Restoring tables (2)")
			cont = process_job(o, td, job)
			dbt.mutex.Lock()
			dbt.current_threads--
			dbt.mutex.Unlock()
		case SHUTDOWN:
			cont = false
		case IGNORED:
			time.Sleep(1000 * time.Microsecond)
		default:
			return
		}
	}
}
