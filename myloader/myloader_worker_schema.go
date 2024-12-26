package myloader

import (
	log "github.com/sirupsen/logrus"
)

func (o *OptionEntries) schema_queue_push(current_ft file_type) {
	log.Tracef("refresh_db_queue2 <- %v", current_ft)
	g_async_queue_push(o.global.refresh_db_queue2, current_ft)
}

func (o *OptionEntries) set_db_schema_created(real_db_name *database, conf *configuration) {
	var cj *control_job
	var ft file_type
	var queue *asyncQueue
	var object_queue *asyncQueue = conf.table_queue
	real_db_name.schema_state = CREATED
	if o.global.sequences_processed < o.global.sequences {
		ft = SCHEMA_SEQUENCE
		queue = real_db_name.sequence_queue
	} else {
		ft = SCHEMA_TABLE
		queue = real_db_name.queue
	}
	cj = g_async_queue_try_pop(queue).(*control_job)
	for cj != nil {
		g_async_queue_push(object_queue, cj)
		log.Tracef("refresh_db_queue2 <- %v (requeuing from db queue)", ft)
		g_async_queue_push(o.global.refresh_db_queue2, ft)
		cj = g_async_queue_try_pop(queue).(*control_job)
	}

}
func (o *OptionEntries) set_db_schema_state_to_created(conf *configuration) {
	conf.table_list_mutex.Lock()
	for _, dbt := range conf.table_list {
		dbt.mutex.Lock()
		if dbt.schema_state == NOT_FOUND {
			dbt.schema_state = CREATED
		}
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

func set_table_schema_state_to_created(conf *configuration) {
	conf.table_list_mutex.Lock()
	var dbt *db_table
	for _, dbt = range conf.table_list {
		dbt.mutex.Lock()
		if dbt.schema_state == NOT_FOUND {
			dbt.schema_state = CREATED
		}
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

func (o *OptionEntries) process_schema(td *thread_data) bool {
	var ft file_type
	var real_db_name *database
	var job *control_job
	var ret = true
	var postpone_load bool = o.OverwriteTables && !o.OverwriteUnsafe
	ft = o.global.refresh_db_queue2.pop().(file_type)
	log.Tracef("refresh_db_queue2 -> %v", ft)
	switch ft {
	case SCHEMA_CREATE:
		job = g_async_queue_pop(td.conf.database_queue).(*control_job)
		real_db_name = job.data.restore_job.data.srj.database
		log.Tracef("database_queue -> %v: %s", ft, real_db_name.name)
		real_db_name.mutex.Lock()
		ret = o.process_job(td, job, nil)
		o.set_db_schema_created(real_db_name, td.conf)
		log.Debugf("Set DB created: %s", real_db_name.name)
		real_db_name.mutex.Unlock()
		break
	case CJT_RESUME:
		o.cjt_resume()
	case SCHEMA_TABLE, SCHEMA_SEQUENCE:
		var qname string
		job = g_async_queue_pop(td.conf.table_queue).(*control_job)
		qname = "table_queue"
		if job.job_type == JOB_SHUTDOWN {
			var rjob *control_job = g_async_queue_try_pop(td.conf.retry_queue).(*control_job)
			if rjob != nil {
				g_async_queue_push(td.conf.table_queue, job)
				job = rjob
				qname = "retry_queue"
			}
		}
		var restore bool = job.job_type == JOB_RESTORE
		var retry = false
		var filename string
		if restore {
			filename = job.data.restore_job.filename
			log.Tracef("%s -> %v: %s", qname, ft, filename)
		} else {
			log.Tracef("%s -> %v", qname, job.job_type)
		}
		ret = o.process_job(td, job, &retry)
		if retry {
			g_assert(restore)
			log.Tracef("retry_queue <- %v: %s", ft, filename)
			g_async_queue_push(td.conf.retry_queue, job)
			o.refresh_db_and_jobs(ft)
			break
		}
		if ft == SCHEMA_TABLE { /* TODO: for spoof view table don't do DATA */
			o.refresh_db_and_jobs(DATA)
		} else if restore {
			g_assert(ft == SCHEMA_SEQUENCE && o.global.sequences_processed < o.global.sequences)
			o.global.sequences_mutex.Lock()
			o.global.sequences_processed++
			log.Tracef("Processed sequence: %s (%d of %d)", filename, o.global.sequences_processed, o.global.sequences)
			o.global.sequences_mutex.Unlock()
		}
		break
	case INTERMEDIATE_ENDED:
		if !o.global.second_round {
			o.global.sequences_mutex.Lock()
			if o.global.sequences_processed < o.global.sequences {
				log.Tracef("INTERMEDIATE_ENDED waits %d sequences", o.global.sequences-o.global.sequences_processed)
				o.refresh_db_and_jobs(INTERMEDIATE_ENDED)
				o.global.sequences_mutex.Unlock()
				return true
			}
			o.global.sequences_mutex.Lock()
			/* Wait while all DB created and go "second round" */
			for _, real_db_name = range o.global.db_hash {
				real_db_name.mutex.Lock()
				if real_db_name.schema_state != CREATED {
					log.Debugf("INTERMEDIATE_ENDED waits %s created, current state: %s", real_db_name.name, status2str(real_db_name.schema_state))
					if real_db_name.schema_state == NOT_FOUND {
						real_db_name.schema_state = NOT_FOUND_2
					} else if real_db_name.schema_state == NOT_FOUND_2 {
						log.Warnf("Schema file for `%s` not found, continue anyways", real_db_name.name)
						real_db_name.schema_state = CREATED
					}
					o.refresh_db_and_jobs(INTERMEDIATE_ENDED)
					real_db_name.mutex.Unlock()
					return true
				}
				real_db_name.mutex.Unlock()
				o.set_db_schema_created(real_db_name, td.conf)
			}
			log.Infof("Schema creation enqueing completed")
			o.global.second_round = true
			log.Tracef("refresh_db_queue2 <- %v (first round)", ft)
			g_async_queue_push(o.global.refresh_db_queue2, ft)
		} else {
			set_table_schema_state_to_created(td.conf)
			log.Infof("Table creation enqueing completed")
			var n uint
			/* we also sending to ourselves and upper loop of worker_schema_thread() will send us to SCHEMA_TABLE/JOB_SHUTDOWN */
			for n = 0; n < o.MaxThreadsForSchemaCreation; n++ {
				td = o.global.schema_td[n]
				log.Tracef("table_queue <- JOB_SHUTDOWN")
				g_async_queue_push(td.conf.table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
				log.Tracef("refresh_db_queue2 <- %v (second round)", SCHEMA_TABLE)
				if !postpone_load || n < o.MaxThreadsForIndexCreation-1 {
					g_async_queue_push(o.global.refresh_db_queue2, SCHEMA_TABLE)
				}
			}
			if postpone_load {
				g_async_queue_push(o.global.refresh_db_queue2, CJT_RESUME)
			}
		}
		break
	default:
		log.Infof("Default in schema: %d", ft)
		break
	}

	return ret
}

func (o *OptionEntries) worker_schema_thread(td *thread_data, thread_id uint) {
	defer o.global.schema_threads[thread_id].thread.Done()
	var conf *configuration = td.conf
	g_async_queue_push(conf.ready, 1)

	log.Infof("S-Thread %d: Starting import", td.thread_id)
	var cont bool = true
	for cont {
		cont = o.process_schema(td)
	}
	log.Infof("S-Thread %d: Import completed", td.thread_id)
}

func (o *OptionEntries) initialize_worker_schema(conf *configuration) {
	var n uint
	o.global.init_connection_mutex = g_mutex_new()
	o.global.refresh_db_queue2 = g_async_queue_new(o.BufferSize)
	o.global.schema_threads = make([]*GThreadFunc, o.MaxThreadsForSchemaCreation)
	o.global.schema_td = make([]*thread_data, o.MaxThreadsForSchemaCreation)
	log.Infof("Initializing initialize_worker_schema")
	for n = 0; n < o.MaxThreadsForSchemaCreation; n++ {
		t := new(thread_data)
		o.global.schema_threads[n] = new(GThreadFunc)
		initialize_thread_data(o.global.schema_td[n], conf, WAITING, n+1+o.NumThreads, nil)
		o.global.schema_td[n] = t
		go o.worker_schema_thread(o.global.schema_td[n], n)
	}

}

func (o *OptionEntries) wait_schema_worker_to_finish() {
	var n uint
	for n = 0; n < o.MaxThreadsForSchemaCreation; n++ {
		o.global.schema_threads[n].thread.Wait()
	}

}

func free_schema_worker_threads(o *OptionEntries) {
	o.global.schema_td = nil
	o.global.schema_threads = nil
}
