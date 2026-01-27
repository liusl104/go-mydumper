package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	refresh_db_queue2 *GAsyncQueue
	schema_td         []*thread_data
	second_round      bool = false

	schema_threads []*GThread
)

func schema_queue_push(current_ft file_type, message string) {
	log.Debugf("refresh_db_queue2 <- %s%s", ft2str(current_ft), message)
	G_async_queue_push(refresh_db_queue2, current_ft)
}

func set_db_schema_created(real_db_name *database, conf *configuration) {
	var cj *control_job
	var ft file_type
	var queue *GAsyncQueue
	var object_queue *GAsyncQueue = conf.table_queue
	real_db_name.schema_state = CREATED
	if sequences_processed < sequences {
		ft = SCHEMA_SEQUENCE
		queue = real_db_name.sequence_queue
	} else {
		ft = SCHEMA_TABLE
		queue = real_db_name.queue
	}
	task := G_async_queue_try_pop(queue)
	for task != nil {
		cj = task.(*control_job)
		G_async_queue_push(object_queue, cj)
		schema_queue_push(ft, " (requeuing from db queue)")
		task = G_async_queue_try_pop(queue)
	}

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

func process_schema(td *thread_data) bool {
	var ft file_type
	var real_db_name *database
	var job *control_job
	var ret = true
	var postpone_load bool = OverwriteTables && !OverwriteUnsafe
	ft = G_async_queue_pop(refresh_db_queue2).(file_type)
	log.Debugf("refresh_db_queue2 -> %s", ft2str(ft))
	switch ft {
	case SCHEMA_CREATE:
		job = G_async_queue_pop(td.conf.database_queue).(*control_job)
		real_db_name = job.data.restore_job.data.srj.database
		log.Debugf("database_queue -> %s: %s", ft2str(ft), real_db_name.name)
		real_db_name.mutex.Lock()
		ret = process_job(td, job, nil)
		set_db_schema_created(real_db_name, td.conf)
		log.Debugf("Set DB created: %s", real_db_name.name)
		real_db_name.mutex.Unlock()
		break
	case CJT_RESUME:
		cjt_resume()
		fallthrough // 与 C 版本 "// fall through" 对应，需继续执行 SCHEMA_TABLE 分支以从 table_queue 取 JOB_SHUTDOWN 并退出
	case SCHEMA_TABLE, SCHEMA_SEQUENCE:
		var qname string
		job = G_async_queue_pop(td.conf.table_queue).(*control_job)
		qname = "table_queue"
		if job.job_type == JOB_SHUTDOWN {
			var task = G_async_queue_try_pop(td.conf.retry_queue)
			if task != nil {
				rjob := task.(*control_job)
				G_async_queue_push(td.conf.table_queue, job)
				job = rjob
				qname = "retry_queue"
			}
		}
		var restore bool = job.job_type == JOB_RESTORE
		var retry = false
		var filename string
		if restore {
			filename = job.data.restore_job.filename
			log.Debugf("%s -> %s: %s", qname, ft2str(ft), filename)
		} else {
			log.Debugf("%s -> %s", qname, jtype2str(job.job_type))
		}
		ret = process_job(td, job, &retry)
		if retry {
			G_assert(restore)
			log.Debugf("retry_queue <- %s: %s", ft2str(ft), filename)
			G_async_queue_push(td.conf.retry_queue, job)
			enroute_into_the_right_queue_based_on_file_type(ft)
			break
		}
		if ft == SCHEMA_TABLE { /* TODO: for spoof view table don't do DATA */
			enroute_into_the_right_queue_based_on_file_type(DATA)
		} else if restore {
			G_assert(ft == SCHEMA_SEQUENCE && sequences_processed < sequences)
			sequences_mutex.Lock()
			sequences_processed++
			log.Debugf("Processed sequence: %s (%d of %d)", filename, sequences_processed, sequences)
			sequences_mutex.Unlock()
		}
		break
	case INTERMEDIATE_ENDED:
		if !second_round {
			sequences_mutex.Lock()
			if sequences_processed < sequences {
				log.Debugf("INTERMEDIATE_ENDED waits %d sequences", sequences-sequences_processed)
				enroute_into_the_right_queue_based_on_file_type(INTERMEDIATE_ENDED)
				sequences_mutex.Unlock()
				return true
			}
			sequences_mutex.Unlock()
			/* Wait while all DB created and go "second round" */
			for _, real_db_name = range db_hash {
				real_db_name.mutex.Lock()
				if real_db_name.schema_state != CREATED {
					log.Debugf("INTERMEDIATE_ENDED waits %s created, current state: %s", real_db_name.name, status2str(real_db_name.schema_state))
					if real_db_name.schema_state == NOT_FOUND {
						real_db_name.schema_state = NOT_FOUND_2
					} else if real_db_name.schema_state == NOT_FOUND_2 {
						log.Warnf("Schema file for `%s` not found, continue anyways", real_db_name.name)
						real_db_name.schema_state = CREATED
					}
					enroute_into_the_right_queue_based_on_file_type(INTERMEDIATE_ENDED)
					real_db_name.mutex.Unlock()
					return true
				}
				real_db_name.mutex.Unlock()
				set_db_schema_created(real_db_name, td.conf)
			}
			log.Infof("Schema creation enqueing completed")
			second_round = true
			schema_queue_push(ft, " (first round)")
		} else {
			set_table_schema_state_to_created(td.conf)
			log.Infof("Table creation enqueing completed")
			var n uint
			/* we also sending to ourselves and upper loop of worker_schema_thread() will send us to SCHEMA_TABLE/JOB_SHUTDOWN */
			// td = schema_td[n]
			for n = 0; n < MaxThreadsForSchemaCreation; n++ {
				td = schema_td[n]
				log.Debugf("table_queue <- JOB_SHUTDOWN")
				G_async_queue_push(td.conf.table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
				if !postpone_load || n < MaxThreadsForSchemaCreation-1 {
					schema_queue_push(SCHEMA_TABLE, " (second round)")
				}
			}
			if postpone_load {
				schema_queue_push(CJT_RESUME, "")
			}
		}
		break
	default:
		log.Infof("Default in schema: %d", ft)
		break
	}

	return ret
}

func worker_schema_thread(c any) {
	td := c.(*thread_data)
	var cnf *configuration = td.conf
	G_async_queue_push(cnf.ready, 1)

	log.Infof("S-Thread %d: Starting import", td.thread_id)
	var cont bool = true
	for cont {
		cont = process_schema(td)
	}
	log.Infof("S-Thread %d: Import completed", td.thread_id)
}

func initialize_worker_schema(conf *configuration) {
	var n uint
	refresh_db_queue2 = G_async_queue_new()
	schema_threads = make([]*GThread, MaxThreadsForSchemaCreation)
	schema_td = make([]*thread_data, MaxThreadsForSchemaCreation)
	log.Infof("Initializing initialize_worker_schema")
	for n = 0; n < MaxThreadsForSchemaCreation; n++ {
		schema_td[n] = new(thread_data)
		initialize_thread_data(schema_td[n], conf, WAITING, n+1+NumThreads, nil)
	}

}

func start_worker_schema() {
	var n uint
	for n = 0; n < MaxThreadsForSchemaCreation; n++ {
		schema_threads[n] = M_thread_new("myloader_schema", worker_schema_thread, schema_td[n], "Schema thread could not be created")
	}
}
func wait_schema_worker_to_finish() {
	var n uint
	log.Debugf("Waiting schema worker to finish")
	for n = 0; n < MaxThreadsForSchemaCreation; n++ {
		log.Debugf("wait schema thread id : %d", schema_threads[n].Thread_id)
		G_thread_join(schema_threads[n])
		log.Debugf("schema thread id : %d do", schema_threads[n].Thread_id)
	}
	log.Debugf("Schema worker finished")
}

func free_schema_worker_threads() {
	schema_td = nil
	schema_threads = nil
}
