package myloader

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

func schema_queue_push(o *OptionEntries, current_ft file_type) {
	o.global.refresh_db_queue2.push(current_ft)
}

func set_db_schema_state_to_created(o *OptionEntries, database *database, object_queue *asyncQueue) {
	database.schema_state = CREATED
	var cj *control_job
	task := database.queue.try_pop()
	for task != nil {
		cj = task.(*control_job)
		object_queue.push(cj)
		o.global.refresh_db_queue2.push(SCHEMA_TABLE)
		task = database.queue.try_pop()
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

func process_schema(o *OptionEntries, td *thread_data) bool {
	var ft file_type
	var lkey string
	var real_db_name *database
	var job *control_job
	var ret = true
	ft = o.global.refresh_db_queue2.pop().(file_type)
	switch ft {
	case SCHEMA_CREATE:
		job = td.conf.database_queue.pop().(*control_job)
		real_db_name = job.data.restore_job.data.srj.database
		real_db_name.mutex.Lock()
		ret = process_job(o, td, job)
		set_db_schema_state_to_created(o, real_db_name, td.conf.table_queue)
		real_db_name.mutex.Unlock()
	case SCHEMA_TABLE:
		job = td.conf.table_queue.pop().(*control_job)
		execute_use_if_needs_to(o, td, job.use_database, "Restoring table structure")
		ret = process_job(o, td, job)
		refresh_db_and_jobs(o, DATA)

	case INTERMEDIATE_ENDED:
		if !o.global.second_round {
			for lkey, real_db_name = range o.global.db_hash {
				_ = lkey
				real_db_name.mutex.Lock()
				set_db_schema_state_to_created(o, real_db_name, td.conf.table_queue)
				real_db_name.mutex.Unlock()
			}
			log.Info("Schema creation enqueing completed")
			o.global.second_round = true
			o.global.refresh_db_queue2.push(ft)
		} else {
			set_table_schema_state_to_created(td.conf)
			log.Infof("Table creation enqueing completed")
			var n uint
			for n = 0; n < o.Threads.MaxThreadsForSchemaCreation; n++ {
				td.conf.table_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
				o.global.refresh_db_queue2.push(SCHEMA_TABLE)
			}
		}

	default:
		log.Infof("Default in schema: %d", ft)

	}

	return ret
}

func worker_schema_thread(o *OptionEntries, td *thread_data) {
	defer o.global.schema_threads.Done()
	var conf = td.conf
	var err error
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
			log.Fatalf("S-Thread %d: Error switching to database `%s` when initializing", td.thread_id, td.current_database)
		}
	}

	log.Infof("S-Thread %d: Starting import", td.thread_id)
	var cont = true
	for cont {
		cont = process_schema(o, td)
	}
	log.Infof("S-Thread %d: Import completed", td.thread_id)

	if td.thrconn != nil {
		err = td.thrconn.Close()
	}
	log.Debugf("S-Thread %d: ending", td.thread_id)
	_ = err
}

func initialize_worker_schema(o *OptionEntries, conf *configuration) {
	var n uint
	o.global.init_connection_mutex = g_mutex_new()
	o.global.refresh_db_queue2 = g_async_queue_new(o.Common.BufferSize)
	o.global.schema_threads = new(sync.WaitGroup)
	o.global.schema_td = make([]*thread_data, o.Threads.MaxThreadsForSchemaCreation)
	log.Infof("Initializing initialize_worker_schema")
	for n = 0; n < o.Threads.MaxThreadsForSchemaCreation; n++ {
		t := new(thread_data)
		o.global.schema_td[n] = t
		o.global.schema_td[n].conf = conf
		o.global.schema_td[n].thread_id = n + 1 + o.Common.NumThreads
		o.global.schema_td[n].status = WAITING
		o.global.schema_threads.Add(1)
		go worker_schema_thread(o, o.global.schema_td[n])
	}

}

func wait_schema_worker_to_finish(o *OptionEntries) {
	o.global.schema_threads.Wait()
}

func free_schema_worker_threads(o *OptionEntries) {
	o.global.schema_td = nil
	o.global.schema_threads = nil
}
