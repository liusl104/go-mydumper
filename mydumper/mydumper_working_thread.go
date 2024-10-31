package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

type process_fun func(o *OptionEntries, td *thread_data, job *job) bool
type write_fun func(p []byte) (int, error)
type close_fun func() error
type flush_fun func() error

func initialize_working_thread(o *OptionEntries) {
	o.global.database_counter = 0
	if o.global.max_chunk_step_size > math.MaxUint64/uint64(o.Common.NumThreads) {
		o.global.max_chunk_step_size = math.MaxUint64 / uint64(o.Common.NumThreads)
		log.Errorf("This should not happen")
	}
	o.global.character_set_hash = make(map[string]string)
	o.global.character_set_hash_mutex = g_mutex_new()
	o.global.innodb_table = new(MList)
	o.global.non_innodb_table = new(MList)
	o.global.innodb_table.list = nil
	o.global.non_innodb_table.list = nil
	o.global.non_innodb_table.mutex = g_mutex_new()
	o.global.innodb_table.mutex = g_mutex_new()
	o.global.view_schemas_mutex = g_mutex_new()
	o.global.table_schemas_mutex = g_mutex_new()
	o.global.trigger_schemas_mutex = g_mutex_new()
	o.global.all_dbts_mutex = g_mutex_new()
	o.global.init_mutex = g_mutex_new()
	o.global.binlog_snapshot_gtid_executed = ""
	if o.Lock.LessLocking {
		o.global.less_locking_threads = o.Common.NumThreads
	}
	if o.Filter.IgnoreEngines != "" {
		o.global.ignore = strings.Split(o.Filter.IgnoreEngines, ",")
	}

	if !o.Extra.Compress && o.Exec.ExecPerThreadExtension == "" {
		o.Exec.ExecPerThreadExtension = EMPTY_STRING
		initialize_file_handler(o, false)
	} else {
		/*if o.Extra.CompressMethod != "" && (o.Exec.Exec_per_thread != "" || o.Exec.Exec_per_thread_extension != "") {
			log.Fatalf("--compression and --exec-per-thread are not comptatible")
		}*/
		// var cmd string
		if strings.Compare(strings.ToUpper(o.Extra.CompressMethod), GZIP) == 0 {
			/*cmd = get_gzip_cmd()
			if cmd == "" {
				log.Fatalf("gzip command not found on any static location, use --exec-per-thread for non default locations")
			}*/
			// o.Exec.Exec_per_thread = fmt.Sprintf("%s -c", cmd)
			o.Exec.ExecPerThreadExtension = GZIP_EXTENSION
		} else if strings.Compare(strings.ToUpper(o.Extra.CompressMethod), ZSTD) == 0 {
			/*cmd = get_zstd_cmd()
			if cmd == "" {
				log.Fatalf("zstd command not found on any static location, use --exec-per-thread for non default locations")
			}*/
			// o.Exec.Exec_per_thread = fmt.Sprintf("%s -c", cmd)
			o.Exec.ExecPerThreadExtension = ZSTD_EXTENSION
		} else {
			log.Fatalf("%s command not found on any static location", o.Extra.CompressMethod)
		}
		initialize_file_handler(o, true)
	}
	initialize_jobs(o)
	initialize_chunk(o)
	if o.Checksum.DumpChecksums {
		o.Checksum.DataChecksums = true
		o.Checksum.SchemaChecksums = true
		o.Checksum.RoutineChecksums = true
	}
}

func finalize_working_thread(o *OptionEntries) {
	o.global.character_set_hash = nil
	o.global.character_set_hash_mutex = nil
	o.global.view_schemas_mutex = nil
	o.global.table_schemas_mutex = nil
	o.global.trigger_schemas_mutex = nil
	o.global.all_dbts_mutex = nil
	o.global.init_mutex = nil
	if o.global.binlog_snapshot_gtid_executed != "" {
		o.global.binlog_snapshot_gtid_executed = ""
	}
	finalize_chunk(o)
}

func thd_JOB_TABLE(o *OptionEntries, td *thread_data, job *job) {
	var dtj *dump_table_job = job.job_data.(*dump_table_job)
	new_table_to_dump(o, td.thrconn, td.conf, dtj.is_view, dtj.is_sequence, dtj.database, dtj.table, dtj.collation, dtj.engine)
	dtj.collation = ""
	dtj.engine = ""
	dtj = nil
}

func thd_JOB_DUMP_ALL_DATABASES(o *OptionEntries, td *thread_data, job *job) {
	var databases *mysql.Result
	var err error
	databases, err = td.thrconn.Execute("SHOW DATABASES")
	if err != nil {
		log.Fatalf("Unable to list databases: %v", err)
	}
	for _, row := range databases.Values {
		if fmt.Sprintf("%s", row[0].AsString()) == "information_schema" ||
			fmt.Sprintf("%s", row[0].AsString()) == "performance_schema" ||
			fmt.Sprintf("%s", row[0].AsString()) == "data_dictionary" ||
			(o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, string(row[0].AsString()), "")) {
			continue
		}

		var db_tmp *database
		if get_database(o, td.thrconn, string(row[0].AsString()), &db_tmp) && !o.Objects.NoSchemas && eval_regex(o, string(row[0].AsString()), "") {
			db_tmp.ad_mutex.Lock()
			if !db_tmp.already_dumped {
				create_job_to_dump_schema(o, db_tmp, td.conf)
				db_tmp.already_dumped = true
			}
			db_tmp.ad_mutex.Unlock()
		}
		create_job_to_dump_database(o, db_tmp, td.conf)
	}
	if g_atomic_int_dec_and_test(&o.global.database_counter) {
		td.conf.db_ready.push(1)
	}
}

func thd_JOB_DUMP_DATABASE(o *OptionEntries, td *thread_data, job *job) {
	var ddj = job.job_data.(*dump_database_job)
	log.Infof("Thread %d: dumping db information for `%s`", td.thread_id, ddj.database.name)
	dump_database_thread(o, td.thrconn, td.conf, ddj.database)
	if g_atomic_int_dec_and_test(&o.global.database_counter) {
		td.conf.db_ready.push(1)
	}
}

func get_table_info_to_process_from_list(o *OptionEntries, conn *client.Conn, conf *configuration, table_list []string) {
	var query string
	var x int
	var dt []string
	for x = 0; x < len(table_list); x++ {
		dt = strings.Split(table_list[x], ".")
		query = fmt.Sprintf("SHOW TABLE STATUS FROM %s%s%s LIKE '%s'", o.global.identifier_quote_character_str, dt[0],
			o.global.identifier_quote_character_str, dt[1])
		var result *mysql.Result

		var err error
		result, err = conn.Execute(query)
		if err != nil {
			log.Fatalf("Error showing table status on: %s - Could not execute query: %v", dt[0], err)
		}
		var ecol int = -1
		var ccol int = -1
		var collcol int = -1
		var rowscol int = 0
		determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
		var db *database

		if get_database(o, conn, dt[0], &db) {
			if !db.already_dumped {
				db.ad_mutex.Lock()
				if !db.already_dumped {
					create_job_to_dump_schema(o, db, conf)
					db.already_dumped = true
				}
				db.ad_mutex.Unlock()
			}
		}
		if len(result.Values) == 0 {
			log.Fatalf("Could not list tables for %s", db.name)
		}

		for _, row := range result.Values {
			var is_view, is_sequence bool
			if (o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
				is_view = true
			}
			if (o.global.detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "SEQUENCE") {
				is_sequence = true
			}
			if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, db.name, string(row[0].AsString())) {
				continue
			}
			if is_mysql_special_tables(db.name, string(row[0].AsString())) {
				continue
			}
			if !eval_regex(o, db.name, string(row[0].AsString())) {
				continue
			}
			create_job_to_dump_table(conf, is_view, is_sequence, db, string(row[o.global.tablecol].AsString()), string(row[collcol].AsString()), string(row[ecol].AsString()))
		}
	}
	if g_atomic_int_dec_and_test(&o.global.database_counter) {
		conf.db_ready.push(1)
	}
}

func thd_JOB_DUMP_TABLE_LIST(o *OptionEntries, td *thread_data, job *job) {
	var dtlj = job.job_data.(*dump_table_list_job)
	get_table_info_to_process_from_list(o, td.thrconn, td.conf, dtlj.table_list)
}

func new_partition_step(partition string) *chunk_step {
	_ = partition
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.char_step = new(char_step)
	cs.integer_step = new(integer_step)
	return cs
}

func m_async_queue_push_conservative(queue *asyncQueue, element *job) {
	// Each job weights 500 bytes aprox.
	// if we reach to 200k of jobs, which is 100MB of RAM, we are going to wait 5 seconds
	// which is not too much considering that it will impossible to proccess 200k of jobs
	// in 5 seconds.
	// I don't think that we need to this values as parameters, unless that a user needs to
	// set hundreds of threads
	for queue.length > 200000 {
		log.Warnf("Too many jobs in the queue. We are pausing the jobs creation for 5 seconds.")
		time.Sleep(5 * time.Second)
	}
	g_async_queue_push(queue, element)
}

func thd_JOB_DUMP(o *OptionEntries, td *thread_data, job *job) {
	var tj = job.job_data.(*table_job)
	if o.Lock.UseSavepoints {
		if td.table_name != "" {
			if tj.dbt.table != td.table_name {
				_, err := td.thrconn.Execute(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER))
				if err != nil {
					log.Fatalf("Rollback to savepoint failed: %v", err)
				}
				_, err = td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
				if err != nil {
					log.Fatalf("Savepoint failed: %v", err)
				}
				td.table_name = tj.dbt.table

			} else {
				_, err := td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
				if err != nil {
					log.Fatalf("Savepoint failed: %v", err)
				}
				td.table_name = tj.dbt.table
			}
		} else {
			_, err := td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
			if err != nil {
				log.Fatalf("Savepoint failed: %v", err)
			}
			td.table_name = tj.dbt.table
		}
	}
	tj.td = td
	tj.chunk_step_item.chunk_functions.process(o, tj, tj.chunk_step_item)
	tj.dbt.chunks_mutex.Lock()
	tj.dbt.current_threads_running--
	tj.dbt.chunks_mutex.Unlock()
	free_table_job(o, tj)
}

func initialize_thread(o *OptionEntries, td *thread_data) {
	var err error
	td.thrconn, err = m_connect(o)
	if err != nil {
		log.Fatalf("connect db fail:%v", err)
	}
	log.Infof("Thread %d: connected using MySQL connection ID %d", td.thread_id, td.thrconn.GetConnectionID())

}

func initialize_consistent_snapshot(o *OptionEntries, td *thread_data) {
	var err error
	if o.global.sync_wait != -1 {
		_, err = td.thrconn.Execute(fmt.Sprintf("SET SESSION WSREP_SYNC_WAIT = %d", o.global.sync_wait))
		if err != nil {
			log.Fatalf("Failed to set wsrep_sync_wait for the thread: %v", err)
		}
	}
	set_transaction_isolation_level_repeatable_read(td.thrconn)
	var start_transaction_retry uint
	var cont bool
	for !cont && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
		log.Debugf("Thread %d: Start transaction # %d", td.thread_id, start_transaction_retry)
		_, err = td.thrconn.Execute("START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")
		if err != nil {
			log.Fatalf("Failed to start consistent snapshot: %v", err)
		}
		var res *mysql.Result
		res, err = td.thrconn.Execute("SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'")
		if err != nil {
			log.Warnf("Failed to get binlog_snapshot_gtid_executed: %v", err)
			td.binlog_snapshot_gtid_executed = ""
		} else {
			for _, row := range res.Values {
				if row != nil {
					td.binlog_snapshot_gtid_executed = string(row[1].AsString())
				} else {
					td.binlog_snapshot_gtid_executed = ""
				}
			}
		}
		start_transaction_retry++
		td.conf.gtid_pos_checked.push(1)
		cont = td.conf.are_all_threads_in_same_pos.pop().(int) == 1
	}
	if td.binlog_snapshot_gtid_executed == "" {
		if o.Lock.NoLocks {
			log.Warnf("We are not able to determine if the backup will be consistent.")
		} else {
			o.global.it_is_a_consistent_backup = true
		}
	} else {
		if cont {
			log.Infof("All threads in the same position. This will be a consistent backup.")
			o.global.it_is_a_consistent_backup = true
		} else {
			if o.Lock.NoLocks {
				log.Warnf("Backup will not be consistent, but we are continuing because you use --no-locks.")
			} else {
				log.Fatalf("Backup will not be consistent. Threads are in different points in time. Use --no-locks if you expect inconsistent backups.")
			}
		}
	}
}

func check_connection_status(o *OptionEntries, td *thread_data) {
	var err error
	if o.global.detected_server == SERVER_TYPE_TIDB {
		err = set_tidb_snapshot(o, td.thrconn)
		if err == nil {
			log.Infof("Thread %d: set to tidb_snapshot '%s'", td.thread_id, o.Lock.TidbSnapshot)
		}
	}
	if o.global.need_dummy_read {
		_, err = td.thrconn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy")
	}
	if o.global.need_dummy_toku_read {
		_, err = td.thrconn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy")
	}

}

func write_snapshot_info(o *OptionEntries, conn *client.Conn, file *os.File) {
	var master, mdb *mysql.Result
	var masterlog, mastergtid string
	var masterpos uint64
	var err error
	master, err = conn.Execute(o.global.show_binary_log_status)
	if err != nil {
		log.Fatalf("Failed to get master status: %v", err)
	}
	for _, row := range master.Values {
		masterlog = string(row[0].AsString())
		masterpos = row[1].AsUint64()
		if len(row) == 5 {
			mastergtid = remove_new_line(string(row[4].AsString()))
		} else {
			mdb, err = conn.Execute("SELECT @@gtid_binlog_pos")
			if err != nil {
				log.Fatalf("Failed to get master gtid pos: %v", err)
			}
			for _, row = range mdb.Values {
				mastergtid = remove_new_line(string(row[0].AsString()))
			}
		}
	}
	if masterlog != "" {
		fmt.Fprintf(file, "[source]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n")
		if o.Extra.SourceData > 0 {
			fmt.Fprintf(file, "#SOURCE_HOST = \"%s\"\n#SOURCE_PORT = \n#SOURCE_USER = \"\"\n#SOURCE_PASSWORD = \"\"\n", o.Connection.Hostname)
			if o.Extra.SourceData&1<<(3) > 0 {
				fmt.Fprintf(file, "SOURCE_SSL = 1\n")
			} else {
				fmt.Fprintf(file, "#SOURCE_SSL = {0|1}\n")
			}
			fmt.Fprintf(file, "executed_gtid_set = \"%s\"\n", mastergtid)
			if o.Extra.SourceData&1<<(4) > 0 {
				fmt.Fprintf(file, "SOURCE_AUTO_POSITION = 1\n")
				fmt.Fprintf(file, "#SOURCE_LOG_FILE = \"%s\"\n#SOURCE_LOG_POS = %d\n", masterlog, masterpos)
			} else {
				fmt.Fprintf(file, "SOURCE_LOG_FILE = \"%s\"\nSOURCE_LOG_POS = %d\n", masterlog, masterpos)
				fmt.Fprintf(file, "#SOURCE_AUTO_POSITION = {0|1}\n")
			}
			fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", o.Extra.SourceData&1<<(0))
			if o.Extra.SourceData&1<<(1) > 0 {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 1)
			} else {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 0)
			}

			if o.Extra.SourceData&1<<(2) > 0 {
				fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 1)
			} else {
				fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 0)
			}

		} else {
			fmt.Fprintf(file, "File = %s\nPosition = %d\nExecuted_Gtid_Set = %s\n", masterlog, masterpos, mastergtid)
		}
		log.Infof("Written master status")
	}
	file.Sync()
	master = nil
	mdb = nil
}

func process_job_builder_job(o *OptionEntries, td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DUMP_TABLE_LIST:
		thd_JOB_DUMP_TABLE_LIST(o, td, job)
	case JOB_DUMP_DATABASE:
		thd_JOB_DUMP_DATABASE(o, td, job)
	case JOB_DUMP_ALL_DATABASES:
		thd_JOB_DUMP_ALL_DATABASES(o, td, job)
	case JOB_TABLE:
		thd_JOB_TABLE(o, td, job)
	case JOB_WRITE_MASTER_STATUS:
		write_snapshot_info(o, td.thrconn, job.job_data.(*os.File))
		td.conf.binlog_ready.push(1)
	case JOB_SHUTDOWN:
		return false
	default:
		log.Errorf("Something very bad happened!")
	}
	return true
}

func process_job(o *OptionEntries, td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DETERMINE_CHUNK_TYPE:
		set_chunk_strategy_for_dbt(o, td.thrconn, job.job_data.(*db_table))
	case JOB_DUMP:
		thd_JOB_DUMP(o, td, job)
	case JOB_DUMP_NON_INNODB:
		thd_JOB_DUMP(o, td, job)
	case JOB_DEFER:

	case JOB_CHECKSUM:
		do_JOB_CHECKSUM(o, td, job)
	case JOB_CREATE_DATABASE:
		do_JOB_CREATE_DATABASE(o, td, job)
	case JOB_CREATE_TABLESPACE:
		do_JOB_CREATE_TABLESPACE(o, td, job)
	case JOB_SCHEMA:
		do_JOB_SCHEMA(o, td, job)
	case JOB_VIEW:
		do_JOB_VIEW(o, td, job)
	case JOB_SEQUENCE:
		do_JOB_SEQUENCE(o, td, job)
	case JOB_TRIGGERS:
		do_JOB_TRIGGERS(o, td, job)
	case JOB_SCHEMA_TRIGGERS:
		do_JOB_SCHEMA_TRIGGERS(o, td, job)
	case JOB_SCHEMA_POST:
		do_JOB_SCHEMA_POST(o, td, job)
	case JOB_WRITE_MASTER_STATUS:
		write_snapshot_info(o, td.thrconn, job.job_data.(*os.File))
		td.conf.binlog_ready.push(1)
	case JOB_SHUTDOWN:
		return false
	default:
		log.Errorf("Something very bad happened!")
	}
	return true
}

func check_pause_resume(td *thread_data) {
	if td.conf.pause_resume != nil {
		task := td.conf.pause_resume.try_pop()
		if task != nil {
			td.pause_resume_mutex = task.(*sync.Mutex)
		} else {
			td.pause_resume_mutex = nil
		}
		if td.pause_resume_mutex != nil {
			log.Infof("Thread %d: Pausing thread", td.thread_id)
			td.pause_resume_mutex.Lock()
			td.pause_resume_mutex.Unlock()
			td.pause_resume_mutex = nil
		}
	}
}

func process_queue(o *OptionEntries, queue *asyncQueue, td *thread_data, do_builder bool, chunk_step_queue *asyncQueue) {
	var j *job
	for {
		check_pause_resume(td)
		if chunk_step_queue != nil {
			chunk_step_queue.push(1)
		}
		j = queue.pop().(*job)
		if o.global.shutdown_triggered && j.types != JOB_SHUTDOWN {
			log.Infof("Thread %d: Process has been cacelled", td.thread_id)
			return
		}
		if do_builder {
			if !process_job_builder_job(o, td, j) {
				break
			}
		} else {
			if !process_job(o, td, j) {
				break
			}
		}
	}
}

func build_lock_tables_statement(o *OptionEntries, conf *configuration) {
	o.global.non_innodb_table_mutex.Lock()
	var dbt *db_table
	var i int
	for i, dbt = range o.global.non_innodb_table.list {
		if i == 0 {
			conf.lock_tables_statement = fmt.Sprintf("LOCK TABLES %s%s%s.%s%s%s READ LOCAL", o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str,
				o.global.identifier_quote_character_str, dbt.table, o.global.identifier_quote_character_str)
			continue
		}
		conf.lock_tables_statement += fmt.Sprintf(", %s%s%s.%s%s%s READ LOCAL", o.global.identifier_quote_character_str, dbt.database.name, o.global.identifier_quote_character_str,
			o.global.identifier_quote_character_str, dbt.table, o.global.identifier_quote_character_str)
	}
	o.global.non_innodb_table_mutex.Unlock()
}

func update_estimated_remaining_chunks_on_dbt(dbt *db_table) {
	var total uint64
	for _, l := range dbt.chunks {
		switch l.(*chunk_step_item).chunk_type {
		case INTEGER:
			total += l.(*chunk_step).integer_step.estimated_remaining_steps
		case CHAR:
			total += l.(*chunk_step).char_step.estimated_remaining_steps
		default:
			total++
		}
	}
	dbt.estimated_remaining_steps = total
}

func working_thread(o *OptionEntries, td *thread_data) {
	if o.global.threads == nil {
		o.global.threads = new(sync.WaitGroup)
	}
	o.global.threads.Add(1)
	defer o.global.threads.Done()
	o.global.init_mutex.Lock()
	var err error
	td.thrconn, err = m_connect(o)
	o.global.init_mutex.Unlock()
	initialize_thread(o, td)
	execute_gstring(td.thrconn, o.global.set_session)
	// Initialize connection
	if !o.Statement.SkipTz {
		_, err = td.thrconn.Execute("/*!40103 SET TIME_ZONE='+00:00' */")
		if err != nil {
			log.Fatalf("Failed to set time zone: %v", err)
		}
	}

	if !td.less_locking_stage {
		if o.Lock.UseSavepoints {
			_, err = td.thrconn.Execute("SET SQL_LOG_BIN = 0")
			if err != nil {
				log.Fatalf("Failed to disable binlog for the thread: %v", err)
			}
		}
		initialize_consistent_snapshot(o, td)
		check_connection_status(o, td)
	}
	td.conf.ready.push(1)
	// Thread Ready to process jobs
	log.Infof("Thread %d: Creating Jobs", td.thread_id)
	process_queue(o, td.conf.initial_queue, td, true, nil)
	td.conf.ready.push(1)
	log.Infof("Thread %d: Schema queue", td.thread_id)
	process_queue(o, td.conf.schema_queue, td, false, nil)

	if o.Stream.Stream {
		send_initial_metadata(o)
	}
	if !o.Objects.NoData {
		log.Infof("Thread %d: Schema Done, Starting Non-Innodb", td.thread_id)

		td.conf.ready.push(1)
		td.conf.ready_non_innodb_queue.pop()
		if o.Lock.LessLocking {
			// Sending LOCK TABLE over all non-innodb tables
			if td.conf.lock_tables_statement != "" {
				_, err = td.thrconn.Execute(td.conf.lock_tables_statement)
				log.Errorf("Error locking non-innodb tables %v", err)
			}
			// This push will unlock the FTWRL on the Main Connection
			td.conf.unlock_tables.push(1)

			process_queue(o, td.conf.non_innodb.queue, td, false, td.conf.non_innodb.request_chunk)
			process_queue(o, td.conf.non_innodb.deferQueue, td, false, nil)
			_, err = td.thrconn.Execute(UNLOCK_TABLES)
			if err != nil {
				log.Errorf("Error locking non-innodb tables %v", err)
			}
		} else {
			process_queue(o, td.conf.non_innodb.queue, td, false, td.conf.non_innodb.request_chunk)
			process_queue(o, td.conf.non_innodb.deferQueue, td, false, nil)
			td.conf.unlock_tables.push(1)
		}

		log.Infof("Thread %d: Non-Innodb Done, Starting Innodb", td.thread_id)
		process_queue(o, td.conf.innodb.queue, td, false, td.conf.innodb.request_chunk)
		process_queue(o, td.conf.innodb.deferQueue, td, false, nil)
		//  start_processing(td, resume_mutex);
	} else {
		td.conf.unlock_tables.push(1)
	}
	if o.Lock.UseSavepoints && td.table_name != "" {
		_, err = td.thrconn.Execute(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER))
		if err != nil {
			log.Fatalf("Rollback to savepoint failed: %v", err)
		}
	}
	process_queue(o, td.conf.post_data_queue, td, false, nil)

	log.Infof("Thread %d: shutting down", td.thread_id)

	if td.binlog_snapshot_gtid_executed != "" {
		td.binlog_snapshot_gtid_executed = ""
	}

	if td.thrconn != nil {
		td.thrconn.Close()
	}
	return
}

func get_insertable_fields(o *OptionEntries, conn *client.Conn, database string, table string) string {
	var field_list string
	var query string
	var res *mysql.Result
	var err error
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and `extra` not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%'", database, table)
	res, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("get insertable field fail:%v", err)
	}
	var first = true
	for _, row := range res.Values {
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name string = o.global.identifier_quote_character_protect(string(row[0].AsString()))

		tb := fmt.Sprintf("%s%s%s", o.global.identifier_quote_character_str, field_name, o.global.identifier_quote_character_str)
		field_list += tb
	}
	return field_list
}

func has_json_fields(conn *client.Conn, database string, table string) bool {
	var res *mysql.Result
	var err error
	var query string
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_TYPE ='json'", database, table)
	res, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("query [%s] fail:%v", query, err)
	}
	if len(res.Values) == 0 {
		return false
	}
	for _, row := range res.Values {
		_ = row
		return true
	}
	return false
}

func get_anonymized_function_for(o *OptionEntries, conn *client.Conn, database string, table string) []*function_pointer {
	var k = fmt.Sprintf("`%s`.`%s`", database, table)
	ht, ok := o.global.conf_per_table.all_anonymized_function[k]
	var anonymized_function_list []*function_pointer
	if ok {
		query := fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;", database, table)
		res, _ := conn.Execute(query)
		log.Infof("Using masquerade function on `%s`.`%s`", database, table)
		for _, row := range res.Values {
			fp, _ := ht[string(row[0].AsString())]
			// if fp != nil {
			if fp != "nil" {
				log.Infof("Masquerade function found on `%s`.`%s`.`%s`", database, table, row[0].AsString())
				// anonymized_function_list = append(anonymized_function_list, fp)
			} else {
				if o.global.pp == nil {
					o.global.pp = nil
				}
				anonymized_function_list = append(anonymized_function_list, o.global.pp)
			}
		}
	}
	return anonymized_function_list
}

func detect_generated_fields(o *OptionEntries, conn *client.Conn, database string, table string) bool {
	var res *mysql.Result
	var err error
	var result bool
	var query string
	if o.Extra.IgnoreGeneratedFields {
		return false
	}
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%' and extra not like '%%DEFAULT_GENERATED%%'", database, table)
	res, err = conn.Execute(query)
	if err != nil {
		log.Errorf("get column name fail:%v", err)
		return false
	}
	for _, row := range res.Values {
		_ = row
		result = true
	}
	return result
}

func get_character_set_from_collation(o *OptionEntries, conn *client.Conn, collation string) string {
	o.global.character_set_hash_mutex.Lock()
	character_set, _ := o.global.character_set_hash[collation]
	if character_set == "" {
		query := fmt.Sprintf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE collation_name='%s'", collation)
		res, err := conn.Execute(query)
		if err != nil {
			log.Errorf("get character set name fail:%v", err)
		}
		for _, row := range res.Values {
			character_set = string(row[0].AsString())
			o.global.character_set_hash[collation] = character_set
		}
	}
	o.global.character_set_hash_mutex.Unlock()
	return character_set
}

func get_primary_key_separated_by_comma(o *OptionEntries, dbt *db_table) {
	var field_list string
	var list = dbt.primary_key
	var first = true
	for _, row := range list {
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name = o.global.identifier_quote_character_protect(row)
		var tb = fmt.Sprintf("%s%s%s", o.global.identifier_quote_character_str, field_name, o.global.identifier_quote_character_str)
		field_list += tb
	}
	dbt.primary_key_separated_by_comma = field_list
}

func new_db_table(o *OptionEntries, d **db_table, conn *client.Conn, conf *configuration, database *database, table string, table_collation string, is_sequence bool) bool {
	var b bool
	var lkey = build_dbt_key(o, database.name, table)
	o.global.all_dbts_mutex.Lock()
	var dbt *db_table
	dbt = o.global.all_dbts[lkey]
	if dbt != nil {
		b = false
		o.global.all_dbts_mutex.Unlock()
	} else {
		dbt = new(db_table)
		dbt.key = lkey
		dbt.object_to_export = new(object_to_export)
		dbt.status = UNDEFINED
		o.global.all_dbts[lkey] = dbt
		o.global.all_dbts_mutex.Unlock()
		dbt.database = database
		dbt.table = o.global.identifier_quote_character_protect(table)
		dbt.table_filename = get_ref_table(o, dbt.table)
		dbt.is_sequence = is_sequence
		if table_collation == "" {
			dbt.character_set = ""
		} else {
			dbt.character_set = get_character_set_from_collation(o, conn, table_collation)
		}
		dbt.has_json_fields = has_json_fields(conn, dbt.database.name, dbt.table)
		dbt.rows_lock = g_mutex_new()
		dbt.escaped_table = escape_string(dbt.table)
		dbt.anonymized_function = get_anonymized_function_for(o, conn, dbt.database.name, dbt.table)
		dbt.where = o.global.conf_per_table.all_where_per_table[lkey]
		dbt.limit = o.global.conf_per_table.all_limit_per_table[lkey]
		dbt.columns_on_select = o.global.conf_per_table.all_columns_on_select_per_table[lkey]
		dbt.columns_on_insert = o.global.conf_per_table.all_columns_on_insert_per_table[lkey]
		parse_object_to_export(dbt.object_to_export, o.global.conf_per_table.all_object_to_export[lkey])
		dbt.partition_regex = o.global.conf_per_table.all_partition_regex_per_table[lkey]
		dbt.max_threads_per_table = o.Chunks.MaxThreadsPerTable
		dbt.current_threads_running = 0
		var rows_p_chunk = o.global.conf_per_table.all_rows_per_table[lkey]
		if rows_p_chunk != "" {
			dbt.split_integer_tables = parse_rows_per_chunk(rows_p_chunk, &(dbt.min_chunk_step_size), &(dbt.starting_chunk_step_size), &(dbt.max_chunk_step_size))
		} else {
			dbt.split_integer_tables = o.global.split_integer_tables
			dbt.min_chunk_step_size = o.global.min_chunk_step_size
			dbt.starting_chunk_step_size = o.global.starting_chunk_step_size
			dbt.max_chunk_step_size = o.global.max_chunk_step_size
		}
		if dbt.min_chunk_step_size == 1 && dbt.min_chunk_step_size == dbt.starting_chunk_step_size && dbt.starting_chunk_step_size != dbt.max_chunk_step_size {
			dbt.min_chunk_step_size = 2
			dbt.starting_chunk_step_size = 2
			log.Warnf("Setting min and start rows per file to 2 on %s", lkey)
		}
		n, ok := o.global.conf_per_table.all_num_threads_per_table[lkey]
		if ok {
			dbt.num_threads = n
		} else {
			dbt.num_threads = o.Common.NumThreads
		}
		dbt.estimated_remaining_steps = 1
		dbt.min = ""
		dbt.max = ""
		dbt.chunks = nil
		dbt.load_data_header = new(strings.Builder)
		dbt.load_data_suffix = new(strings.Builder)
		dbt.insert_statement = nil
		dbt.chunks_mutex = g_mutex_new()
		//  g_mutex_lock(dbt.chunks_mutex);
		dbt.chunks_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
		dbt.chunks_completed = 0
		get_primary_key(o, conn, dbt, conf)
		dbt.primary_key_separated_by_comma = ""
		if o.Extra.OrderByPrimaryKey {
			get_primary_key_separated_by_comma(o, dbt)
		}
		dbt.multicolumn = len(dbt.primary_key) > 1

		//  dbt.primary_key = get_primary_key_string(conn, dbt.database.name, dbt.table);
		dbt.chunk_filesize = o.Extra.ChunkFilesize
		//  create_job_to_determine_chunk_type(dbt, g_async_queue_push, );

		dbt.complete_insert = o.Statement.CompleteInsert || detect_generated_fields(o, conn, dbt.database.escaped, dbt.escaped_table)
		if dbt.complete_insert {
			dbt.select_fields = get_insertable_fields(o, conn, dbt.database.escaped, dbt.escaped_table)
		} else {
			dbt.select_fields = "*"
		}
		dbt.indexes_checksum = ""
		dbt.data_checksum = ""
		dbt.schema_checksum = ""
		dbt.triggers_checksum = ""
		dbt.rows = 0
		// dbt.chunk_functions.process=NULL;
		b = true
	}
	*d = dbt
	return b
}

func free_db_table(dbt *db_table) {
	dbt.chunks_mutex.Lock()
	dbt.rows_lock = nil
	dbt.escaped_table = ""
	dbt.insert_statement = nil
	dbt.select_fields = ""
	dbt.min = ""
	dbt.max = ""
	dbt.data_checksum = ""
	dbt.chunks_completed = 0
	dbt.chunks_mutex.Unlock()
	dbt = nil
}

func new_table_to_dump(o *OptionEntries, conn *client.Conn, conf *configuration, is_view bool, is_sequence bool, database *database, table string, collation string, ecol string) {
	database.ad_mutex.Lock()
	if !database.already_dumped {
		create_job_to_dump_schema(o, database, conf)
		database.already_dumped = true
	}
	database.ad_mutex.Unlock()

	var dbt *db_table
	var b = new_db_table(o, &dbt, conn, conf, database, table, collation, is_sequence)
	if b {
		if (!is_view || o.Objects.ViewsAsTables) && !is_sequence {
			// with trx_consistency_only we dump all as innodb_table
			if !o.Objects.NoSchemas && !dbt.object_to_export.no_schema {
				//      write_table_metadata_into_file(dbt);
				o.global.table_schemas_mutex.Lock()
				o.global.table_schemas = append(o.global.table_schemas, dbt)
				o.global.table_schemas_mutex.Unlock()
				create_job_to_dump_table_schema(o, dbt, conf)
			}
			if o.Objects.DumpTriggers && !database.dump_triggers && !dbt.object_to_export.no_trigger {
				create_job_to_dump_triggers(o, conn, dbt, conf)
			}
			if !o.Objects.NoData && !dbt.object_to_export.no_data {
				if ecol != "" && strings.ToUpper(ecol) != "MRG_MYISAM" {
					if o.Checksum.DataChecksums && !(o.get_major() == 5 && o.get_secondary() == 7 && dbt.has_json_fields) {
						create_job_to_dump_checksum(o, dbt, conf)
					}
					if o.Lock.TrxConsistencyOnly || (ecol != "" && (ecol == "InnoDB" || ecol == "TokuDB")) {
						dbt.is_innodb = true
						o.global.innodb_table.mutex.Lock()
						o.global.innodb_table.list = append(o.global.innodb_table.list, dbt)
						o.global.innodb_table.mutex.Unlock()

					} else {
						dbt.is_innodb = false
						o.global.non_innodb_table.mutex.Lock()
						o.global.non_innodb_table.list = append(o.global.non_innodb_table.list, dbt)
						o.global.non_innodb_table.mutex.Unlock()
					}
				} else {
					if is_view {
						dbt.is_innodb = false
						o.global.non_innodb_table.mutex.Lock()
						o.global.non_innodb_table.list = append(o.global.non_innodb_table.list, dbt)
						o.global.non_innodb_table.mutex.Unlock()
					}
				}
			}
		} else if is_view {
			if !o.Objects.NoSchemas && !dbt.object_to_export.no_schema {
				create_job_to_dump_view(o, dbt, conf)
			}
		} else { // is_sequence
			if !o.Objects.NoSchemas && !dbt.object_to_export.no_schema {
				create_job_to_dump_sequence(o, dbt, conf)
			}
		}
	}
	// if a view or sequence we care only about schema
}

func determine_if_schema_is_elected_to_dump_post(o *OptionEntries, conn *client.Conn, database *database) bool {
	var query string
	var result *mysql.Result
	var err error
	if o.Objects.DumpRoutines {
		if o.global.nroutines < 0 {
			log.Fatalf("nroutines value error")
		}
		var r uint
		for r = 0; r < o.global.nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", o.global.routine_type[r], database.escaped)
			result, err = conn.Execute(query)
			if err != nil {
				log.Errorf("Error showing procedure on: %s - Could not execute query: %v", database.name, err)
				return false
			}
			for _, row := range result.Values {
				if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, database.name, string(row[1].AsString())) {
					continue
				}
				if !eval_regex(o, database.name, string(row[1].AsString())) {
					continue
				}
				return true
			}
		}

		if o.Objects.DumpEvents {
			query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", o.global.identifier_quote_character_str, database.name, o.global.identifier_quote_character_str)
			result, err = conn.Execute(query)
			if err != nil {
				log.Errorf("Error showing function on: %s - Could not execute query: %v", database.name, err)
				return false
			}
			for _, row := range result.Values {
				if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, database.name, string(row[1].AsString())) {
					continue
				}
				if !eval_regex(o, database.name, string(row[1].AsString())) {
					continue
				}
				return true
			}
		}

	}
	return false
}

func dump_database_thread(o *OptionEntries, conn *client.Conn, conf *configuration, database *database) {
	var query = fmt.Sprintf("SHOW TABLE STATUS")
	var err error
	var result *mysql.Result
	err = conn.UseDB(database.name)
	if err != nil {
		log.Errorf("Could not select database: %s (%v)", database.name, err)
		return
	}
	result, err = conn.Execute(query)
	if err != nil {
		log.Errorf("Error showing table on: %s - Could not execute query: %v", database.name, err)
	}

	var ecol int = -1
	var ccol int = -1
	var collcol int = -1
	var rowscol int = 0
	determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
	if len(result.Values) == 0 {
		log.Errorf("Could not list tables for %s", database.name)
		return
	}

	var row []mysql.FieldValue
	for _, row = range result.Values {
		var dump = true
		var is_view = false
		var is_sequence = false
		if (o.is_mysql_like() || o.global.detected_server == SERVER_TYPE_TIDB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
			is_view = true
		}
		if o.global.detected_server == SERVER_TYPE_MARIADB && string(row[ccol].AsString()) == "SEQUENCE" {
			is_sequence = true
		}
		if !is_view && row[ecol].Value() == nil {
			log.Warnf("Broken table detected, please review: %s.%s", database.name, row[0].AsString())
			if o.Extra.ExitIfBrokenTableFound {
				os.Exit(EXIT_FAILURE)
			}
			dump = false
		}
		if dump && len(o.global.ignore) > 0 && !is_view && !is_sequence {
			for _, ignore := range o.global.ignore {
				if strings.ToLower(ignore) == strings.ToLower(string(row[ecol].AsString())) {
					dump = false
					break
				}
			}
		}
		if is_view && o.Objects.NoDumpViews {
			dump = false
		}

		if is_sequence && o.global.no_dump_sequences {
			dump = false
		}
		if !dump {
			continue
		}
		if len(o.global.tables) > 0 && !is_table_in_list(database.name, string(row[0].AsString()), o.global.tables) {
			continue
		}
		/* Special tables */
		if is_mysql_special_tables(database.name, string(row[0].AsString())) {
			dump = false
			continue
		}
		/* Checks skip list on 'database.table' string */
		if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, database.name, string(row[0].AsString())) {
			continue
		}

		/* Checks PCRE expressions on 'database.table' string */
		if !eval_regex(o, database.name, string(row[0].AsString())) {
			continue
		}

		/* Check if the table was recently updated */
		if len(o.global.no_updated_tables) > 0 && !is_view && !is_sequence {
			for _, iter := range o.global.no_updated_tables {
				if strings.Compare(iter, fmt.Sprintf("%s.%s", database.name, row[0].AsString())) == 0 {
					log.Infof("NO UPDATED TABLE: %s.%s", database.name, row[0].AsString())
					dump = false
				}
			}
		}

		if !dump {
			continue
		}
		create_job_to_dump_table(conf, is_view, is_sequence, database, string(row[o.global.tablecol].AsString()), string(row[collcol].AsString()), string(row[ecol].AsString()))
	}
	if determine_if_schema_is_elected_to_dump_post(o, conn, database) {
		create_job_to_dump_post(o, database, conf)
	}
	if database.dump_triggers {
		create_job_to_dump_schema_triggers(o, database, conf)
	}
	if o.Objects.DumpTriggers && database.dump_triggers {
		create_job_to_dump_schema_triggers(o, database, conf)
	}
	return
}
