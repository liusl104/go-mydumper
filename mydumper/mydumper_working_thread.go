package mydumper

import (
	"container/list"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	INSERT_IGNORE = "INSERT IGNORE"
	INSERT        = "INSERT"
	REPLACE       = "REPLACE"
	UNLOCK_TABLES = "UNLOCK TABLES"
	EMPTY_STRING  = ""
)

var (
	DiskLimits                           string
	ExitIfBrokenTableFound               bool
	SuccessOn1146                        bool
	BuildEmptyFiles                      bool
	UseSavepoints                        bool
	DumpChecksums                        bool
	DataChecksums                        bool
	SchemaChecksums                      bool
	RoutineChecksums                     bool
	IgnoreEngines                        string
	WhereOption                          string
	DumpEvents                           bool
	DumpRoutines                         bool
	ViewsAsTables                        bool
	NoDumpViews                          bool
	SkipTz                               bool
	starting_chunk_step_size             uint64
	min_chunk_step_size                  uint64
	max_chunk_step_size                  uint64
	database_counter                     int64
	character_set_hash                   map[string]string
	character_set_hash_mutex             *sync.Mutex
	innodb_table                         *MList
	non_innodb_table                     *MList
	view_schemas_mutex                   *sync.Mutex
	table_schemas_mutex                  *sync.Mutex
	all_dbts_mutex                       *sync.Mutex
	trigger_schemas_mutex                *sync.Mutex
	init_mutex                           *sync.Mutex
	binlog_snapshot_gtid_executed        string
	binlog_snapshot_gtid_executed_status bool
	binlog_snapshot_gtid_executed_count  uint
	ignore                               []string
	less_locking_threads                 uint
	sync_wait                            int = -1
	tablecol                             uint
	no_dump_sequences                    bool
)

type process_fun func(td *thread_data, job *job) bool
type write_fun func(p []byte) (int, error)
type close_fun func() error
type flush_fun func() error

func initialize_working_thread() {
	database_counter = 0
	if max_chunk_step_size > math.MaxUint64/uint64(NumThreads) {
		max_chunk_step_size = math.MaxUint64 / uint64(NumThreads)
		log.Errorf("This should not happen")
	}
	character_set_hash = make(map[string]string)
	character_set_hash_mutex = G_mutex_new()
	innodb_table = new(MList)
	non_innodb_table = new(MList)
	innodb_table.list = nil
	non_innodb_table.list = nil
	non_innodb_table.mutex = G_mutex_new()
	innodb_table.mutex = G_mutex_new()
	view_schemas_mutex = G_mutex_new()
	table_schemas_mutex = G_mutex_new()
	trigger_schemas_mutex = G_mutex_new()
	all_dbts_mutex = G_mutex_new()
	init_mutex = G_mutex_new()
	binlog_snapshot_gtid_executed = ""
	if LessLocking {
		less_locking_threads = NumThreads
	}
	if IgnoreEngines != "" {
		ignore = strings.Split(IgnoreEngines, ",")
	}

	if Compress == "" && ExecPerThreadExtension == "" {
		ExecPerThreadExtension = EMPTY_STRING
		initialize_file_handler(false)
	} else {
		/*if CompressMethod != "" && (Exec_per_thread != "" || Exec_per_thread_extension != "") {
			log.Fatalf("--compression and --exec-per-thread are not comptatible")
		}*/
		// var cmd string
		if strings.Compare(strings.ToUpper(compress_method), GZIP) == 0 {
			/*cmd = get_gzip_cmd()
			if cmd == "" {
				log.Fatalf("gzip command not found on any static location, use --exec-per-thread for non default locations")
			}*/
			// Exec_per_thread = fmt.Sprintf("%s -c", cmd)
			ExecPerThreadExtension = GZIP_EXTENSION
		} else if strings.Compare(strings.ToUpper(compress_method), ZSTD) == 0 {
			/*cmd = get_zstd_cmd()
			if cmd == "" {
				log.Fatalf("zstd command not found on any static location, use --exec-per-thread for non default locations")
			}*/
			// Exec_per_thread = fmt.Sprintf("%s -c", cmd)
			ExecPerThreadExtension = ZSTD_EXTENSION
		} else {
			log.Errorf("%s command not found on any static location", compress_method)
		}
		initialize_file_handler(true)
	}
	initialize_jobs()
	initialize_chunk()
	if DumpChecksums {
		DataChecksums = true
		SchemaChecksums = true
		RoutineChecksums = true
	}
}

func finalize_working_thread() {
	character_set_hash = nil
	character_set_hash_mutex = nil
	view_schemas_mutex = nil
	table_schemas_mutex = nil
	trigger_schemas_mutex = nil
	all_dbts_mutex = nil
	init_mutex = nil
	if binlog_snapshot_gtid_executed != "" {
		binlog_snapshot_gtid_executed = ""
	}
	finalize_chunk()
}

func thd_JOB_TABLE(td *thread_data, job *job) {
	var dtj *dump_table_job = job.job_data.(*dump_table_job)
	new_table_to_dump(td.thrconn, td.conf, dtj.is_view, dtj.is_sequence, dtj.database, dtj.table, dtj.collation, dtj.engine)
	dtj.collation = ""
	dtj.engine = ""
	dtj = nil
}

func thd_JOB_DUMP_ALL_DATABASES(td *thread_data, job *job) {
	var databases *mysql.Result
	var err error
	databases = td.thrconn.Execute("SHOW DATABASES")
	if td.thrconn.Err != nil {
		log.Criticalf("Unable to list databases: %v", err)
		return
	}
	for _, row := range databases.Values {
		if fmt.Sprintf("%s", row[0].AsString()) == "information_schema" ||
			fmt.Sprintf("%s", row[0].AsString()) == "performance_schema" ||
			fmt.Sprintf("%s", row[0].AsString()) == "data_dictionary" ||
			(TablesSkiplistFile != "" && Check_skiplist(string(row[0].AsString()), "")) {
			continue
		}

		var db_tmp *database
		if get_database(td.thrconn, string(row[0].AsString()), &db_tmp) && !NoSchemas && Eval_regex(string(row[0].AsString()), "") {
			db_tmp.ad_mutex.Lock()
			if !db_tmp.already_dumped {
				create_job_to_dump_schema(db_tmp, td.conf)
				db_tmp.already_dumped = true
			}
			db_tmp.ad_mutex.Unlock()
		}
		create_job_to_dump_database(db_tmp, td.conf)
	}
	if G_atomic_int_dec_and_test(&database_counter) {
		G_async_queue_push(td.conf.db_ready, 1)
	}
}

func thd_JOB_DUMP_DATABASE(td *thread_data, job *job) {
	var ddj = job.job_data.(*dump_database_job)
	log.Infof("Thread %d: dumping db information for `%s`", td.thread_id, ddj.database.name)
	dump_database_thread(td.thrconn, td.conf, ddj.database)
	if G_atomic_int_dec_and_test(&database_counter) {
		G_async_queue_push(td.conf.db_ready, 1)
	}
}

func get_table_info_to_process_from_list(conn *DBConnection, conf *configuration, table_list []string) {
	var query string
	var x int
	var dt []string
	for x = 0; x < len(table_list); x++ {
		dt = strings.Split(table_list[x], ".")
		query = fmt.Sprintf("SHOW TABLE STATUS FROM %s%s%s LIKE '%s'", Identifier_quote_character_str, dt[0],
			Identifier_quote_character_str, dt[1])
		var result *mysql.Result

		result = conn.Execute(query)
		if conn.Err != nil {
			log.Criticalf("Error showing table status on: %s - Could not execute query: %v", dt[0], conn.Err)
			errors++
			return
		}
		var ecol int = -1
		var ccol int = -1
		var collcol int = -1
		var rowscol int = 0
		determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
		var db *database

		if get_database(conn, dt[0], &db) {
			if !db.already_dumped {
				db.ad_mutex.Lock()
				if !db.already_dumped {
					create_job_to_dump_schema(db, conf)
					db.already_dumped = true
				}
				db.ad_mutex.Unlock()
			}
		}
		if len(result.Values) == 0 {
			log.Criticalf("Could not list tables for %s", db.name)
			errors++
			return
		}

		for _, row := range result.Values {
			var is_view, is_sequence bool
			if (Detected_server == SERVER_TYPE_MYSQL || Detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
				is_view = true
			}
			if (Detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "SEQUENCE") {
				is_sequence = true
			}
			if TablesSkiplistFile != "" && Check_skiplist(db.name, string(row[0].AsString())) {
				continue
			}
			if Is_mysql_special_tables(db.name, string(row[0].AsString())) {
				continue
			}
			if !Eval_regex(db.name, string(row[0].AsString())) {
				continue
			}
			create_job_to_dump_table(conf, is_view, is_sequence, db, string(row[tablecol].AsString()), string(row[collcol].AsString()), string(row[ecol].AsString()))
		}
	}
	if G_atomic_int_dec_and_test(&database_counter) {
		G_async_queue_push(conf.db_ready, 1)
	}
}

func thd_JOB_DUMP_TABLE_LIST(td *thread_data, job *job) {
	var dtlj = job.job_data.(*dump_table_list_job)
	get_table_info_to_process_from_list(td.thrconn, td.conf, dtlj.table_list)
}

func new_partition_step(partition string) *chunk_step {
	_ = partition
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.char_step = new(char_step)
	cs.integer_step = new(integer_step)
	return cs
}

func m_async_queue_push_conservative(queue *GAsyncQueue, element *job) {
	// Each job weights 500 bytes aprox.
	// if we reach to 200k of jobs, which is 100MB of RAM, we are going to wait 5 seconds
	// which is not too much considering that it will impossible to proccess 200k of jobs
	// in 5 seconds.
	// I don't think that we need to this values as parameters, unless that a user needs to
	// set hundreds of threads
	for G_async_queue_length(queue) > 200000 {
		log.Warnf("Too many jobs in the queue. We are pausing the jobs creation for 5 seconds.")
		time.Sleep(5 * time.Second)
	}
	G_async_queue_push(queue, element)
}

func thd_JOB_DUMP(td *thread_data, job *job) {
	var tj = job.job_data.(*table_job)
	if UseSavepoints {
		if td.table_name != "" {
			if tj.dbt.table != td.table_name {
				_ = td.thrconn.Execute("ROLLBACK TO SAVEPOINT %s", MYDUMPER)
				if td.thrconn.Err != nil {
					log.Criticalf("Rollback to savepoint failed: %v", td.thrconn.Err)
				}
				_ = td.thrconn.Execute("SAVEPOINT %s", MYDUMPER)
				if td.thrconn.Err != nil {
					log.Criticalf("Savepoint failed: %v", td.thrconn.Err)
				}
				td.table_name = tj.dbt.table

			} else {
				td.thrconn.Execute("SAVEPOINT %s", MYDUMPER)
				if td.thrconn.Err != nil {
					log.Criticalf("Savepoint failed: %v", td.thrconn.Err)
				}
				td.table_name = tj.dbt.table
			}
		} else {
			td.thrconn.Execute("SAVEPOINT %s", MYDUMPER)
			if td.thrconn.Err != nil {
				log.Criticalf("Savepoint failed: %v", td.thrconn.Err)
			}
			td.table_name = tj.dbt.table
		}
	}
	tj.td = td
	tj.chunk_step_item.chunk_functions.process(tj, tj.chunk_step_item)
	tj.dbt.chunks_mutex.Lock()
	tj.dbt.current_threads_running--
	tj.dbt.chunks_mutex.Unlock()
	free_table_job(tj)
}

func initialize_thread(td *thread_data) {
	var err error
	M_connect(td.thrconn)
	if td.thrconn.Err != nil {
		log.Criticalf("connect db fail:%v", err)
	}
	log.Infof("Thread %d: connected using MySQL connection ID %d", td.thread_id, td.thrconn.GetConnectionID())

}

func initialize_consistent_snapshot(td *thread_data) {
	// var err error
	if sync_wait != -1 {
		_ = td.thrconn.Execute("SET SESSION WSREP_SYNC_WAIT = %d", sync_wait)
		if td.thrconn.Err != nil {
			log.Criticalf("Failed to set wsrep_sync_wait for the thread: %v", td.thrconn.Err)
		}
	}
	set_transaction_isolation_level_repeatable_read(td.thrconn)
	var start_transaction_retry uint
	var cont bool
	for !cont && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
		log.Debugf("Thread %d: Start transaction # %d", td.thread_id, start_transaction_retry)
		_ = td.thrconn.Execute("START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")
		if td.thrconn.Err != nil {
			log.Criticalf("Failed to start consistent snapshot: %v", td.thrconn.Err)
		}
		var res *mysql.Result
		res = td.thrconn.Execute("SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'")
		if td.thrconn.Err != nil {
			log.Warnf("Failed to get binlog_snapshot_gtid_executed: %v", td.thrconn.Err)
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
		G_async_queue_push(td.conf.gtid_pos_checked, 1)
		cont = G_async_queue_pop(td.conf.are_all_threads_in_same_pos).(int) == 1
	}
	if td.binlog_snapshot_gtid_executed == "" {
		if NoLocks {
			log.Warnf("We are not able to determine if the backup will be consistent.")
		} else {
			it_is_a_consistent_backup = true
		}
	} else {
		if cont {
			log.Infof("All threads in the same position. This will be a consistent backup.")
			it_is_a_consistent_backup = true
		} else {
			if NoLocks {
				log.Warnf("Backup will not be consistent, but we are continuing because you use --no-locks.")
			} else {
				log.Criticalf("Backup will not be consistent. Threads are in different points in time. Use --no-locks if you expect inconsistent backups.")
			}
		}
	}
}

func check_connection_status(td *thread_data) {
	var err error
	if Detected_server == SERVER_TYPE_TIDB {
		err = set_tidb_snapshot(td.thrconn)
		if err == nil {
			log.Infof("Thread %d: set to tidb_snapshot '%s'", td.thread_id, TidbSnapshot)
		}
	}
	if need_dummy_read {
		td.thrconn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy")
	}
	if need_dummy_toku_read {
		td.thrconn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy")
	}

}

func write_snapshot_info(conn *DBConnection, file *os.File) {
	var master, mdb *mysql.Result
	var masterlog, mastergtid string
	var masterpos uint64
	master = conn.Execute(Show_binary_log_status)
	if conn.Err != nil {
		log.Warnf("Couldn't get master position: %v", conn.Err)
	}
	for _, row := range master.Values {
		masterlog = string(row[0].AsString())
		masterpos = row[1].AsUint64()
		if len(row) == 5 {
			mastergtid = Remove_new_line(string(row[4].AsString()))
		} else {
			mdb = conn.Execute("SELECT @@gtid_binlog_pos")
			if conn.Err != nil {
				log.Criticalf("Failed to get master gtid pos: %v", conn.Err)
			}
			for _, row = range mdb.Values {
				mastergtid = Remove_new_line(string(row[0].AsString()))
			}
		}
	}
	if masterlog != "" {
		fmt.Fprintf(file, "[source]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n")
		if SourceData > 0 {
			fmt.Fprintf(file, "#SOURCE_HOST = \"%s\"\n#SOURCE_PORT = \n#SOURCE_USER = \"\"\n#SOURCE_PASSWORD = \"\"\n", Hostname)
			if SourceData&1<<(3) > 0 {
				fmt.Fprintf(file, "SOURCE_SSL = 1\n")
			} else {
				fmt.Fprintf(file, "#SOURCE_SSL = {0|1}\n")
			}
			fmt.Fprintf(file, "executed_gtid_set = \"%s\"\n", mastergtid)
			if SourceData&1<<(4) > 0 {
				fmt.Fprintf(file, "SOURCE_AUTO_POSITION = 1\n")
				fmt.Fprintf(file, "#SOURCE_LOG_FILE = \"%s\"\n#SOURCE_LOG_POS = %d\n", masterlog, masterpos)
			} else {
				fmt.Fprintf(file, "SOURCE_LOG_FILE = \"%s\"\nSOURCE_LOG_POS = %d\n", masterlog, masterpos)
				fmt.Fprintf(file, "#SOURCE_AUTO_POSITION = {0|1}\n")
			}
			fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", SourceData&1<<(0))
			if SourceData&1<<(1) > 0 {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 1)
			} else {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 0)
			}

			if SourceData&1<<(2) > 0 {
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

func process_job_builder_job(td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DUMP_TABLE_LIST:
		thd_JOB_DUMP_TABLE_LIST(td, job)
	case JOB_DUMP_DATABASE:
		thd_JOB_DUMP_DATABASE(td, job)
	case JOB_DUMP_ALL_DATABASES:
		thd_JOB_DUMP_ALL_DATABASES(td, job)
	case JOB_TABLE:
		thd_JOB_TABLE(td, job)
	case JOB_WRITE_MASTER_STATUS:
		write_snapshot_info(td.thrconn, job.job_data.(*os.File))
		G_async_queue_push(td.conf.binlog_ready, 1)
	case JOB_SHUTDOWN:
		return false
	default:
		log.Errorf("Something very bad happened!")
	}
	return true
}

func process_job(td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DETERMINE_CHUNK_TYPE:
		set_chunk_strategy_for_dbt(td.thrconn, job.job_data.(*DB_Table))
	case JOB_DUMP:
		thd_JOB_DUMP(td, job)
	case JOB_DUMP_NON_INNODB:
		thd_JOB_DUMP(td, job)
	case JOB_DEFER:

	case JOB_CHECKSUM:
		do_JOB_CHECKSUM(td, job)
	case JOB_CREATE_DATABASE:
		do_JOB_CREATE_DATABASE(td, job)
	case JOB_CREATE_TABLESPACE:
		do_JOB_CREATE_TABLESPACE(td, job)
	case JOB_SCHEMA:
		do_JOB_SCHEMA(td, job)
	case JOB_VIEW:
		do_JOB_VIEW(td, job)
	case JOB_SEQUENCE:
		do_JOB_SEQUENCE(td, job)
	case JOB_TRIGGERS:
		do_JOB_TRIGGERS(td, job)
	case JOB_SCHEMA_TRIGGERS:
		do_JOB_SCHEMA_TRIGGERS(td, job)
	case JOB_SCHEMA_POST:
		do_JOB_SCHEMA_POST(td, job)
	case JOB_WRITE_MASTER_STATUS:
		write_snapshot_info(td.thrconn, job.job_data.(*os.File))
		G_async_queue_push(td.conf.binlog_ready, 1)
	case JOB_SHUTDOWN:
		return false
	default:
		log.Errorf("Something very bad happened!")
	}
	return true
}

func check_pause_resume(td *thread_data) {
	if td.conf.pause_resume != nil {
		task := G_async_queue_try_pop(td.conf.pause_resume)
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

func process_queue(queue *GAsyncQueue, td *thread_data, do_builder bool, chunk_step_queue *GAsyncQueue) {
	var j *job
	for {
		check_pause_resume(td)
		if chunk_step_queue != nil {
			G_async_queue_push(chunk_step_queue, 1)
		}
		j = G_async_queue_pop(queue).(*job)
		if shutdown_triggered && j.types != JOB_SHUTDOWN {
			log.Infof("Thread %d: Process has been cacelled", td.thread_id)
			return
		}
		if do_builder {
			if !process_job_builder_job(td, j) {
				break
			}
		} else {
			if !process_job(td, j) {
				break
			}
		}
	}
}

func build_lock_tables_statement(conf *configuration) {
	non_innodb_table.mutex.Lock()
	var dbt *DB_Table
	var iter *list.List = non_innodb_table.list
	if iter != nil {
		dbt = iter.Front().Value.(*DB_Table)
		conf.lock_tables_statement = fmt.Sprintf("LOCK TABLES %s%s%s.%s%s%s READ LOCAL", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
			Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
		for e := iter.Front().Next(); e.Value != nil; e.Next() {
			dbt = e.Value.(*DB_Table)
			conf.lock_tables_statement += fmt.Sprintf(", %s%s%s.%s%s%s READ LOCAL", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
				Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
		}
	}
	non_innodb_table.mutex.Unlock()
}

func update_estimated_remaining_chunks_on_dbt(dbt *DB_Table) {
	var total uint64
	for l := dbt.chunks.Front(); l != nil; l = l.Next() {
		switch l.Value.(*chunk_step_item).chunk_type {
		case INTEGER:
			data := l.Value.(*chunk_step_item)
			total += data.chunk_step.integer_step.estimated_remaining_steps
		case CHAR:
			data := l.Value.(*chunk_step_item)
			total += data.chunk_step.char_step.estimated_remaining_steps
		default:
			total++
		}
	}
	dbt.estimated_remaining_steps = total
}

func working_thread(td *thread_data, thread_id uint) {

	threads[thread_id].Thread.Add(1)
	defer threads[thread_id].Thread.Done()
	init_mutex.Lock()
	td.thrconn = Mysql_init()
	init_mutex.Unlock()
	initialize_thread(td)
	Execute_gstring(td.thrconn, Set_session)
	// Initialize connection
	if !SkipTz {
		_ = td.thrconn.Execute("/*!40103 SET TIME_ZONE='+00:00' */")
		if td.thrconn.Err != nil {
			log.Criticalf("Failed to set time zone: %v", td.thrconn.Err)
		}
	}

	if !td.less_locking_stage {
		if UseSavepoints {
			_ = td.thrconn.Execute("SET SQL_LOG_BIN = 0")
			if td.thrconn.Err != nil {
				log.Criticalf("Failed to disable binlog for the thread: %v", td.thrconn.Err)
			}
		}
		initialize_consistent_snapshot(td)
		check_connection_status(td)
	}
	G_async_queue_push(td.conf.ready, 1)
	// Thread Ready to process jobs
	log.Infof("Thread %d: Creating Jobs", td.thread_id)
	process_queue(td.conf.initial_queue, td, true, nil)
	G_async_queue_push(td.conf.ready, 1)
	log.Infof("Thread %d: Schema queue", td.thread_id)
	process_queue(td.conf.schema_queue, td, false, nil)

	if Stream != "" {
		send_initial_metadata()
	}
	if !NoData {
		log.Infof("Thread %d: Schema Done, Starting Non-Innodb", td.thread_id)

		G_async_queue_push(td.conf.ready, 1)
		G_async_queue_pop(td.conf.ready_non_innodb_queue)
		if LessLocking {
			// Sending LOCK TABLE over all non-innodb tables
			if td.conf.lock_tables_statement != "" {
				_ = td.thrconn.Execute(td.conf.lock_tables_statement)
				if td.thrconn.Err != nil {
					log.Errorf("Error locking non-innodb tables %v", td.thrconn.Err)
				}
			}
			// This push will unlock the FTWRL on the Main Connection
			G_async_queue_push(td.conf.unlock_tables, 1)

			process_queue(td.conf.non_innodb.queue, td, false, td.conf.non_innodb.request_chunk)
			process_queue(td.conf.non_innodb.deferQueue, td, false, nil)
			_ = td.thrconn.Execute(UNLOCK_TABLES)
			if td.thrconn.Err != nil {
				log.Errorf("Error locking non-innodb tables %v", td.thrconn.Err)
			}
		} else {
			process_queue(td.conf.non_innodb.queue, td, false, td.conf.non_innodb.request_chunk)
			process_queue(td.conf.non_innodb.deferQueue, td, false, nil)
			G_async_queue_push(td.conf.unlock_tables, 1)
		}

		log.Infof("Thread %d: Non-Innodb Done, Starting Innodb", td.thread_id)
		process_queue(td.conf.innodb.queue, td, false, td.conf.innodb.request_chunk)
		process_queue(td.conf.innodb.deferQueue, td, false, nil)
		//  start_processing(td, resume_mutex);
	} else {
		G_async_queue_push(td.conf.unlock_tables, 1)
	}
	if UseSavepoints && td.table_name != "" {
		_ = td.thrconn.Execute(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER))
		if td.thrconn.Err != nil {
			log.Criticalf("Rollback to savepoint failed: %v", td.thrconn.Err)
		}
	}
	process_queue(td.conf.post_data_queue, td, false, nil)

	log.Infof("Thread %d: shutting down", td.thread_id)

	if td.binlog_snapshot_gtid_executed != "" {
		td.binlog_snapshot_gtid_executed = ""
	}

	if td.thrconn != nil {
		td.thrconn.Close()
	}
	return
}

func get_insertable_fields(conn *DBConnection, database string, table string) string {
	var field_list string
	var query string
	var res *mysql.Result
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and `extra` not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%'", database, table)
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Criticalf("get insertable field fail:%v", conn.Err)
	}
	var first = true
	for _, row := range res.Values {
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name string = identifier_quote_character_protect(string(row[0].AsString()))

		tb := fmt.Sprintf("%s%s%s", Identifier_quote_character_str, field_name, Identifier_quote_character_str)
		field_list += tb
	}
	return field_list
}

func has_json_fields(conn *DBConnection, database string, table string) bool {
	var res *mysql.Result
	var query string
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_TYPE ='json'", database, table)
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Criticalf("query [%s] fail:%v", query, conn.Err)
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

func get_anonymized_function_for(conn *DBConnection, database string, table string) []*Function_pointer {
	var k = fmt.Sprintf("`%s`.`%s`", database, table)
	ht, ok := conf_per_table.All_anonymized_function[k]
	var anonymized_function_list []*Function_pointer
	if ok {
		query := fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;", database, table)
		res := conn.Execute(query)
		log.Infof("Using masquerade function on `%s`.`%s`", database, table)
		for _, row := range res.Values {
			fp, _ := ht[string(row[0].AsString())]
			// if fp != nil {
			if fp != nil {
				log.Infof("Masquerade function found on `%s`.`%s`.`%s`", database, table, row[0].AsString())
				anonymized_function_list = append(anonymized_function_list, fp)
			} else {
				if pp == nil {
					pp = nil
				}
				anonymized_function_list = append(anonymized_function_list, pp)
			}
		}
	}
	return anonymized_function_list
}

func detect_generated_fields(conn *DBConnection, database string, table string) bool {
	var res *mysql.Result
	var result bool
	var query string
	if IgnoreGeneratedFields {
		return false
	}
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%' and extra not like '%%DEFAULT_GENERATED%%'", database, table)
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Errorf("get column name fail:%v", conn.Err)
		return false
	}
	for _, row := range res.Values {
		_ = row
		result = true
	}
	return result
}

func get_character_set_from_collation(conn *DBConnection, collation string) string {
	character_set_hash_mutex.Lock()
	character_set, _ := character_set_hash[collation]
	if character_set == "" {
		query := fmt.Sprintf("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE collation_name='%s'", collation)
		res := conn.Execute(query)
		if conn.Err != nil {
			log.Errorf("get character set name fail:%v", conn.Err)
		}
		for _, row := range res.Values {
			character_set = string(row[0].AsString())
			character_set_hash[collation] = character_set
		}
	}
	character_set_hash_mutex.Unlock()
	return character_set
}

func get_primary_key_separated_by_comma(dbt *DB_Table) {
	var field_list string
	var list = dbt.primary_key
	var first = true
	for _, row := range list {
		if first {
			first = false
		} else {
			field_list += ","
		}
		var field_name = identifier_quote_character_protect(row)
		var tb = fmt.Sprintf("%s%s%s", Identifier_quote_character_str, field_name, Identifier_quote_character_str)
		field_list += tb
	}
	dbt.primary_key_separated_by_comma = field_list
}

func new_db_table(d **DB_Table, conn *DBConnection, conf *configuration, database *database, table string, table_collation string, is_sequence bool) bool {
	var b bool
	var lkey = Build_dbt_key(database.name, table)
	all_dbts_mutex.Lock()
	var dbt *DB_Table
	dbt = all_dbts[lkey]
	if dbt != nil {
		b = false
		all_dbts_mutex.Unlock()
	} else {
		dbt = new(DB_Table)
		dbt.key = lkey
		dbt.object_to_export = new(Object_to_export)
		dbt.status = UNDEFINED
		all_dbts[lkey] = dbt
		all_dbts_mutex.Unlock()
		dbt.database = database
		dbt.table = identifier_quote_character_protect(table)
		dbt.table_filename = get_ref_table(dbt.table)
		dbt.is_sequence = is_sequence
		if table_collation == "" {
			dbt.character_set = ""
		} else {
			dbt.character_set = get_character_set_from_collation(conn, table_collation)
		}
		dbt.has_json_fields = has_json_fields(conn, dbt.database.name, dbt.table)
		dbt.rows_lock = G_mutex_new()
		dbt.escaped_table = escape_string(dbt.table)
		dbt.anonymized_function = get_anonymized_function_for(conn, dbt.database.name, dbt.table)
		dbt.where = conf_per_table.All_where_per_table[lkey]
		dbt.limit = conf_per_table.All_limit_per_table[lkey]
		dbt.columns_on_select = conf_per_table.All_columns_on_select_per_table[lkey]
		dbt.columns_on_insert = conf_per_table.All_columns_on_insert_per_table[lkey]
		Parse_object_to_export(dbt.object_to_export, conf_per_table.All_object_to_export[lkey])
		dbt.partition_regex = conf_per_table.All_partition_regex_per_table[lkey]
		dbt.max_threads_per_table = MaxThreadsPerTable
		dbt.current_threads_running = 0
		var rows_p_chunk = conf_per_table.All_rows_per_table[lkey]
		if rows_p_chunk != "" {
			dbt.split_integer_tables = parse_rows_per_chunk(rows_p_chunk, &(dbt.min_chunk_step_size), &(dbt.starting_chunk_step_size), &(dbt.max_chunk_step_size))
		} else {
			dbt.split_integer_tables = split_integer_tables
			dbt.min_chunk_step_size = min_chunk_step_size
			dbt.starting_chunk_step_size = starting_chunk_step_size
			dbt.max_chunk_step_size = max_chunk_step_size
		}
		if dbt.min_chunk_step_size == 1 && dbt.min_chunk_step_size == dbt.starting_chunk_step_size && dbt.starting_chunk_step_size != dbt.max_chunk_step_size {
			dbt.min_chunk_step_size = 2
			dbt.starting_chunk_step_size = 2
			log.Warnf("Setting min and start rows per file to 2 on %s", lkey)
		}
		n, ok := conf_per_table.All_num_threads_per_table[lkey]
		if ok {
			dbt.num_threads = n
		} else {
			dbt.num_threads = NumThreads
		}
		dbt.estimated_remaining_steps = 1
		dbt.min = ""
		dbt.max = ""
		dbt.chunks = nil
		dbt.load_data_header = nil
		dbt.load_data_suffix = nil
		dbt.insert_statement = nil
		dbt.chunks_mutex = G_mutex_new()
		//  g_mutex_lock(dbt.chunks_mutex);
		dbt.chunks_queue = G_async_queue_new(BufferSize)
		dbt.chunks_completed = 0
		get_primary_key(conn, dbt, conf)
		dbt.primary_key_separated_by_comma = ""
		if OrderByPrimaryKey {
			get_primary_key_separated_by_comma(dbt)
		}
		dbt.multicolumn = len(dbt.primary_key) > 1

		//  dbt.primary_key = get_primary_key_string(conn, dbt.database.name, dbt.table);
		dbt.chunk_filesize = ChunkFilesize
		//  create_job_to_determine_chunk_type(dbt, G_async_queue_push, );

		dbt.complete_insert = CompleteInsert || detect_generated_fields(conn, dbt.database.escaped, dbt.escaped_table)
		if dbt.complete_insert {
			dbt.select_fields = get_insertable_fields(conn, dbt.database.escaped, dbt.escaped_table)
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

func free_db_table(dbt *DB_Table) {
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

func new_table_to_dump(conn *DBConnection, conf *configuration, is_view bool, is_sequence bool, database *database, table string, collation string, ecol string) {
	database.ad_mutex.Lock()
	if !database.already_dumped {
		create_job_to_dump_schema(database, conf)
		database.already_dumped = true
	}
	database.ad_mutex.Unlock()

	var dbt *DB_Table
	var b = new_db_table(&dbt, conn, conf, database, table, collation, is_sequence)
	if b {
		if (!is_view || ViewsAsTables) && !is_sequence {
			// with trx_consistency_only we dump all as innodb_table
			if !NoSchemas && !dbt.object_to_export.No_schema {
				//      write_table_metadata_into_file(dbt);
				table_schemas_mutex.Lock()
				table_schemas = append(table_schemas, dbt)
				table_schemas_mutex.Unlock()
				create_job_to_dump_table_schema(dbt, conf)
			}
			if DumpTriggers && !database.dump_triggers && !dbt.object_to_export.No_trigger {
				create_job_to_dump_triggers(conn, dbt, conf)
			}
			if !NoData && !dbt.object_to_export.No_data {
				if ecol != "" && strings.ToUpper(ecol) != "MRG_MYISAM" {
					if DataChecksums && !(Get_major() == 5 && Get_secondary() == 7 && dbt.has_json_fields) {
						create_job_to_dump_checksum(dbt, conf)
					}
					if TrxConsistencyOnly || (ecol != "" && (ecol == "InnoDB" || ecol == "TokuDB")) {
						dbt.is_innodb = true
						innodb_table.mutex.Lock()
						innodb_table.list.PushFront(dbt)
						innodb_table.mutex.Unlock()

					} else {
						dbt.is_innodb = false
						non_innodb_table.mutex.Lock()
						non_innodb_table.list.PushFront(dbt)
						non_innodb_table.mutex.Unlock()
					}
				} else {
					if is_view {
						dbt.is_innodb = false
						non_innodb_table.mutex.Lock()
						non_innodb_table.list.PushFront(dbt)
						non_innodb_table.mutex.Unlock()
					}
				}
			}
		} else if is_view {
			if !NoSchemas && !dbt.object_to_export.No_schema {
				create_job_to_dump_view(dbt, conf)
			}
		} else { // is_sequence
			if !NoSchemas && !dbt.object_to_export.No_schema {
				create_job_to_dump_sequence(dbt, conf)
			}
		}
	}
	// if a view or sequence we care only about schema
}

func determine_if_schema_is_elected_to_dump_post(conn *DBConnection, database *database) bool {
	var query string
	var result *mysql.Result
	if DumpRoutines {
		G_assert(nroutines > 0)
		var r uint
		for r = 0; r < nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", routine_type[r], database.escaped)
			result = conn.Execute(query)
			if conn.Err != nil {
				log.Criticalf("Error showing procedure on: %s - Could not execute query: %v", database.name, conn.Err)
				errors++
				return false
			}
			for _, row := range result.Values {
				if TablesSkiplistFile != "" && Check_skiplist(database.name, string(row[1].AsString())) {
					continue
				}
				if !Eval_regex(database.name, string(row[1].AsString())) {
					continue
				}
				return true
			}
		}

		if DumpEvents {
			query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", Identifier_quote_character_str, database.name, Identifier_quote_character_str)
			result = conn.Execute(query)
			if conn.Err != nil {
				log.Criticalf("Error showing events on: %s - Could not execute query: %v", database.name, conn.Err)
				errors++
				return false
			}
			for _, row := range result.Values {
				if TablesSkiplistFile != "" && Check_skiplist(database.name, string(row[1].AsString())) {
					continue
				}
				if !Eval_regex(database.name, string(row[1].AsString())) {
					continue
				}
				return true
			}
		}

	}
	return false
}

func dump_database_thread(conn *DBConnection, conf *configuration, database *database) {
	var query = fmt.Sprintf("SHOW TABLE STATUS")
	var result *mysql.Result
	if !conn.UseDB(database.name) {
		log.Criticalf("Could not select database: %s (%v)", database.name, conn.Err)
		errors++
		return
	}
	result = conn.Execute(query)
	if conn.Err != nil {
		log.Criticalf("Error showing table on: %s - Could not execute query: %v", database.name, conn.Err)
		errors++
		return
	}

	var ecol int = -1
	var ccol int = -1
	var collcol int = -1
	var rowscol int = 0
	determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
	if len(result.Values) == 0 {
		log.Criticalf("Could not list tables for %s", database.name)
		errors++
		return
	}

	var row []mysql.FieldValue
	for _, row = range result.Values {
		var dump = true
		var is_view = false
		var is_sequence = false
		if (Is_mysql_like() || Detected_server == SERVER_TYPE_TIDB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
			is_view = true
		}
		if Detected_server == SERVER_TYPE_MARIADB && string(row[ccol].AsString()) == "SEQUENCE" {
			is_sequence = true
		}
		if !is_view && row[ecol].Value() == nil {
			log.Warnf("Broken table detected, please review: %s.%s", database.name, row[0].AsString())
			if ExitIfBrokenTableFound {
				os.Exit(EXIT_FAILURE)
			}
			dump = false
		}
		if dump && len(ignore) > 0 && !is_view && !is_sequence {
			for _, ignore := range ignore {
				if strings.ToLower(ignore) == strings.ToLower(string(row[ecol].AsString())) {
					dump = false
					break
				}
			}
		}
		if is_view && NoDumpViews {
			dump = false
		}

		if is_sequence && no_dump_sequences {
			dump = false
		}
		if !dump {
			continue
		}
		if len(Tables) > 0 && !Is_table_in_list(database.name, string(row[0].AsString()), Tables) {
			continue
		}
		/* Special tables */
		if Is_mysql_special_tables(database.name, string(row[0].AsString())) {
			dump = false
			continue
		}
		/* Checks skip list on 'database.table' string */
		if TablesSkiplistFile != "" && Check_skiplist(database.name, string(row[0].AsString())) {
			continue
		}

		/* Checks PCRE expressions on 'database.table' string */
		if !Eval_regex(database.name, string(row[0].AsString())) {
			continue
		}

		/* Check if the table was recently updated */
		if len(no_updated_tables) > 0 && !is_view && !is_sequence {
			for _, iter := range no_updated_tables {
				if strings.Compare(iter, fmt.Sprintf("%s.%s", database.name, row[0].AsString())) == 0 {
					log.Infof("NO UPDATED TABLE: %s.%s", database.name, row[0].AsString())
					dump = false
				}
			}
		}

		if !dump {
			continue
		}
		create_job_to_dump_table(conf, is_view, is_sequence, database, string(row[tablecol].AsString()), string(row[collcol].AsString()), string(row[ecol].AsString()))
	}
	if determine_if_schema_is_elected_to_dump_post(conn, database) {
		create_job_to_dump_post(database, conf)
	}
	if database.dump_triggers {
		create_job_to_dump_schema_triggers(database, conf)
	}
	if DumpTriggers && database.dump_triggers {
		create_job_to_dump_schema_triggers(database, conf)
	}
	return
}
