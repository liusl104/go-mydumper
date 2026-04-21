package mydumper

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

const (
	INSERT_IGNORE = "INSERT IGNORE"
	INSERT        = "INSERT"
	REPLACE       = "REPLACE"
	UNLOCK_TABLES = "UNLOCK TABLES"
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
	IgnoreEnginesStr                     string
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
	transactional_table                  *MList
	non_transactional_table              *MList
	view_schemas_mutex                   *sync.Mutex
	table_schemas_mutex                  *sync.Mutex
	all_dbts_mutex                       *sync.Mutex
	trigger_schemas_mutex                *sync.Mutex
	init_mutex                           *sync.Mutex
	binlog_snapshot_gtid_executed        string
	binlog_snapshot_gtid_executed_status bool
	binlog_snapshot_gtid_executed_count  uint
	ignore_engines                       []string
	less_locking_threads                 uint
	sync_wait                            int = -1
	tablecol                             uint
	isms                                 bool
	no_dump_sequences                    bool
)

type thread_data_buffers struct {
	statement *GString
	row       *GString
	escaped   *GString
	column    *GString
}

type thread_data struct {
	conf                          *Configuration
	thread_id                     uint
	table_name                    string
	thrconn                       *DBConnection
	binlog_snapshot_gtid_executed string
	pause_resume_mutex            *sync.Mutex
	thread_data_buffers           *thread_data_buffers
}
type process_fun func(td *thread_data, job *job) bool
type write_fun func(p []byte) (int, error)
type write_str_fun func(str string) (int, error)
type close_fun func() error
type flush_fun func() error

// initialize_working_thread sets up chunk step limits, character_set_hash, transactional/non_transactional lists, file handler, jobs, and chunks.
func initialize_working_thread() {
	database_counter = 0
	if max_chunk_step_size > math.MaxUint64/uint64(NumThreads) {
		max_chunk_step_size = math.MaxUint64 / uint64(NumThreads)
		log.Errorf("This should not happen")
	}
	character_set_hash = make(map[string]string)
	character_set_hash_mutex = G_mutex_new()
	transactional_table = NewMList()
	non_transactional_table = NewMList()
	// transactional_table.list = nil
	// non_transactional_table.list = nil
	non_transactional_table.mutex = G_mutex_new()
	transactional_table.mutex = G_mutex_new()

	view_schemas_mutex = G_mutex_new()
	table_schemas_mutex = G_mutex_new()
	trigger_schemas_mutex = G_mutex_new()
	init_mutex = G_mutex_new()
	binlog_snapshot_gtid_executed = ""

	if IgnoreEnginesStr != "" {
		ignore_engines = strings.Split(IgnoreEnginesStr, ",")
	}
	initialize_file_handler()
	initialize_jobs()
	initialize_chunk()
	if DumpChecksums {
		DataChecksums = true
		SchemaChecksums = true
		RoutineChecksums = true
	}
}

// start_working_thread creates NumThreads worker threads (working_thread) and waits for them to be ready (and optionally for GTID sync).
func start_working_thread(c any) {
	conf := c.(*Configuration)
	var n uint
	threads = make([]*GThread, NumThreads)
	thd = make([]*thread_data, NumThreads) // thread_data
	log.Infof("Creating workers")
	for n = 0; n < NumThreads; n++ {
		thd[n] = new(thread_data)
		thd[n].conf = conf
		thd[n].thread_id = n + 1
		thd[n].binlog_snapshot_gtid_executed = ""
		thd[n].pause_resume_mutex = nil
		thd[n].table_name = ""
		thd[n].thread_data_buffers = new(thread_data_buffers)
		thd[n].thread_data_buffers.statement = G_string_sized_new(2 * StatementSize)
		thd[n].thread_data_buffers.row = G_string_sized_new(StatementSize)
		thd[n].thread_data_buffers.column = G_string_sized_new(StatementSize)
		thd[n].thread_data_buffers.escaped = G_string_sized_new(StatementSize)
		threads[n] = M_thread_new("data", working_thread, thd[n], "Data thread could not be created")
	}
	if SyncThreadLockMode == GTID {
		var _binlog_snapshot_gtid_executed string = ""
		var binlog_snapshot_gtid_executed_status_local bool = false
		var start_transaction_retry uint = 0
		for !binlog_snapshot_gtid_executed_status_local && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
			binlog_snapshot_gtid_executed_status_local = true
			for n = 0; n < NumThreads; n++ {
				G_async_queue_pop(conf.gtid_pos_checked)
			}
			_binlog_snapshot_gtid_executed = thd[0].binlog_snapshot_gtid_executed
			for n = 1; n < NumThreads; n++ {
				binlog_snapshot_gtid_executed_status_local = binlog_snapshot_gtid_executed_status_local && strings.Compare(thd[n].binlog_snapshot_gtid_executed, _binlog_snapshot_gtid_executed) == 0
			}
			for n = 0; n < NumThreads; n++ {
				if binlog_snapshot_gtid_executed_status_local {
					G_async_queue_push(conf.are_all_threads_in_same_pos, 1)
				} else {
					G_async_queue_push(conf.are_all_threads_in_same_pos, 2)
				}
			}
			start_transaction_retry++
		}
	}
	for n = 0; n < NumThreads; n++ {
		G_async_queue_pop(conf.ready)
	}
}

// finalize_working_thread clears thread-related globals and calls finalize_table.
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
	thd = nil
	threads = nil
	finalize_table()
}

// wait_working_thread_to_finish joins all worker threads.
func wait_working_thread_to_finish() {
	var n uint
	log.Infof("Waiting threads to complete")
	for n = 0; n < NumThreads; n++ {
		G_thread_join(threads[n])
	}
}

// thd_JOB_DUMP_ALL_DATABASES lists databases via SHOW DATABASES, creates dump_schema and dump_database jobs for each (filtered), and signals db_ready when done.
func thd_JOB_DUMP_ALL_DATABASES(td *thread_data, job *job) {
	var databases *MYSQL_RES
	var row []FieldValue
	databases = M_store_result(td.thrconn, "SHOW DATABASES", M_critical, "Unable to list databases")
	for {
		row = Mysql_fetch_row(databases)
		if row == nil {
			break
		}
		if strings.EqualFold(string(row[0].AsString()), "information_schema") ||
			strings.EqualFold(string(row[0].AsString()), "performance_schema") ||
			strings.EqualFold(string(row[0].AsString()), "data_dictionary") ||
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
	Mysql_free_result(databases)
}

// thd_JOB_DUMP_DATABASE runs dump_database_thread for the job's database and signals db_ready when database_counter reaches zero.
func thd_JOB_DUMP_DATABASE(td *thread_data, job *job) {
	var ddj = job.job_data.(*dump_database_job)
	log.Infof("Thread %d: dumping db information for `%s`", td.thread_id, ddj.database.name)
	dump_database_thread(td.thrconn, td.conf, ddj.database)
	if G_atomic_int_dec_and_test(&database_counter) {
		G_async_queue_push(td.conf.db_ready, 1)
	}
}

// get_table_info_to_process_from_list iterates table_list (db.table), fetches SHOW TABLE STATUS, and creates db_table + dump jobs for each table.
func get_table_info_to_process_from_list(conn *DBConnection, conf *Configuration, table_list []string) {
	var query string
	var x int
	var dt []string
	for x = 0; x < len(table_list); x++ {
		dt = strings.Split(table_list[x], ".")
		query = fmt.Sprintf("SHOW TABLE STATUS FROM %s%s%s LIKE '%s'", Identifier_quote_character_str, dt[0],
			Identifier_quote_character_str, dt[1])
		var result = M_store_result(conn, query, M_critical, "Error showing table status on: %s - Could not execute query", dt[0])
		if result == nil {
			return
		}
		var ecol uint = 0
		var ccol uint = 0
		var collcol uint = 0
		var rowscol uint = 0
		determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol)
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
		var row []FieldValue
		for {
			row = Mysql_fetch_row(result)
			if row == nil {
				break
			}
			var is_view, is_sequence bool
			if (Get_product() == SERVER_TYPE_MYSQL ||
				Get_product() == SERVER_TYPE_MARIADB ||
				Get_product() == SERVER_TYPE_DOLT) &&
				(row[ecol].Value() == nil) &&
				(row[ccol].Value() == nil || strings.EqualFold(string(row[ccol].AsString()), "VIEW")) {
				is_view = true
			}
			if (Detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || strings.EqualFold(string(row[ccol].AsString()), "SEQUENCE")) {
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
		Mysql_free_result(result)

	}
	if G_atomic_int_dec_and_test(&database_counter) {
		G_async_queue_push(conf.db_ready, 1)
	}
}

// thd_JOB_DUMP_TABLE_LIST delegates to get_table_info_to_process_from_list for the job's table list.
func thd_JOB_DUMP_TABLE_LIST(td *thread_data, job *job) {
	var dtlj = job.job_data.(*dump_table_list_job)
	get_table_info_to_process_from_list(td.thrconn, td.conf, dtlj.table_list)
}

// new_partition_step creates a chunk_step with a single partition_step for the given partition name.
func new_partition_step(partition string) *chunk_step {
	_ = partition
	var cs = new(chunk_step)
	cs.partition_step = new(partition_step)
	cs.char_step = new(char_step)
	cs.integer_step = new(integer_step)
	return cs
}

// m_async_queue_push_conservative pushes the job to the queue only if the table's current_threads_running is below max_threads_per_table.
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

// thd_JOB_DUMP processes a table dump job: runs the chunk's process function (e.g. write_table_job_into_file) and decrements current_threads_running.
func thd_JOB_DUMP(td *thread_data, job *job) {
	var tj = job.job_data.(*table_job)
	if UseSavepoints {
		if td.table_name != "" {
			if tj.dbt.table != td.table_name {
				M_query_critical(td.thrconn, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", MYDUMPER), "Rollback to savepoint failed")
				M_query_critical(td.thrconn, fmt.Sprintf("SAVEPOINT %s", MYDUMPER), "Savepoint failed")
				td.table_name = tj.dbt.table
			}
		} else {
			M_query_critical(td.thrconn, fmt.Sprintf("SAVEPOINT %s", MYDUMPER), "Savepoint failed")
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

// initialize_thread connects the thread to the DB, applies Set_session, and sets tidb_snapshot if needed.
func initialize_thread(td *thread_data) {

	M_connect(td.thrconn)
	log.Infof("Thread %d: connected using MySQL connection ID %d", td.thread_id, td.thrconn.GetConnectionID())

}

// initialize_consistent_snapshot starts a transaction (and optionally sets GTID snapshot) so the thread sees a consistent snapshot.
func initialize_consistent_snapshot(td *thread_data) {
	// var err error

	if sync_wait != -1 {
		M_query_critical(td.thrconn, fmt.Sprintf("SET SESSION WSREP_SYNC_WAIT = %d", sync_wait), "Failed to set wsrep_sync_wait for the thread")
		if td.thrconn.Err != nil {
			log.Criticalf("Failed to set wsrep_sync_wait for the thread: %v", td.thrconn.Err)
		}
	}
	set_transaction_isolation_level_repeatable_read(td.thrconn)
	var start_transaction_retry uint
	var cont bool
	if SyncThreadLockMode == GTID {
		for !cont && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
			log.Debugf("Thread %d: Start transaction # %d", td.thread_id, start_transaction_retry)
			M_query_critical(td.thrconn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", "Failed to start consistent snapshot")
			var res *MYSQL_RES
			res = M_store_result_critical(td.thrconn, "SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'", "Failed to get binlog_snapshot_gtid_executed")
			if res != nil {
				var row []FieldValue = Mysql_fetch_row(res)
				if row != nil {
					td.binlog_snapshot_gtid_executed = string(row[1].AsString())
				} else {
					M_critical("Failed to get content of binlog_snapshot_gtid_executed")
				}
				Mysql_free_result(res)
			} else {
				M_critical("Failed to get content of binlog_snapshot_gtid_executed: %s", Mysql_error(td.thrconn))
			}

			start_transaction_retry++
			G_async_queue_push(td.conf.gtid_pos_checked, 1)
			cont = G_async_queue_pop(td.conf.are_all_threads_in_same_pos).(int) == 1
		}
		if cont {
			log.Infof("All threads in the same position. This will be a consistent backup.")
			it_is_a_consistent_backup = true
		} else {
			M_critical("We were not able to sync all threads. We unsuccessfully tried %d times. Reducing the amount of threads might help.", MAX_START_TRANSACTION_RETRIES)
		}
	} else {
		M_query_critical(td.thrconn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", "Failed to start consistent snapshot")
	}
}

// check_connection_status pings the connection and reconnects + reapplies Set_session if needed.
func check_connection_status(td *thread_data) {
	if Get_product() == SERVER_TYPE_TIDB {
		set_tidb_snapshot(td.thrconn)
		log.Infof("Thread %d: set to tidb_snapshot '%s'", td.thread_id, TidbSnapshot)
	}
	if need_dummy_read {
		var res = M_store_result(td.thrconn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy", M_warning, "Failed to select on mysql.mydumperdummy")
		if res != nil {
			Mysql_free_result(res)
		}
	}
	if need_dummy_toku_read {
		var res = M_store_result(td.thrconn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy", M_warning, "Failed to select on mysql.tokudbdummy")
		if res != nil {
			Mysql_free_result(res)
		}
	}

}

// get_binlog_position retrieves current binlog file, position, and optionally GTID from the server and assigns to the given pointers.
func get_binlog_position(conn *DBConnection, masterlog *string, masterpos *string, mastergtid *string) {
	var mr *M_ROW = M_store_result_row(conn, Show_binary_log_status, M_warning, M_message, "Couldn't get master position")
	if mr.Row != nil {
		*masterlog = string(mr.Row[0].AsString())
		*masterpos = string(mr.Row[1].AsString())
		// Oracle/Percona GTID
		if Mysql_num_fields(mr.Res) == 5 {
			*mastergtid = Remove_new_line(string(mr.Row[4].AsString()))
		} else {
			// Let's try with MariaDB 10.x
			// Use gtid_binlog_pos due to issue with gtid_current_pos with galera
			// cluster, gtid_binlog_pos works as well with normal mariadb server
			// https://jira.mariadb.org/browse/MDEV-10279
			M_store_result_row_free(mr)
			mr = M_store_result_row(conn, "SELECT @@gtid_binlog_pos", nil, nil, "Failed to get @@gtid_binlog_pos")
			if mr.Row != nil {
				*mastergtid = Remove_new_line(string(mr.Row[0].AsString()))
			}
		}
	}
	M_store_result_row_free(mr)
}

// write_snapshot_info writes metadata (binlog position, GTID, snapshot timestamp) to the metadata file.
func write_snapshot_info(conn *DBConnection, file *os.File) {
	get_binlog_position(conn, &initial_source_log, &initial_source_pos, &initial_source_gtid)
	if initial_source_log != "" {
		fmt.Fprintf(file, "[source]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\n")
		if SourceData.Enabled {
			fmt.Fprintf(file, "#SOURCE_HOST = \"%s\"\n#SOURCE_PORT = \n#SOURCE_USER = \"\"\n#SOURCE_PASSWORD = \"\"\n", Hostname)
			if SourceData.Source_ssl {
				fmt.Fprintf(file, "SOURCE_SSL = 1\n")
			} else {
				fmt.Fprintf(file, "#SOURCE_SSL = {0|1}\n")
			}
			if initial_source_gtid != "" {
				fmt.Fprintf(file, "executed_gtid_set = \"%s\"\n", initial_source_gtid)
			}
			if SourceData.Auto_position {
				fmt.Fprintf(file, "#SOURCE_LOG_FILE = \"%s\"\n#SOURCE_LOG_POS = %s\n", initial_source_log, initial_source_pos)
				fmt.Fprintf(file, "SOURCE_AUTO_POSITION = 1\n")
			} else {
				fmt.Fprintf(file, "SOURCE_LOG_FILE = \"%s\"\nSOURCE_LOG_POS = %s\n", initial_source_log, initial_source_pos)
				fmt.Fprintf(file, "#SOURCE_AUTO_POSITION = {0|1}\n")
			}
			if SourceData.Exec_reset_replica {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 1)
			} else {
				fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 0)
			}

			if SourceData.Exec_change_source {
				fmt.Fprintf(file, "nmyloader_exec_change_source = %d\n", 1)
			} else {
				fmt.Fprintf(file, "nmyloader_exec_change_source = %d\n", 0)
			}

			if SourceData.Exec_start_replica {
				fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 1)
			} else {
				fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 0)
			}

		}
		log.Infof("Written master status")
	}
	file.Sync()

}

// write_replica_info writes replica/source status (binlog, GTID) to the metadata file.
func write_replica_info(conn *DBConnection, file *os.File) {
	var slave *MYSQL_RES
	var fields []*sql.ColumnType
	var row []FieldValue
	var slavehost string
	var slavelog string
	var slavepos string
	var slavegtid string
	var channel_name string
	var gtid_title string
	var i uint
	var slave_count uint
	if isms {
		M_query_critical(conn, Show_all_replicas_status, fmt.Sprintf("Error executing %s", Show_all_replicas_status))
	} else {
		M_query_critical(conn, Show_replica_status, fmt.Sprintf("Error executing %s", Show_replica_status))
	}
	slave = Mysql_store_result(conn)
	var replication_section_str = G_string_sized_new(100)
	if slave != nil {
		for {
			row = Mysql_fetch_row(slave)
			if row == nil {
				break
			}
			G_string_set_size(replication_section_str, 0)
			fields = Mysql_fetch_fields(slave)

		}
	}
	for i = 0; i < Mysql_num_fields(slave); i++ {
		if strings.EqualFold(fields[i].Name(), "exec_master_log_pos") || strings.EqualFold(fields[i].Name(), "exec_source_log_pos") {
			slavepos = string(row[i].AsString())
		} else if strings.EqualFold(fields[i].Name(), "relay_master_log_file") || strings.EqualFold(fields[i].Name(), "relay_source_log_file") {
			slavelog = string(row[i].AsString())
		} else if strings.EqualFold(fields[i].Name(), "master_host") || strings.EqualFold(fields[i].Name(), "source_host") {
			slavehost = string(row[i].AsString())
		} else if strings.EqualFold(fields[i].Name(), "Executed_Gtid_Set") {
			gtid_title = "Executed_Gtid_Set"
			slavegtid = Remove_new_line(string(row[i].AsString()))
		} else if strings.EqualFold(fields[i].Name(), "Gtid_Slave_Pos") || strings.EqualFold(fields[i].Name(), "Gtid_source_Pos") {
			gtid_title = fields[i].Name()
			slavegtid = Remove_new_line(string(row[i].AsString()))
		} else if (strings.EqualFold(fields[i].Name(), "connection_name") || strings.EqualFold(fields[i].Name(), "Channel_Name")) && len(row[i].AsString()) > 1 {
			channel_name = string(row[i].AsString())
		}
		G_string_append_printf(replication_section_str, "# %s = ", fields[i].Name())
		if GetStandardType(fields[i].DatabaseTypeName()) != "MYSQL_TYPE_LONG" && GetStandardType(fields[i].DatabaseTypeName()) != "MYSQL_TYPE_LONGLONG" && GetStandardType(fields[i].DatabaseTypeName()) != "MYSQL_TYPE_INT24" && GetStandardType(fields[i].DatabaseTypeName()) != "MYSQL_TYPE_SHORT" {
			G_string_append_printf(replication_section_str, "'%s'\n", Remove_new_line(string(row[i].AsString())))
		} else {
			G_string_append_printf(replication_section_str, "%s\n", Remove_new_line(string(row[i].AsString())))
		}

	}
	if slavehost != "" {
		slave_count++
		if channel_name != "" {
			fmt.Fprintf(file, "[replication%s%s]", ".", channel_name)
		} else {
			fmt.Fprintf(file, "[replication%s%s]", "", "")
		}
		if slavegtid != "" && len(slavegtid) > 0 {
			fmt.Fprintf(file, "%s = \"%s\"\n", gtid_title, slavegtid)
		}

		if ReplicaData.Auto_position {
			fmt.Fprintf(file, "#SOURCE_LOG_FILE = \"%s\"\n#SOURCE_LOG_POS = %s\n", slavelog, slavepos)
			fmt.Fprintf(file, "SOURCE_AUTO_POSITION = 1\n")
		} else {
			fmt.Fprintf(file, "SOURCE_LOG_FILE = \"%s\"\nSOURCE_LOG_POS = %s\n", slavelog, slavepos)
			fmt.Fprintf(file, "#SOURCE_AUTO_POSITION = {0|1}\n")
		}
		fmt.Fprintf(file, "%s", replication_section_str.Str.String())
		if ReplicaData.Source_ssl {
			fmt.Fprintf(file, "SOURCE_SSL = 1\n")
		} else {
			fmt.Fprintf(file, "#SOURCE_SSL = {0|1}\n")
		}
		if ReplicaData.Exec_reset_replica {
			fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 1)
		} else {
			fmt.Fprintf(file, "myloader_exec_reset_replica = %d\n", 0)
		}
		if ReplicaData.Exec_change_source {
			fmt.Fprintf(file, "myloader_exec_change_source = %d\n", 1)
		} else {
			fmt.Fprintf(file, "myloader_exec_change_source = %d\n", 0)
		}
		if ReplicaData.Exec_start_replica {
			fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 1)
		} else {
			fmt.Fprintf(file, "myloader_exec_start_replica = %d\n", 0)
		}
		log.Infof("Written slave status")
	}
	if slave_count > 1 {
		log.Warnf("Multisource replication found. Do not trust in the exec_master_log_pos as it might cause data inconsistencies. Search 'Replication and Transaction Inconsistencies' on MySQL Documentation")
	}
	file.Sync()
	if slave != nil {
		Mysql_free_result(slave)
	}
}

// process_job_builder_job handles JOB_DETERMINE_CHUNK_TYPE: sets chunk strategy for dbt and enqueues chunk dump jobs; returns true if a job was processed.
func process_job_builder_job(td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DUMP_TABLE_LIST:
		thd_JOB_DUMP_TABLE_LIST(td, job)
		break
	case JOB_DUMP_DATABASE:
		thd_JOB_DUMP_DATABASE(td, job)
		break
	case JOB_DUMP_ALL_DATABASES:
		thd_JOB_DUMP_ALL_DATABASES(td, job)
		break
	case JOB_TABLE:
		thd_JOB_TABLE(td, job)
		break
	case JOB_WRITE_SOURCE_AND_REPLICA_STATUS:
		//      if (source_data.enabled)
		write_snapshot_info(td.thrconn, job.job_data.(*os.File))
		// Write replica information
		if (Get_product() != SERVER_TYPE_TIDB) && ReplicaData.Enabled {
			write_replica_info(td.thrconn, job.job_data.(*os.File))
		}
		G_async_queue_push(td.conf.source_and_replica_status_queue, 1)

		break
	case JOB_SHUTDOWN:
		return false
	default:
		log.Error("Something very bad happened!")
	}
	return true
}

// process_job dispatches the job to the appropriate handler (JOB_DUMP, JOB_TABLE, JOB_DUMP_DATABASE, etc.); returns true if handled.
func process_job(td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DETERMINE_CHUNK_TYPE:
		set_chunk_strategy_for_dbt(td.thrconn, job.job_data.(*db_table))
		break
	case JOB_DUMP:
		thd_JOB_DUMP(td, job)
		break
	case JOB_DUMP_NON_INNODB:
		thd_JOB_DUMP(td, job)
		break
	case JOB_DEFER:
		break
	case JOB_CHECKSUM:
		do_JOB_CHECKSUM(td, job)
		break
	case JOB_CREATE_DATABASE:
		do_JOB_CREATE_DATABASE(td, job)
		break
	case JOB_CREATE_TABLESPACE:
		do_JOB_CREATE_TABLESPACE(td, job)
		break
	case JOB_SCHEMA:
		do_JOB_SCHEMA(td, job)
		break
	case JOB_VIEW:
		do_JOB_VIEW(td, job)
		break
	case JOB_SEQUENCE:
		do_JOB_SEQUENCE(td, job)
		break
	case JOB_TRIGGERS:
		do_JOB_TRIGGERS(td, job)
		break
	case JOB_SCHEMA_TRIGGERS:
		do_JOB_SCHEMA_TRIGGERS(td, job)
		break
	case JOB_SCHEMA_POST:
		do_JOB_SCHEMA_POST(td, job)
		break
	case JOB_SHUTDOWN:

		return false

	default:
		log.Errorf("Something very bad happened! %v", job.types)
	}
	return true
}

// check_pause_resume blocks until the thread's pause_resume_mutex is unlocked (used for disk space or user SIGINT pause).
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

// process_queue pops jobs from queue, optionally processes builder jobs first, and runs process_job until JOB_SHUTDOWN.
func process_queue(queue *GAsyncQueue, td *thread_data, do_builder bool, chunk_step_queue *GAsyncQueue) {
	var j *job
	for {
		check_pause_resume(td)
		if chunk_step_queue != nil {
			G_async_queue_push(chunk_step_queue, 1)
		}
		val := G_async_queue_pop(queue)
		if val == nil {
			return
		}
		j = val.(*job)
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

// build_lock_tables_statement builds the LOCK TABLE ... READ statement from conf.lock_tables_statement and table list.
func build_lock_tables_statement(conf *Configuration) {
	non_transactional_table.mutex.Lock()
	var dbt *db_table
	iter := non_transactional_table.list.Front()

	if iter.Value != nil {
		dbt = iter.Value.(*db_table)
		conf.lock_tables_statement = G_string_sized_new(30)
		G_string_printf(conf.lock_tables_statement, "LOCK TABLES %s%s%s.%s%s%s READ LOCAL", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
			Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
		iter = iter.Next()
		for ; iter != nil; iter = iter.Next() {
			dbt = iter.Value.(*db_table)
			G_string_append_printf(conf.lock_tables_statement, ", %s%s%s.%s%s%s READ LOCAL", Identifier_quote_character_str, dbt.database.name, Identifier_quote_character_str,
				Identifier_quote_character_str, dbt.table, Identifier_quote_character_str)
		}
	}
	non_transactional_table.mutex.Unlock()
}

// update_estimated_remaining_chunks_on_dbt sets dbt.estimated_remaining_steps from the chunk step item (integer/char/partition).
func update_estimated_remaining_chunks_on_dbt(dbt *db_table) {
	var l = dbt.chunks.Front()
	var total uint64

	for l != nil {
		switch l.Value.(*chunk_step_item).chunk_type {
		case INTEGER:
			total += l.Value.(*chunk_step_item).chunk_step.integer_step.estimated_remaining_steps
		case CHAR:
			total += l.Value.(*chunk_step_item).chunk_step.char_step.estimated_remaining_steps
		default:
			total++
		}
		l = l.Next()
	}
	dbt.estimated_remaining_steps = total
}

// working_thread is the main loop for a worker: initialize_thread, initialize_consistent_snapshot, then process_queue for data and optionally chunk_step queues.
func working_thread(c any) {
	td := c.(*thread_data)
	init_mutex.Lock()
	td.thrconn = Mysql_init()
	init_mutex.Unlock()
	initialize_thread(td)
	Execute_gstring(td.thrconn, Set_session)
	// Initialize connection
	if !SkipTz {
		M_query_critical(td.thrconn, "/*!40103 SET TIME_ZONE='+00:00' */", "Failed to set time zone")
	}
	if UseSavepoints {
		M_query_critical(td.thrconn, "SET SQL_LOG_BIN = 0", "Failed to disable binlog for the thread")
	}

	initialize_consistent_snapshot(td)
	check_connection_status(td)

	G_async_queue_push(td.conf.ready, 1)
	// Thread Ready to process jobs
	log.Infof("Thread %d: Creating Jobs", td.thread_id)
	process_queue(td.conf.initial_queue, td, true, nil)
	G_async_queue_push(td.conf.initial_completed_queue, 1)
	log.Infof("Thread %d: Schema queue", td.thread_id)
	process_queue(td.conf.schema_queue, td, false, nil)

	if Stream != "" {
		send_initial_metadata()
	}
	if !NoData {
		log.Infof("Thread %d: Schema jobs are done, Starting exporting data for Non-Transactional tables", td.thread_id)

		G_async_queue_push(td.conf.ready, 1)
		G_async_queue_pop(td.conf.ready_non_transactional_queue)
		if TrxTables != 0 {
			// Processing non-transactional tables
			// This queue should be empty, but we are processing just in case.
			process_queue(td.conf.non_transactional.queue, td, false, td.conf.non_transactional.request_chunk)
			process_queue(td.conf.non_transactional.deferQueue, td, false, nil)
			// This push will unlock the FTWRL on the Main Connection
			G_async_queue_push(td.conf.unlock_tables, 1)
		} else {
			// Sending LOCK TABLE over all non-transactional tables
			if td.conf.lock_tables_statement != nil {
				log.Infof("Thread %d: Locking non-transactional tables", td.thread_id)
				M_query_critical(td.thrconn, td.conf.lock_tables_statement.Str.String(), "Error locking non-transactional tables")
			}
			// This push will unlock the FTWRL on the Main Connection
			G_async_queue_push(td.conf.unlock_tables, 1)

			// Processing non-transactional tables
			process_queue(td.conf.non_transactional.queue, td, false, td.conf.non_transactional.request_chunk)
			process_queue(td.conf.non_transactional.deferQueue, td, false, nil)

			// At this point, this thread is able to unlock the non-transactional tables
			M_query_critical(td.thrconn, UNLOCK_TABLES, "Error locking non-transactional tables")
		}
		// Processing Transactional tables
		log.Infof("Thread %d: Non-Transactional tables are done, Starting exporting data for Transactional tables", td.thread_id)
		process_queue(td.conf.transactional.queue, td, false, td.conf.transactional.request_chunk)
		process_queue(td.conf.transactional.deferQueue, td, false, nil)
		//  start_processing(td, resume_mutex);
	} else {
		G_async_queue_push(td.conf.unlock_tables, 1)
	}
	if UseSavepoints && td.table_name != "" {
		M_query_critical(td.thrconn, "ROLLBACK TO SAVEPOINT mydumper", "Rollback to savepoint failed")
	}
	log.Infof("Thread %d: Processing remaining objects jobs", td.thread_id)
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

// new_table_to_dump creates a db_table for the given database/table, fetches SHOW TABLE STATUS, and enqueues schema/dump/trigger/view/checksum jobs as needed.
func new_table_to_dump(conn *DBConnection, conf *Configuration, is_view bool, is_sequence bool, database *database, table string, collation string, ecol string) {
	database.ad_mutex.Lock()
	if !database.already_dumped {
		create_job_to_dump_schema(database, conf)
		database.already_dumped = true
	}
	database.ad_mutex.Unlock()

	var dbt *db_table
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
				if ecol != "" && !strings.EqualFold(ecol, "MRG_MYISAM") {
					if DataChecksums && !(Get_major() == 5 && Get_secondary() == 7 && dbt.has_json_fields) {
						create_job_to_dump_checksum(dbt, conf)
					}
					if TrxTables != 0 || (ecol != "" && (strings.EqualFold(ecol, "InnoDB") || strings.EqualFold(ecol, "TokuDB"))) {
						dbt.is_transactional = true
						transactional_table.mutex.Lock()
						transactional_table.list.PushBack(dbt)
						transactional_table.mutex.Unlock()
					} else {
						dbt.is_transactional = false
						non_transactional_table.mutex.Lock()
						non_transactional_table.list.PushBack(dbt)
						non_transactional_table.mutex.Unlock()
					}
				} else {
					if is_view {
						dbt.is_transactional = false
						non_transactional_table.mutex.Lock()
						non_transactional_table.list.PushBack(dbt)
						non_transactional_table.mutex.Unlock()
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

// determine_if_schema_is_elected_to_dump_post returns true if the database has any tables/views/routines/events that were selected for dump (for post-schema jobs).
func determine_if_schema_is_elected_to_dump_post(conn *DBConnection, database *database) bool {
	var query string
	var result *MYSQL_RES = Mysql_store_result(conn)
	var row []FieldValue
	if DumpRoutines {
		G_assert(nroutines > 0)
		var r uint
		for r = 0; r < nroutines; r++ {
			query = fmt.Sprintf("SHOW %s STATUS WHERE CAST(Db AS BINARY) = '%s'", routine_type[r], database.escaped)
			result = M_store_result(conn, query, M_critical, "Error showing procedure on: %s - Could not execute query", database.name)
			if result == nil {
				return false
			}
			for {
				row = Mysql_fetch_row(result)
				if row == nil {
					break
				}
				if TablesSkiplistFile != "" && Check_skiplist(database.name, string(row[1].AsString())) {
					continue
				}
				if !Eval_regex(database.name, string(row[1].AsString())) {
					continue
				}
				Mysql_free_result(result)
				return true
			}
			Mysql_free_result(result)
		}
	}
	if DumpEvents {
		query = fmt.Sprintf("SHOW EVENTS FROM %s%s%s", Identifier_quote_character_str, database.name, Identifier_quote_character_str)
		result = M_store_result(conn, query, M_critical, "Error showing events on: %s - Could not execute query", database.name)
		if result == nil {
			return false
		}
		for {
			row = Mysql_fetch_row(result)
			if row == nil {
				break
			}
			if TablesSkiplistFile != "" && Check_skiplist(database.name, string(row[1].AsString())) {
				continue
			}
			if !Eval_regex(database.name, string(row[1].AsString())) {
				continue
			}
			Mysql_free_result(result)
			return true
		}
		Mysql_free_result(result)
	}

	return false
}

// dump_database_thread dumps one database: tables (with chunk jobs), views, sequences, triggers, routines, events, and post-schema jobs.
func dump_database_thread(conn *DBConnection, conf *Configuration, database *database) {
	if !conn.UseDB(database.name) {
		log.Criticalf("Could not select database: %s (%v)", database.name, conn.Err)
		Errors++
		return
	}
	var query string = "SHOW TABLE STATUS"
	var result = M_store_result(conn, query, M_critical, "Error showing tables on: %s - Could not execute query", database.name)
	if result == nil {
		return
	}

	var ecol uint = 0
	var ccol uint = 0
	var collcol uint = 0
	var rowscol uint = 0
	var i = 0
	determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol)
	var row []FieldValue
	for {
		row = Mysql_fetch_row(result)
		if row == nil {
			break
		}
		var dump = true
		var is_view = false
		var is_sequence = false
		if (Is_mysql_like() || Detected_server == SERVER_TYPE_TIDB) && row[ecol].Value() == nil && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
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
		if dump && len(ignore_engines) > 0 && !is_view && !is_sequence {
			for i = 0; i < len(ignore_engines); i++ {
				if strings.EqualFold(ignore_engines[i], string(row[ecol].AsString())) {
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
	if DumpTriggers && database.dump_triggers {
		create_job_to_dump_schema_triggers(database, conf)
	}
	return
}

// thd_JOB_TABLE handles JOB_TABLE: creates db_table via new_db_table, sets chunk strategy, and enqueues schema/dump/trigger/view/checksum jobs.
func thd_JOB_TABLE(td *thread_data, job *job) {
	var dtj *dump_table_job = job.job_data.(*dump_table_job)
	new_table_to_dump(td.thrconn, td.conf, dtj.is_view, dtj.is_sequence, dtj.database, dtj.table, dtj.collation, dtj.engine)
	dtj.collation = ""
	dtj.engine = ""
	dtj = nil
}

/*
func get_insertable_fields(conn *DBConnection, database string, table string) string {
	var field_list string
	var query string
	var res *Result
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and `extra` not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%'", database, table)
	res = conn.Executes(query)
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

// get_anonymized_function_for returns the per-column anonymization function pointers for the table from conf_per_table, or nil.
func get_anonymized_function_for(conn *DBConnection, database string, table string) []*Function_pointer {
	var k = fmt.Sprintf("`%s`.`%s`", database, table)
	ht, ok := conf_per_table.All_anonymized_function[k]
	var anonymized_function_list []*Function_pointer
	if ok {
		query := fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' ORDER BY ORDINAL_POSITION;", database, table)
		res := conn.Executes(query)
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
*/
