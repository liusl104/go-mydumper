package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type process_fun func(o *OptionEntries, td *thread_data, job *job) bool
type write_fun func(p []byte) (int, error)
type close_fun func() error
type flush_fun func() error

func parse_rows_per_chunk(o *OptionEntries) {
	var split = strings.Split(o.Chunks.RowsPerChunk, ":")
	var err error
	switch len(split) {
	case 0:
		log.Fatalf("This should not happend")
	case 1:
		o.global.rows_per_file, err = strconv.ParseUint(split[0], 10, 64)
		o.global.min_rows_per_file = o.global.rows_per_file
		o.global.max_rows_per_file = o.global.rows_per_file
	case 2:
		o.global.min_rows_per_file, err = strconv.ParseUint(split[0], 10, 64)
		o.global.rows_per_file, err = strconv.ParseUint(split[1], 10, 64)
		o.global.max_rows_per_file = o.global.rows_per_file
	default:
		o.global.min_rows_per_file, err = strconv.ParseUint(split[0], 10, 64)
		o.global.rows_per_file, err = strconv.ParseUint(split[1], 10, 64)
		o.global.max_rows_per_file, err = strconv.ParseUint(split[3], 10, 64)
	}
	if err != nil {
		log.Fatalf("--rows option value illegal")
	}
}

func m_fopen(o *OptionEntries, filename *string, t string) (*file_write, error) {
	_ = o
	var mode int
	if strings.ToLower(t) == "w" {
		mode = os.O_CREATE | os.O_WRONLY
	} else {
		mode = os.O_RDONLY
	}
	file, err := os.OpenFile(*filename, mode, 0660)
	if err != nil {
		return nil, err
	}
	var f = new(file_write)
	f.write = file.Write
	f.close = file.Close
	f.flush = file.Sync
	return f, err
}

func initialize_working_thread(o *OptionEntries) {
	o.global.database_counter = 0
	if o.Chunks.RowsPerChunk != "" {
		parse_rows_per_chunk(o)
	}
	if o.global.max_rows_per_file > math.MaxUint64/uint64(o.Common.NumThreads) {
		o.global.max_rows_per_file = math.MaxUint64 / uint64(o.Common.NumThreads)
		log.Errorf("This should not happen")
	}
	o.global.character_set_hash = make(map[string]string)
	o.global.character_set_hash_mutex = g_mutex_new()
	o.global.non_innodb_table_mutex = g_mutex_new()
	o.global.innodb_table_mutex = g_mutex_new()
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
	m_close = m_close_pipe
	m_write = write_file
	if !o.Extra.Compress && o.Exec.ExecPerThreadExtension == "" {
		m_open = m_fopen
		m_close = m_close_file
		m_write = write_file
		o.Exec.ExecPerThreadExtension = EMPTY_STRING
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
		m_open = m_open_pipe
		m_close = m_close_pipe
	}
	initialize_jobs(o)
	initialize_chunk(o)
	initialize_write(o)

	if o.Checksum.DumpChecksums {
		o.Checksum.DataChecksums = true
		o.Checksum.SchemaChecksums = true
		o.Checksum.RoutineChecksums = true
	}
}

func finalize_working_thread(o *OptionEntries) {
	o.global.character_set_hash = nil
	o.global.character_set_hash_mutex = nil
	o.global.innodb_table_mutex = nil
	o.global.non_innodb_table_mutex = nil
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

func free_table_job(tj *table_job) {
	tj.where = ""
	tj.order_by = ""
	tj.sql_filename = ""
}

func thd_JOB_DUMP_ALL_DATABASES(o *OptionEntries, td *thread_data, job *job) {
	var databases *mysql.Result
	var err error
	databases, err = td.thrconn.Execute("SHOW DATABASES")
	if err != nil {
		log.Fatalf("Unable to list databases: %v", err)
	}
	for _, row := range databases.Values {
		if fmt.Sprintf("%s", row[0].AsString()) == "information_schema" || fmt.Sprintf("%s", row[0].AsString()) == "performance_schema" || fmt.Sprintf("%s", row[0].AsString()) == "data_dictionary" {
			continue
		}
		var db bool
		var db_tmp *database
		db_tmp, db = get_database(o, td.thrconn, string(row[0].AsString()), db_tmp)
		if db && !o.Objects.NoSchemas && eval_regex(o, string(row[0].AsString()), "") {
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
	_ = job
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
		if o.global.detected_server == SERVER_TYPE_MARIADB {
			query = fmt.Sprintf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT, TABLE_COLLATION as COLLATION, AVG_ROW_LENGTH, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'", dt[0], dt[1])
		} else {
			query = fmt.Sprintf("SHOW TABLE STATUS FROM %s LIKE '%s'", dt[0], dt[1])
		}
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
		// var db *database
		var db *database
		var getDB bool
		db, getDB = get_database(o, conn, dt[0], db)
		if getDB {
			if !db.already_dumped {
				db.ad_mutex.Lock()
				if !db.already_dumped {
					create_job_to_dump_schema(o, db, conf)
					db.already_dumped = true
				}
				db.ad_mutex.Unlock()
			}
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
			if !eval_regex(o, db.name, string(row[0].AsString())) {
				continue
			}
			if rowscol > 0 {
				new_table_to_dump(o, conn, conf, is_view, is_sequence, db, string(row[0].AsString()), string(row[collcol].AsString()), string(row[6].AsString()), string(row[ecol].AsString()), row[rowscol].AsUint64())
			} else {
				new_table_to_dump(o, conn, conf, is_view, is_sequence, db, string(row[0].AsString()), string(row[collcol].AsString()), string(row[6].AsString()), string(row[ecol].AsString()), 0)
			}
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

func m_async_queue_push_conservative(query *asyncQueue, element *job) {

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
			} else {
				_, err := td.thrconn.Execute(fmt.Sprintf("SAVEPOINT %s", MYDUMPER))
				if err != nil {
					log.Fatalf("Savepoint failed: %v", err)
				}
				td.table_name = tj.dbt.table
			}
		}
	}
	tj.td = td
	switch tj.dbt.chunk_type {
	case INTEGER:
		process_integer_chunk(o, td, tj)
		tj.chunk_step.integer_step.status = COMPLETED
	case CHAR:
		process_char_chunk(o, td, tj)
	case PARTITION:
		process_partition_chunk(o, td, tj)
	case NONE:
		write_table_job_into_file(o, td.thrconn, tj)
	default:
		log.Errorf("dbt on UNDEFINED shouldn't happen. This must be a bug")
	}
	if tj.sql_file != nil {
		m_close(o, td.thread_id, tj.sql_file, tj.sql_filename, tj.filesize, tj.dbt)
	}
	if tj.dat_file != nil {
		m_close(o, td.thread_id, tj.dat_file, tj.dat_filename, tj.filesize, tj.dbt)
	}
	free_table_job(tj)
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
	err = set_transaction_isolation_level_repeatable_read(td.thrconn)
	var start_transaction_retry uint
	var cont bool
	for !cont && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
		log.Debugf("Thread %d: Start trasaction # %d", td.thread_id, start_transaction_retry)
		_, err = td.thrconn.Execute("START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")
		if err != nil {
			log.Fatalf("Failed to start consistent snapshot: %v", err)
		}
		var res *mysql.Result
		res, err = td.thrconn.Execute("SHOW STATUS LIKE 'binlog_snapshot_gtid_executed'")
		if err != nil {
			log.Warnf("Failed to get binlog_snapshot_gtid_executed: %v", err)
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

func write_snapshot_info(conn *client.Conn, file *os.File) {
	var master, slave, mdb *mysql.Result
	var masterlog, mastergtid, slavehost, slavelog, slavepos, slavegtid, channel_name, gtid_title string
	var masterpos uint64
	var i uint
	var isms bool
	var err error
	master, err = conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		log.Fatalf("show master status:%v", err)
	}
	for _, row := range master.Values {
		masterlog = string(row[0].AsString())
		masterpos = row[1].AsUint64()
		if master.ColumnNumber() == 5 {
			mastergtid = remove_new_line(string(row[4].AsString()))
		} else {
			mdb, err = conn.Execute("SELECT @@gtid_binlog_pos")
			for _, row = range mdb.Values {
				mastergtid = remove_new_line(string(row[0].AsString()))
			}
		}
	}
	if masterlog != "" {
		fmt.Fprintf(file, "[master]\n# Channel_Name = '' # It can be use to setup replication FOR CHANNEL\nFile = %s\nPosition = %d\nExecuted_Gtid_Set = %s\n\n", masterlog, masterpos, mastergtid)
		log.Infof("Written master status")
	}
	isms = false
	rest, err := conn.Execute("SELECT @@default_master_connection")
	if err == nil && len(rest.Values) > 0 {
		log.Infof("Multisource slave detected.")
		isms = true
	}
	if isms {
		slave, err = conn.Execute("SHOW ALL SLAVES STATUS")
	} else {
		slave, err = conn.Execute("SHOW SLAVES STATUS")
	}
	var slave_count uint
	var replication_section_str string
	if slave != nil {
		for _, row := range slave.Values {
			slavepos = ""
			slavelog = ""
			slavehost = ""
			slavegtid = ""
			channel_name = ""
			gtid_title = ""
			for i = 0; i < uint(slave.ColumnNumber()); i++ {
				if string(slave.Fields[i].Name) == "exec_master_log_pos" {
					slavepos = string(row[i].AsString())
				} else if string(slave.Fields[i].Name) == "relay_master_log_file" {
					slavelog = string(row[i].AsString())
				} else if string(slave.Fields[i].Name) == "master_host" {
					slavehost = string(row[i].AsString())
				} else if string(slave.Fields[i].Name) == "Executed_Gtid_Set" {
					gtid_title = "Executed_Gtid_Set"
					slavegtid = remove_new_line(string(row[i].AsString()))
				} else if string(slave.Fields[i].Name) == "Gtid_Slave_Pos" {
					gtid_title = "Gtid_Slave_Pos"
					slavegtid = remove_new_line(string(row[i].AsString()))
				} else if string(slave.Fields[i].Name) == "connection_name" || string(slave.Fields[i].Name) == "Channel_Name" && len(row[i].AsString()) > 1 {
					channel_name = string(row[i].AsString())
				}
				replication_section_str += fmt.Sprintf("# %s = ", slave.Fields[i].Name)
				if slave.Fields[i].Type != mysql.MYSQL_TYPE_LONG && slave.Fields[i].Type != mysql.MYSQL_TYPE_LONGLONG && slave.Fields[i].Type != mysql.MYSQL_TYPE_INT24 && slave.Fields[i].Type != mysql.MYSQL_TYPE_SHORT {
					replication_section_str += fmt.Sprintf("'%s'\n", remove_new_line(string(row[i].AsString())))
				} else {
					replication_section_str += fmt.Sprintf("%s\n", remove_new_line(string(row[i].AsString())))
				}
			}
			if slavehost != "" {
				slave_count++
				if channel_name != "" {
					fmt.Fprintf(file, "[replication%s%s]", ".", channel_name)
				} else {
					fmt.Fprintf(file, "[replication]")
				}
				fmt.Fprintf(file, "\n# relay_master_log_file = '%s'\n# exec_master_log_pos = %s\n# %s = %s\n", slavelog, slavepos, gtid_title, slavegtid)
				fmt.Fprintf(file, replication_section_str)
				fmt.Fprintf(file, "# myloader_exec_reset_slave = 0 # 1 means execute the command\n# myloader_exec_change_master = 0 # 1 means execute the command\n# myloader_exec_start_slave = 0 # 1 means execute the command\n")
				log.Infof("Written slave status")
			}
		}
	}

	if slave_count > 1 {
		log.Warnf("Multisource replication found. Do not trust in the exec_master_log_pos as it might cause data inconsistencies. Search 'Replication and Transaction Inconsistencies' on MySQL Documentation")
	}
	file.Sync()

}

func process_job_builder_job(o *OptionEntries, td *thread_data, job *job) bool {
	switch job.types {
	case JOB_DUMP_TABLE_LIST:
		thd_JOB_DUMP_TABLE_LIST(o, td, job)
	case JOB_DUMP_DATABASE:
		thd_JOB_DUMP_DATABASE(o, td, job)
	case JOB_DUMP_ALL_DATABASES:
		thd_JOB_DUMP_ALL_DATABASES(o, td, job)
	case JOB_WRITE_MASTER_STATUS:
		write_snapshot_info(td.thrconn, job.job_data.(*os.File))
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
		write_snapshot_info(td.thrconn, job.job_data.(*os.File))
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

func process_queue(o *OptionEntries, queue *asyncQueue, td *thread_data, p process_fun, f func(o *OptionEntries)) {
	var j *job
	for {
		check_pause_resume(td)
		if f != nil {
			f(o)
		}
		j = queue.pop().(*job)
		if o.global.shutdown_triggered && j.types == JOB_SHUTDOWN {
			log.Infof("Thread %d: Process has been cacelled", td.thread_id)
			return
		}
		if !p(o, td, j) {
			break
		}
	}
}

func build_lock_tables_statement(o *OptionEntries, conf *configuration) {
	o.global.non_innodb_table_mutex.Lock()
	var dbt *db_table
	var i int
	for i, dbt = range o.global.non_innodb_table {
		if i == 0 {
			conf.lock_tables_statement = fmt.Sprintf("LOCK TABLES `%s`.`%s` READ LOCAL", dbt.database.name, dbt.table)
			continue
		}
		conf.lock_tables_statement += fmt.Sprintf(", `%s`.`%s` READ LOCAL", dbt.database.name, dbt.table)
	}
	o.global.non_innodb_table_mutex.Unlock()
}

func update_estimated_remaining_chunks_on_dbt(dbt *db_table) {
	var total uint64
	for _, l := range dbt.chunks {
		switch dbt.chunk_type {
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

func update_where_on_table_job(o *OptionEntries, td *thread_data, tj *table_job) {
	update_estimated_remaining_chunks_on_dbt(tj.dbt)
	tj.where = ""
	switch tj.dbt.chunk_type {
	case INTEGER:
		if tj.chunk_step.integer_step.is_unsigned {
			if tj.chunk_step.integer_step.types.unsign.min == tj.chunk_step.integer_step.types.unsign.max {
				log.Warnf("Thread %d: This shouldn't happen 1", td.thread_id)
				tj.where = fmt.Sprintf("(%s ( `%s` = %d))", tj.chunk_step.integer_step.prefix, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.types.unsign.cursor)
			} else {
				tj.where = fmt.Sprintf("( %s ( %d <= `%s` AND `%s` <= %d))", tj.chunk_step.integer_step.prefix, tj.chunk_step.integer_step.types.unsign.min, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.types.unsign.cursor)
			}
		} else {
			if tj.chunk_step.integer_step.types.sign.min == tj.chunk_step.integer_step.types.sign.max {
				log.Warnf("Thread %d: This shouldn't happen 2", td.thread_id)
				tj.where = fmt.Sprintf("(%s ( `%s` = %d))", tj.chunk_step.integer_step.prefix, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.types.sign.cursor)
			} else {
				tj.where = fmt.Sprintf("( %s ( %d <= `%s` AND `%s` <= %d))", tj.chunk_step.integer_step.prefix, tj.chunk_step.integer_step.types.sign.min, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.field, tj.chunk_step.integer_step.types.sign.cursor)
			}
		}
	case CHAR:
		if td != nil {
			if tj.chunk_step.char_step.cmax == "" {
				tj.where = fmt.Sprintf("(%s(`%s` >= '%s'))", tj.chunk_step.char_step.prefix, tj.chunk_step.char_step.field, tj.chunk_step.char_step.cmin_escaped)
			} else {
				update_cursor(o, td.thrconn, tj)
				tj.where = fmt.Sprintf("(%s('%s' < `%s` AND `%s` <= '%s'))", tj.chunk_step.char_step.prefix, tj.chunk_step.char_step.cmin_escaped, tj.chunk_step.char_step.field, tj.chunk_step.char_step.field, tj.chunk_step.char_step.cursor_escaped)
			}
		}
	default:
		return
	}
}

func process_integer_chunk_job(o *OptionEntries, td *thread_data, tj *table_job) bool {
	check_pause_resume(td)
	if o.global.shutdown_triggered {
		return true
	}
	tj.chunk_step.integer_step.mutex.Lock()
	if tj.chunk_step.integer_step.check_max {
		update_integer_max(o, td.thrconn, tj)
		tj.chunk_step.integer_step.check_max = false
	}
	if tj.chunk_step.integer_step.check_min {
		update_integer_min(o, td.thrconn, tj)
		tj.chunk_step.integer_step.check_min = false
	}

	if tj.chunk_step.integer_step.is_unsigned {
		if tj.chunk_step.integer_step.step-1 > tj.chunk_step.integer_step.types.unsign.max-tj.chunk_step.integer_step.types.unsign.min {
			tj.chunk_step.integer_step.types.unsign.cursor = tj.chunk_step.integer_step.types.unsign.max
		} else {
			tj.chunk_step.integer_step.types.unsign.cursor = tj.chunk_step.integer_step.types.unsign.min + tj.chunk_step.integer_step.step - 1
		}
		tj.chunk_step.integer_step.estimated_remaining_steps = (tj.chunk_step.integer_step.types.unsign.max - tj.chunk_step.integer_step.types.unsign.cursor) / tj.chunk_step.integer_step.step
	} else {
		if tj.chunk_step.integer_step.step-1 > uint64(gint64_abs(tj.chunk_step.integer_step.types.sign.max-tj.chunk_step.integer_step.types.sign.min)) {
			tj.chunk_step.integer_step.types.sign.cursor = tj.chunk_step.integer_step.types.sign.max
		} else {
			tj.chunk_step.integer_step.types.sign.cursor = tj.chunk_step.integer_step.types.sign.min + int64(tj.chunk_step.integer_step.step) - 1
		}
		tj.chunk_step.integer_step.estimated_remaining_steps = uint64((tj.chunk_step.integer_step.types.sign.max - tj.chunk_step.integer_step.types.sign.cursor) / int64(tj.chunk_step.integer_step.step))
	}
	tj.chunk_step.integer_step.mutex.Unlock()
	if tj.chunk_step.integer_step.is_unsigned {
		if tj.chunk_step.integer_step.types.unsign.cursor == tj.chunk_step.integer_step.types.unsign.min {
			return false
		} else {
			if tj.chunk_step.integer_step.types.sign.cursor == tj.chunk_step.integer_step.types.sign.min {
				return false
			}
		}
	}

	update_where_on_table_job(o, td, tj)
	//  message_dumping_data(td,tj);

	var from = time.Now()
	write_table_job_into_file(o, td.thrconn, tj)
	var to = time.Now()

	var diff = to.Sub(from).Seconds()

	if diff > 2 {
		tj.chunk_step.integer_step.step = tj.chunk_step.integer_step.step / 2
		if tj.chunk_step.integer_step.step < o.global.min_rows_per_file {
			tj.chunk_step.integer_step.step = o.global.min_rows_per_file
		}
	} else if diff < 1 {
		if tj.chunk_step.integer_step.step*2 == 0 {
			tj.chunk_step.integer_step.step = tj.chunk_step.integer_step.step
		} else {
			tj.chunk_step.integer_step.step = tj.chunk_step.integer_step.step * 2
		}
		if o.global.max_rows_per_file != 0 {
			if tj.chunk_step.integer_step.step > o.global.max_rows_per_file {
				tj.chunk_step.integer_step.step = o.global.max_rows_per_file
			}
		}
	}

	tj.chunk_step.integer_step.mutex.Lock()
	if tj.chunk_step.integer_step.is_unsigned {
		tj.chunk_step.integer_step.types.unsign.min = tj.chunk_step.integer_step.types.unsign.cursor + 1
	} else {
		tj.chunk_step.integer_step.types.sign.min = tj.chunk_step.integer_step.types.sign.cursor + 1
	}
	tj.chunk_step.integer_step.mutex.Unlock()
	return false
}

func process_integer_chunk(o *OptionEntries, td *thread_data, tj *table_job) {
	var dbt = tj.dbt
	var cs = tj.chunk_step
	if process_integer_chunk_job(o, td, tj) {
		log.Infof("Thread %d: Job has been cacelled", td.thread_id)
		return
	}
	atomic.AddInt64(&dbt.chunks_completed, 1)
	cs.integer_step.prefix = ""
	if cs.integer_step.is_unsigned {
		tj.chunk_step.integer_step.mutex.Lock()
		for cs.integer_step.types.unsign.min < cs.integer_step.types.unsign.max {
			tj.chunk_step.integer_step.mutex.Lock()
			if process_integer_chunk_job(o, td, tj) {
				log.Infof("Thread %d: Job has been cacelled", td.thread_id)
				return
			}
			atomic.AddInt64(&dbt.chunks_completed, 1)
			tj.chunk_step.integer_step.mutex.Lock()
		}
		tj.chunk_step.integer_step.mutex.Unlock()
	} else {
		tj.chunk_step.integer_step.mutex.Lock()
		for cs.integer_step.types.sign.min < cs.integer_step.types.sign.max {
			tj.chunk_step.integer_step.mutex.Unlock()
			if process_integer_chunk_job(o, td, tj) {
				log.Infof("Thread %d: Job has been cacelled", td.thread_id)
				return
			}
			atomic.AddInt64(&dbt.chunks_completed, 1)
			tj.chunk_step.integer_step.mutex.Lock()
		}
		tj.chunk_step.integer_step.mutex.Unlock()
	}
	dbt.chunks_mutex.Lock()
	cs.integer_step.mutex.Lock()
	var t []any
	for _, c := range dbt.chunks {
		if c.(*chunk_step) == cs {
			continue
		}
		t = append(t, c)
	}
	dbt.chunks = t
	tj.chunk_step.integer_step.estimated_remaining_steps = 0
	if len(dbt.chunks) == 0 {
		log.Infof("Thread %d: Table %s completed ", td.thread_id, dbt.table)
		dbt.chunks = nil
	}
	dbt.chunks_mutex.Unlock()
	cs.integer_step.mutex.Unlock()
}

func process_char_chunk_job(o *OptionEntries, td *thread_data, tj *table_job) bool {
	check_pause_resume(td)
	if o.global.shutdown_triggered {
		return true
	}
	tj.chunk_step.char_step.mutex.Lock()
	update_where_on_table_job(o, td, tj)
	tj.chunk_step.char_step.mutex.Unlock()
	var from = time.Now()
	write_table_job_into_file(o, td.thrconn, tj)
	var to = time.Now()
	var diff = to.Sub(from).Seconds()
	if diff > 2 {
		tj.chunk_step.char_step.step = tj.chunk_step.char_step.step / 2
		if tj.chunk_step.char_step.step < o.global.min_rows_per_file {
			tj.chunk_step.char_step.step = o.global.min_rows_per_file
		}
	} else if diff < 1 {
		tj.chunk_step.char_step.step = tj.chunk_step.char_step.step * 2
		if o.global.max_rows_per_file != 0 {
			if tj.chunk_step.char_step.step > o.global.max_rows_per_file {
				tj.chunk_step.char_step.step = o.global.max_rows_per_file
			}
		}
	}

	tj.chunk_step.char_step.prefix = ""
	tj.chunk_step.char_step.mutex.Lock()
	next_chunk_in_char_step(tj.chunk_step)
	tj.chunk_step.char_step.mutex.Unlock()
	return false
}

func process_char_chunk(o *OptionEntries, td *thread_data, tj *table_job) {
	var dbt = tj.dbt
	var cs = tj.chunk_step
	var previous = cs.char_step.previous
	var cont bool
	for cs.char_step.previous != nil || cs.char_step.cmax != cs.char_step.cursor {
		if cs.char_step.previous != nil {
			cont = get_new_minmax(o, td, tj.dbt, tj.chunk_step)
			if cont {
				cs.char_step.previous = nil
				cs.char_step.mutex.Lock()
				tj.dbt.chunks = append(tj.dbt.chunks, cs)
				tj.dbt.chunks_mutex.Unlock()
				previous.char_step.mutex.Unlock()
				cs.char_step.mutex.Unlock()
			} else {
				previous.char_step.status = 0
				dbt.chunks_mutex.Unlock()
				previous.char_step.mutex.Unlock()
				return
			}
		} else {
			if strings.Compare(cs.char_step.cmax, cs.char_step.cursor) != 0 {
				if process_char_chunk_job(o, td, tj) {
					log.Infof("Thread %d: Job has been cacelled", td.thread_id)
					return
				}
			} else {
				cs.char_step.mutex.Lock()
				cs.char_step.status = 2
				cs.char_step.mutex.Unlock()
				break
			}
		}
	}
	if strings.Compare(cs.char_step.cursor, cs.char_step.cmin) != 0 {
		if process_char_chunk_job(o, td, tj) {
			log.Infof("Thread %d: Job has been cacelled", td.thread_id)
			return
		}
	}
	dbt.chunks_mutex.Lock()
	cs.char_step.mutex.Lock()
	var t []any
	for _, c := range dbt.chunks {
		if c.(*chunk_step) == cs {
			continue
		}
		t = append(t, c)
	}
	dbt.chunks = t
	cs.char_step.mutex.Unlock()
	dbt.chunks_mutex.Unlock()
}

func process_partition_chunk(o *OptionEntries, td *thread_data, tj *table_job) {
	var cs = tj.chunk_step
	var partition string
	for _, p := range cs.partition_step.list {
		if o.global.shutdown_triggered {
			return
		}
		cs.partition_step.mutex.Lock()
		partition = fmt.Sprintf(" PARTITION (%s) ", p)
		cs.partition_step.mutex.Unlock()
		tj.partition = partition
		write_table_job_into_file(o, td.thrconn, tj)
	}
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
	process_queue(o, td.conf.initial_queue, td, process_job_builder_job, nil)
	td.conf.ready.push(1)
	log.Infof("Thread %d: Schema queue", td.thread_id)
	process_queue(o, td.conf.schema_queue, td, process_job, nil)

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
			process_queue(o, td.conf.non_innodb_queue, td, process_job, give_me_another_non_innodb_chunk_step)
			_, err = td.thrconn.Execute(UNLOCK_TABLES)
			if err != nil {
				log.Errorf("Error locking non-innodb tables %v", err)
			}
		} else {
			process_queue(o, td.conf.non_innodb_queue, td, process_job, give_me_another_non_innodb_chunk_step)
			td.conf.unlock_tables.push(1)
		}

		log.Infof("Thread %d: Non-Innodb Done, Starting Innodb", td.thread_id)
		process_queue(o, td.conf.innodb_queue, td, process_job, give_me_another_innodb_chunk_step)
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
	process_queue(o, td.conf.post_data_queue, td, process_job, nil)

	log.Infof("Thread %d: shutting down", td.thread_id)

	td.binlog_snapshot_gtid_executed = ""

	if td.thrconn != nil {
		td.thrconn.Close()
	}
	return
}

func get_insertable_fields(conn *client.Conn, database string, table string) string {
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
		tb := fmt.Sprintf("`%s`", row[0].AsString())
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
	// var result bool
	var query string
	if o.Extra.IgnoreGeneratedFields {
		return false
	}
	query = fmt.Sprintf("select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra like '%%GENERATED%%' and extra not like '%%DEFAULT_GENERATED%%'", database, table)
	res, err = conn.Execute(query)
	if err != nil {
		log.Errorf("get column name fail:%v", err)
	}
	for _, row := range res.Values {
		_ = row
		return true
	}
	return false
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
			character_set = fmt.Sprintf("%s", row[0].AsString())
			o.global.character_set_hash[collation] = character_set
		}
	}
	o.global.character_set_hash_mutex.Unlock()
	return character_set
}

func new_db_table(o *OptionEntries, conn *client.Conn, conf *configuration, database *database, table string, table_collation string, datalength string, rows_in_sts uint64) *db_table {
	var dbt = new(db_table)
	dbt.database = database
	dbt.table = table
	dbt.table_filename = get_ref_table(o, dbt.table)
	dbt.rows_in_sts = rows_in_sts
	if table_collation != "" {
		dbt.character_set = get_character_set_from_collation(o, conn, table_collation)
	}
	dbt.has_json_fields = has_json_fields(conn, dbt.database.name, dbt.table)
	dbt.rows_lock = g_mutex_new()
	dbt.escaped_table = escape_string(dbt.table)
	dbt.anonymized_function = get_anonymized_function_for(o, conn, dbt.database.name, dbt.table)
	var k = fmt.Sprintf("`%s`.`%s`", dbt.database.name, dbt.table)
	dbt.where, _ = o.global.conf_per_table.all_where_per_table[k]
	dbt.limit, _ = o.global.conf_per_table.all_limit_per_table[k]
	dbt.columns_on_select, _ = o.global.conf_per_table.all_columns_on_select_per_table[k]
	dbt.columns_on_insert, _ = o.global.conf_per_table.all_columns_on_insert_per_table[k]
	var ok bool
	if dbt.num_threads, ok = o.global.conf_per_table.all_num_threads_per_table[k]; !ok {
		dbt.num_threads = o.Common.NumThreads
	}
	dbt.estimated_remaining_steps = 1
	dbt.min = ""
	dbt.max = ""
	dbt.chunk_type = UNDEFINED
	dbt.chunks = nil
	dbt.insert_statement = new(strings.Builder)
	dbt.chunks_mutex = g_mutex_new()
	dbt.chunks_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	dbt.chunks_completed = 0
	dbt.field = get_field_for_dbt(conn, dbt, conf)
	dbt.primary_key = get_primary_key_string(o, conn, dbt.database.name, dbt.table)
	dbt.chunk_filesize = uint(o.Extra.ChunkFilesize)
	dbt.complete_insert = o.Statement.CompleteInsert || detect_generated_fields(o, conn, dbt.database.escaped, dbt.escaped_table)
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
	if datalength == "" {
		dbt.datalength = 0
	} else {
		dbt.datalength, _ = strconv.ParseUint(datalength, 10, 64)
	}

	return dbt
}

func free_db_table(dbt *db_table) {
	dbt.chunks_mutex.Lock()
	dbt.data_checksum = ""
	dbt.field = ""
	var cs *chunk_step
	switch dbt.chunk_type {
	case INTEGER:
		task := dbt.chunks_queue.try_pop()
		if task != nil {
			cs = task.(*chunk_step)
		}
		for cs != nil {
			if cs.integer_step.status == COMPLETED {
				free_integer_step(cs)
			} else {
				log.Errorf("Trying to free uncompleted integer step")
			}
			task = dbt.chunks_queue.try_pop()
			if task != nil {
				cs = task.(*chunk_step)
			} else {
				cs = nil
			}
		}
	default:
		log.Debugf("free %d ", dbt.chunk_type)
	}
	dbt.chunks_mutex.Unlock()

}

func new_table_to_dump(o *OptionEntries, conn *client.Conn, conf *configuration, is_view bool, is_sequence bool, database *database, table string, collation string, datalength string, ecol string, rows_in_sts uint64) {
	database.ad_mutex.Lock()
	if !database.already_dumped {
		create_job_to_dump_schema(o, database, conf)
		database.already_dumped = true
	}
	database.ad_mutex.Unlock()

	var dbt = new_db_table(o, conn, conf, database, table, collation, datalength, rows_in_sts)
	o.global.all_dbts_mutex.Lock()
	o.global.all_dbts = append(o.global.all_dbts, dbt)
	o.global.all_dbts_mutex.Unlock()

	// if a view or sequence we care only about schema
	if (!is_view || o.Objects.ViewsAsTables) && !is_sequence {
		// with trx_consistency_only we dump all as innodb_table
		if !o.Objects.NoSchemas {
			//      write_table_metadata_into_file(dbt);
			o.global.table_schemas_mutex.Lock()
			o.global.table_schemas = append(o.global.table_schemas, dbt)
			o.global.table_schemas_mutex.Unlock()
			create_job_to_dump_table_schema(o, dbt, conf)
		}
		if o.Objects.DumpTriggers && !database.dump_triggers {
			create_job_to_dump_triggers(o, conn, dbt, conf)
		}
		if !o.Objects.NoData {
			if ecol != "" && strings.ToUpper(ecol) != "MRG_MYISAM" {
				if o.Checksum.DataChecksums && !(o.get_major() == 5 && o.get_secondary() == 7 && dbt.has_json_fields) {
					create_job_to_dump_checksum(o, dbt, conf)
				}
				if o.Lock.TrxConsistencyOnly ||
					(ecol != "" && (ecol == "InnoDB" || ecol == "TokuDB")) {
					dbt.is_innodb = true
					o.global.innodb_table_mutex.Lock()
					o.global.innodb_table = append(o.global.innodb_table, dbt)
					o.global.innodb_table_mutex.Unlock()

				} else {
					dbt.is_innodb = false
					o.global.non_innodb_table_mutex.Lock()
					o.global.non_innodb_table = append(o.global.non_innodb_table, dbt)
					o.global.non_innodb_table_mutex.Unlock()
				}
			} else {
				if is_view {
					dbt.is_innodb = false
					o.global.non_innodb_table_mutex.Lock()
					o.global.non_innodb_table = append(o.global.non_innodb_table, dbt)
					o.global.non_innodb_table_mutex.Unlock()
				}
			}
		}
	} else if is_view {
		if !o.Objects.NoSchemas {
			create_job_to_dump_view(o, dbt, conf)
		}
	} else { // is_sequence
		if !o.Objects.NoSchemas {
			create_job_to_dump_sequence(o, dbt, conf)
		}
	}
}

func determine_if_schema_is_elected_to_dump_post(o *OptionEntries, conn *client.Conn, database *database) bool {
	var query string
	var result *mysql.Result
	var err error
	var post_dump bool
	if o.Objects.DumpRoutines {
		query = fmt.Sprintf("SHOW PROCEDURE STATUS WHERE CAST(Db AS BINARY) = '%s'", database.escaped)
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
			post_dump = true
		}
		if !post_dump {
			query = fmt.Sprintf("SHOW FUNCTION STATUS WHERE CAST(Db AS BINARY) = '%s'", database.escaped)
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
				post_dump = true
			}
		}

	}

	if o.Objects.DumpEvents && !post_dump {
		query = fmt.Sprintf("SHOW EVENTS FROM `%s`", database.name)
		result, err = conn.Execute(query)
		if err != nil {
			log.Errorf("Error showing events on: %s - Could not execute query: %v", database.name, err)
			return false
		}
		for _, row := range result.Values {
			if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, database.name, string(row[1].AsString())) {
				continue
			}
			if !eval_regex(o, database.name, string(row[1].AsString())) {
				continue
			}
			post_dump = true
		}

	}
	return post_dump
}

func dump_database_thread(o *OptionEntries, conn *client.Conn, conf *configuration, database *database) {
	var query string
	conn.UseDB(database.name)
	if o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_PERCONA || o.global.detected_server == SERVER_TYPE_UNKNOWN {
		query = fmt.Sprintf("SHOW TABLE STATUS")
	} else if o.global.detected_server == SERVER_TYPE_MARIADB {
		query = fmt.Sprintf("SELECT TABLE_NAME, ENGINE, TABLE_TYPE as COMMENT, TABLE_COLLATION as COLLATION, AVG_ROW_LENGTH, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s'", database.escaped)
	} else {
		return
	}
	result, err := conn.Execute(query)
	if err != nil {
		log.Fatalf("Error showing tables on: %s - Could not execute query: %v", database.name, err)
	}
	var ecol = -1
	var ccol = -1
	var collcol = -1
	var rowscol = 0
	determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
	for _, row := range result.Values {
		var dump = true
		var is_view = false
		var is_sequence = false
		if (o.is_mysql_like() || o.global.detected_server == SERVER_TYPE_TIDB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
			is_view = true
		}
		if o.global.detected_server == SERVER_TYPE_MARIADB && string(row[ccol].AsString()) == "VIEW" {
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
				if strings.ToLower(ignore) == string(row[ecol].AsString()) {
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
		if len(o.global.tables) > 0 {
			if !is_table_in_list(string(row[0].AsString()), o.global.tables) {
				dump = false
			}
		}
		if !dump {
			continue
		}

		/* Special tables */
		if strings.Compare(database.name, "mysql") == 0 && (strings.Compare(string(row[0].AsString()), "general_log") == 0 || strings.Compare(string(row[0].AsString()), "slow_log") == 0 || strings.Compare(string(row[0].AsString()), "innodb_index_stats") == 0 || strings.Compare(string(row[0].AsString()), "innodb_table_stats") == 0) {
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
		rows := row[rowscol].AsUint64()

		new_table_to_dump(o, conn, conf, is_view, is_sequence, database, string(row[0].AsString()), string(row[collcol].AsString()), string(row[6].AsString()), string(row[ecol].AsString()), rows)

	}
	if determine_if_schema_is_elected_to_dump_post(o, conn, database) {
		create_job_to_dump_post(o, database, conf)
	}
	if database.dump_triggers {
		create_job_to_dump_schema_triggers(o, database, conf)
	}
	return
}
