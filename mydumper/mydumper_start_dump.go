package mydumper

import (
	"container/list"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shirou/gopsutil/disk"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"os/signal"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	SourceData                         int
	TidbSnapshot                       string
	NoLocks                            bool
	NoBackupLocks                      bool
	LockAllTables                      bool
	LessLocking                        bool
	TrxConsistencyOnly                 bool
	SkipDdlLocks                       bool
	LongqueryRetries                   int
	LongqueryRetryInterval             int    = 60
	Longquery                          uint64 = 60
	Killqueries                        bool
	Exec_command                       string
	PmmPath                            string
	PmmResolution                      string
	UpdatedSince                       int
	DumpTablespaces                    bool
	chunk_builder                      *GThreadFunc
	threads                            []*GThreadFunc
	td                                 []*thread_data
	all_dbts                           map[string]*DB_Table
	conf_per_table                     *Configuration_per_table = new(Configuration_per_table)
	it_is_a_consistent_backup          bool
	no_updated_tables                  []string
	identifier_quote_character_protect func(string) string
	db_items                           []string
	table_schemas                      []*DB_Table
	pause_at                           uint
	resume_at                          uint
	pmm                                bool
	need_dummy_read                    bool
	need_dummy_toku_read               bool
	pause_mutex_per_thread             []*sync.Mutex
	disk_check_thread                  *GThreadFunc
	sthread                            *GThreadFunc
	pmmthread                          *GThreadFunc
	ready_table_dump_mutex             *sync.Mutex
	replica_stopped                    bool
	exec_command                       string
)

type job_type uint8
type chunk_type uint8
type chunk_states uint8

const MAX_START_TRANSACTION_RETRIES = 5
const MYDUMPER = "mydumper"

const (
	JOB_SHUTDOWN job_type = iota
	JOB_RESTORE
	JOB_DUMP
	JOB_DUMP_NON_INNODB
	JOB_DEFER
	JOB_DETERMINE_CHUNK_TYPE
	JOB_TABLE
	JOB_CHECKSUM
	JOB_SCHEMA
	JOB_VIEW
	JOB_SEQUENCE
	JOB_TRIGGERS
	JOB_SCHEMA_TRIGGERS
	JOB_SCHEMA_POST
	JOB_BINLOG
	JOB_CREATE_DATABASE
	JOB_CREATE_TABLESPACE
	JOB_DUMP_DATABASE
	JOB_DUMP_ALL_DATABASES
	JOB_DUMP_TABLE_LIST
	JOB_WRITE_MASTER_STATUS
)

const (
	NONE chunk_type = iota
	INTEGER
	CHAR
	PARTITION
)

const (
	UNASSIGNED chunk_states = iota
	ASSIGNED
	DUMPING_CHUNK
	COMPLETED
)

type configuration struct {
	use_any_index               string
	initial_queue               *GAsyncQueue
	schema_queue                *GAsyncQueue
	non_innodb                  *table_queuing
	innodb                      *table_queuing
	post_data_queue             *GAsyncQueue
	ready                       *GAsyncQueue
	ready_non_innodb_queue      *GAsyncQueue
	db_ready                    *GAsyncQueue
	binlog_ready                *GAsyncQueue
	unlock_tables               *GAsyncQueue
	pause_resume                *GAsyncQueue
	gtid_pos_checked            *GAsyncQueue
	are_all_threads_in_same_pos *GAsyncQueue
	lock_tables_statement       string
	mutex                       *sync.Mutex
	done                        int
}
type MList struct {
	list  *list.List
	mutex *sync.Mutex
}
type thread_data struct {
	conf                          *configuration
	thread_id                     uint
	table_name                    string
	thrconn                       *DBConnection
	less_locking_stage            bool
	binlog_snapshot_gtid_executed string
	pause_resume_mutex            *sync.Mutex
}

type job struct {
	types    job_type
	job_data any
}

type unsigned_int struct {
	min    uint64
	cursor uint64
	max    uint64
}
type signed_int struct {
	min    int64
	cursor int64
	max    int64
}

type int_types struct {
	unsign *unsigned_int
	sign   *signed_int
}

func new_int_types() *int_types {
	i := new(int_types)
	i.unsign = new(unsigned_int)
	i.sign = new(signed_int)
	return i
}

type integer_step struct {
	is_unsigned               bool
	types                     *int_types
	is_step_fixed_length      bool
	step                      uint64
	min_chunk_step_size       uint64
	max_chunk_step_size       uint64
	estimated_remaining_steps uint64
	check_max                 bool
	check_min                 bool
}

type char_step struct {
	cmin                      string
	cmin_len                  uint
	cmin_clen                 uint
	cmin_escaped              string
	cursor                    string
	cursor_len                uint
	cursor_clen               uint
	cursor_escaped            string
	cmax                      string
	cmax_len                  uint
	cmax_clen                 uint
	cmax_escaped              string
	deep                      uint
	list                      []string
	step                      uint64
	previous                  *chunk_step
	estimated_remaining_steps uint64
	status                    uint
	mutex                     *sync.Mutex
}

type tables_job struct {
	table_job_list []string
}

type dump_database_job struct {
	database *database
}

type dump_table_list_job struct {
	table_list []string
}

type restore_job struct {
	database string
	table    string
	filename string
}

type binlog_job struct {
	filename       string
	start_position uint64
	stop_position  uint64
}
type chunk_functions struct {
	process  func(tj *table_job, csi *chunk_step_item)
	get_next func(dbt *DB_Table) *chunk_step_item
}
type table_queuing struct {
	queue         *GAsyncQueue
	deferQueue    *GAsyncQueue
	request_chunk *GAsyncQueue
	table_list    *MList
	descr         string
}

type fifo struct {
	filename        string
	stdout_filename string
	queue           *GAsyncQueue
	size            float64
	dbt             *DB_Table
	fout            *file_write
	gpid            int
	child_pid       int
	pipe            [2]*file_write
	out_mutes       *sync.Mutex
	err_member      error
}

type dump_table_job struct {
	is_view     bool
	is_sequence bool
	database    *database
	table       string
	collation   string
	engine      string
}
type chunk_step_item struct {
	chunk_step      *chunk_step
	chunk_type      chunk_type
	next            *chunk_step_item
	chunk_functions *chunk_functions
	where           string
	include_null    bool
	prefix          string
	field           string
	number          uint64
	deep            uint
	position        uint
	mutex           *sync.Mutex
	needs_refresh   bool
	status          chunk_states
}
type table_job_file struct {
	filename string
	file     *file_write
}

type table_job struct {
	partition string
	nchunk    uint64
	sub_part  uint
	where     string
	// chunk_step        *chunk_step
	chunk_step_item *chunk_step_item
	order_by        string
	dbt             *DB_Table
	// sql_filename      string
	// sql_file          *file_write
	// dat_filename      string
	// dat_file          *file_write
	sql               *table_job_file
	rows              *table_job_file
	exec_out_filename string
	filesize          float64
	st_in_file        uint
	child_process     int
	char_chunk_part   uint
	td                *thread_data
}

type chunk_step struct {
	integer_step   *integer_step
	char_step      *char_step
	partition_step *partition_step
}

type partition_step struct {
	list              []string
	current_partition string
	number            uint
	deep              uint
	mutex             *sync.Mutex
	assigned          bool
}

type DB_Table struct {
	key                            string
	database                       *database
	table                          string
	table_filename                 string
	escaped_table                  string
	min                            string
	max                            string
	object_to_export               *Object_to_export
	select_fields                  string
	complete_insert                bool
	insert_statement               *GString
	load_data_header               *GString
	load_data_suffix               *GString
	is_innodb                      bool
	is_sequence                    bool
	has_json_fields                bool
	character_set                  string
	rows_total                     uint64
	rows                           uint64
	estimated_remaining_steps      uint64
	rows_lock                      *sync.Mutex
	anonymized_function            []*Function_pointer
	where                          string
	limit                          string
	columns_on_select              string
	columns_on_insert              string
	partition_regex                *regexp.Regexp
	num_threads                    uint
	chunks                         *list.List
	chunks_queue                   *GAsyncQueue
	chunks_mutex                   *sync.Mutex
	primary_key                    []string
	primary_key_separated_by_comma string
	multicolumn                    bool
	chunks_completed               int64
	data_checksum                  string
	schema_checksum                string
	indexes_checksum               string
	triggers_checksum              string
	chunk_filesize                 uint
	split_integer_tables           bool
	min_chunk_step_size            uint64
	starting_chunk_step_size       uint64
	max_chunk_step_size            uint64
	status                         db_table_states
	max_threads_per_table          uint
	current_threads_running        uint
}

type db_table_states int

const (
	UNDEFINED db_table_states = iota
	DEFINING
	READY
)

type lock_function func(conn *DBConnection)

func initialize_start_dump() {
	all_dbts = make(map[string]*DB_Table)
	Initialize_set_names()
	initialize_working_thread()
	Initialize_conf_per_table(conf_per_table)

	// until we have an unique option on lock int_types we need to ensure this
	if NoLocks || TrxConsistencyOnly {
		LessLocking = false
	}

	// clarify binlog coordinates with trx_consistency_only
	if TrxConsistencyOnly {
		log.Warnf("Using trx_consistency_only, binlog coordinates will not be  accurate if you are writing to non transactional tables.")
	}

	if DB != "" {
		db_items = strings.Split(DB, ",")
	}

	if PmmPath != "" {
		pmm = true
		if PmmResolution == "" {
			PmmResolution = "high"
		}
	} else if PmmResolution != "" {
		pmm = true
		PmmPath = fmt.Sprintf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution", PmmResolution)
	}

	/*if o.Stream.Stream && o.Exec.Exec_command != "" {
		log.Fatalf("Stream and execute a command is not supported")
	} */
}

func set_disk_limits(p_at, r_at uint) {
	pause_at = p_at
	resume_at = r_at
}

func is_disk_space_ok(val uint) bool {
	if !path.IsAbs(dump_directory) {
		pwd, _ := os.Getwd()
		dump_directory = path.Join(pwd, dump_directory)
	}
	partitions, err := disk.Partitions(true)
	if err != nil {
		log.Errorf("Error getting partitions: %s", err.Error())
	}
	// 找到指定路径的挂载分区
	var mountPoint string
	for _, partition := range partitions {
		if dump_directory == partition.Mountpoint || (len(dump_directory) > len(partition.Mountpoint) && dump_directory[:len(partition.Mountpoint)] == partition.Mountpoint) {
			mountPoint = partition.Mountpoint
			break
		}
	}

	if mountPoint == "" {
		log.Fatalf("No partition found for path: %s", dump_directory)
	}
	// 获取分区使用情况
	usage, err := disk.Usage(mountPoint)
	if err != nil {
		log.Criticalf("Error getting disk usage: %v", err)
	}
	return usage.Free/1024/1024 > uint64(val)
}

func monitor_disk_space_thread(queue *GAsyncQueue) {
	defer disk_check_thread.Thread.Done()
	var i uint
	for i = 0; i < NumThreads; i++ {
		pause_mutex_per_thread[i] = G_mutex_new()
	}
	var previous_state = true
	var current_state = true
	for DiskLimits != "" {
		if previous_state {
			current_state = is_disk_space_ok(pause_at)
		} else {
			current_state = is_disk_space_ok(resume_at)
		}
		if previous_state != current_state {
			if !current_state {
				log.Warnf("Pausing backup disk space lower than %dMB. You need to free up to %dMB to resume", pause_at, resume_at)
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Lock()
					G_async_queue_push(queue, pause_mutex_per_thread[i])
				}
			} else {
				log.Warnf("Resuming backup")
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Unlock()
				}
			}
			previous_state = current_state
		}
		time.Sleep(10 * time.Second)
	}
	// return
}

func sig_triggered(user_data any, signal os.Signal) bool {
	if signal == syscall.SIGTERM {
		shutdown_triggered = true
	} else {
		var i uint
		if len(pause_mutex_per_thread) == 0 {
			pause_mutex_per_thread = make([]*sync.Mutex, NumThreads)
			for i = 0; i < NumThreads; i++ {
				pause_mutex_per_thread[i] = G_mutex_new()
			}
		}
		if user_data.(*configuration).pause_resume == nil {
			user_data.(*configuration).pause_resume = G_async_queue_new(BufferSize)
		}
		var queue = user_data.(*configuration).pause_resume
		if !DaemonMode {
			var datetimestr = M_date_time_new_now_local()
			fmt.Printf("%s: Ctrl+c detected! Are you sure you want to cancel(Y/N)?", datetimestr)
			for i = 0; i < NumThreads; i++ {
				pause_mutex_per_thread[i].Lock()
				G_async_queue_push(queue, pause_mutex_per_thread[i])
			}
			var c string
			_, _ = fmt.Scanln(&c)
			if strings.ToUpper(c) == "N" {
				datetimestr = M_date_time_new_now_local()
				fmt.Printf("%s: Resuming backup\n", datetimestr)
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Unlock()
				}
				return true
			}
			if strings.ToUpper(c) == "Y" {
				datetimestr = M_date_time_new_now_local()
				fmt.Printf("%s: Backup cancelled\n", datetimestr)
				shutdown_triggered = true
				for i = 0; i < NumThreads; i++ {
					pause_mutex_per_thread[i].Unlock()
				}
				log.Infof("Shutting down gracefully")
				return false
			}
		}
	}
	return false
}

func signal_thread(conf *configuration) {
	defer sthread.Thread.Done()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	sig_triggered(conf, sig)
	log.Infof("Ending signal thread")
	return
}

func initialize_sql_mode(set_session_hash map[string]string) {
	var str = Sql_mode
	str = strings.ReplaceAll(str, "ORACLE", "")
	str = strings.ReplaceAll(str, ",,", ",")
	set_session_hash["SQL_MODE"] = str
}

func mydumper_initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = Initialize_hash_of_session_variables()
	set_session_hash["information_schema_stats_expiry"] = "0 /*!80003"
	return set_session_hash
}

func create_connection() *DBConnection {
	var conn *DBConnection
	M_connect(conn)
	Execute_gstring(conn, Set_session)
	return conn
}

func detect_quote_character(conn *DBConnection) {
	var res *mysql.Result
	var row []mysql.FieldValue
	var query = "SELECT FIND_IN_SET('ANSI', @@SQL_MODE) OR FIND_IN_SET('ANSI_QUOTES', @@SQL_MODE)"
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Warnf("We were not able to determine ANSI mode: %v", conn.Err)
		Identifier_quote_character = BACKTICK
		Identifier_quote_character_str = "`"
		fields_enclosed_by = "\""
		identifier_quote_character_protect = Backtick_protect
		return
	}
	if len(res.Values) == 0 {
		log.Warnf("We were not able to determine ANSI mode")
		return
	}
	for _, row = range res.Values {
		if row[0].AsInt64() == 0 {
			Identifier_quote_character = BACKTICK
			Identifier_quote_character_str = "`"
			fields_enclosed_by = "\""
			identifier_quote_character_protect = Backtick_protect
		} else {
			Identifier_quote_character = DOUBLE_QUOTE
			Identifier_quote_character_str = "\""
			fields_enclosed_by = "'"
			identifier_quote_character_protect = Double_quoute_protect
		}
	}
}

func detect_sql_mode(conn *DBConnection) {
	var res *mysql.Result
	var row []mysql.FieldValue
	var query = "SELECT @@SQL_MODE"
	res = conn.Execute(query)
	if conn.Err != nil {
		log.Critical("Error getting SQL_MODE: %v", conn.Err)
	}
	if len(res.Values) == 0 {
		log.Critical("Error getting SQL_MODE")
	}
	var str string
	for _, row = range res.Values {
		if !strings.Contains(string(row[0].AsString()), "NO_AUTO_VALUE_ON_ZERO") {
			str = fmt.Sprintf("NO_AUTO_VALUE_ON_ZERO,%s", row[0].AsString())
		} else {
			str = fmt.Sprintf("'%s'", row[0].AsString())
		}
		str = strings.ReplaceAll(str, "NO_BACKSLASH_ESCAPES", "")
		str = strings.ReplaceAll(str, ",,", ",")

		str = strings.ReplaceAll(str, "PIPES_AS_CONCAT", "")
		str = strings.ReplaceAll(str, ",,", ",")
		str = strings.ReplaceAll(str, "NO_KEY_OPTIONS", "")
		str = strings.ReplaceAll(str, ",,", ",")
		str = strings.ReplaceAll(str, "NO_TABLE_OPTIONS", "")
		str = strings.ReplaceAll(str, ",,", ",")
		str = strings.ReplaceAll(str, "NO_FIELD_OPTIONS", "")
		str = strings.ReplaceAll(str, ",,", ",")
		str = strings.ReplaceAll(str, "STRICT_TRANS_TABLES", "")
		str = strings.ReplaceAll(str, ",,", ",")
		Sql_mode = str
	}
}

func create_main_connection() (conn *DBConnection) {
	M_connect(conn)
	if conn.Err != nil {
		return
	}
	Set_session = G_string_new("")
	Set_global = G_string_new("")
	Set_global_back = G_string_new("")
	_ = Detect_server_version(conn)
	Detected_server = Get_product()
	var set_session_hash = mydumper_initialize_hash_of_session_variables()
	var set_global_hash = make(map[string]string)
	if Key_file != nil {
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_global_hash, "mydumper_global_variables")
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_session_hash, "mydumper_session_variables")
		Load_per_table_info_from_key_file(Key_file, conf_per_table, nil)
	}
	Sql_mode = set_session_hash["SQL_MODE"]
	if Sql_mode == "" {
		detect_sql_mode(conn)
		initialize_sql_mode(set_session_hash)
	}
	Refresh_set_session_from_hash(Set_session, set_session_hash)
	Refresh_set_global_from_hash(Set_global, Set_global_back, set_global_hash)
	// free_hash_table(set_session_hash)
	Execute_gstring(conn, Set_session)
	Execute_gstring(conn, Set_global)
	detect_quote_character(conn)
	initialize_headers()
	initialize_write()
	switch Detected_server {
	case SERVER_TYPE_MYSQL:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_MARIADB:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_TIDB:
		DataChecksums = false
	case SERVER_TYPE_PERCONA:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_UNKNOWN:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_CLICKHOUSE:
		DataChecksums = false
	default:
		log.Criticalf("Cannot detect server type")
	}
	log.Infof("Connected to %s %d.%d.%d", Get_product_name(), Get_major(), Get_secondary(), Get_revision())
	return conn
}

func get_not_updated(conn *DBConnection, file *os.File) {
	var res *mysql.Result
	var query string
	query = fmt.Sprintf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND UPDATE_TIME < NOW() - INTERVAL '%d' DAY", UpdatedSince)
	res = conn.Execute(query)
	if conn.Err != nil {
		return
	}
	for _, row := range res.Values {
		no_updated_tables = append(no_updated_tables, string(row[0].AsString()))
		_, _ = file.WriteString(fmt.Sprintf("%s\n", row[0].AsString()))
	}
	slices.Sort(no_updated_tables)
	_ = file.Sync()
	return
}

func long_query_wait(conn *DBConnection) {
	var p3 string
	for {
		var longquery_count int
		res := conn.Execute("SHOW PROCESSLIST")
		if conn.Err != nil {
			log.Warnf("Could not check PROCESSLIST, no long query guard enabled: %v", conn.Err)
			break
		} else {

			/* Just in case PROCESSLIST output column order changes */
			var fields = res.Fields
			var i int
			var tcol = -1
			var ccol = -1
			var icol = -1
			var ucol = -1
			for i = 0; i < res.ColumnNumber(); i++ {
				if string(fields[i].Name) == "Command" {
					ccol = i
				} else if string(fields[i].Name) == "Time" {
					tcol = i
				} else if string(fields[i].Name) == "Id" {
					icol = i
				} else if string(fields[i].Name) == "User" {
					ucol = i
				}
			}
			if tcol < 0 || ccol < 0 || icol < 0 {
				log.Critical("Error obtaining information from processlist")
			}
			for _, row := range res.Values {
				if string(row[ccol].AsString()) == "Query" || strings.Contains(string(row[ccol].AsString()), "Dump") {
					continue
				}
				if string(row[ucol].AsString()) == "system user" || string(row[ucol].AsString()) == "event_scheduler" {
					continue
				}
				if row[tcol].AsUint64() > Longquery {
					if Killqueries {
						p3 = fmt.Sprintf("KILL %d", row[icol].AsUint64())
						_ = conn.Execute(p3)
						if conn.Err != nil {
							log.Warnf("Could not KILL slow query: %v", conn.Err)
							longquery_count++
						} else {
							log.Warnf("Killed a query that was running for %ds", row[tcol].AsUint64())
						}
					} else {
						longquery_count++
					}
				}
			}
			if longquery_count == 0 {
				break
			} else {
				if LongqueryRetries == 0 {
					log.Criticalf("There are queries in PROCESSLIST running longer than %ds, aborting dump, use --long-query-guard to change the guard value, kill queries (--kill-long-queries) or use different server for dump", Longquery)
				}
				LongqueryRetries--
				log.Warnf("There are queries in PROCESSLIST running longer than %ds, retrying in %d seconds (%d left).", Longquery, LongqueryRetryInterval, LongqueryRetries)
				time.Sleep(time.Duration(LongqueryRetryInterval) * time.Second)
			}
		}
	}
}
func mysql_query_verbose(conn *DBConnection, q string) error {
	_ = conn.Execute(q)
	if conn.Err == nil {
		log.Infof("%s: OK", q)

	} else {
		log.Errorf("%s: %v", q, conn.Err)
	}
	return conn.Err
}

func send_backup_stage_on_block_commit(conn *DBConnection) {
	err := mysql_query_verbose(conn, "BACKUP STAGE BLOCK_COMMIT")
	if err != nil {
		log.Criticalf("Couldn't acquire BACKUP STAGE BLOCK_COMMIT: %v", err)
		errors++
	}
}

func send_mariadb_backup_locks(conn *DBConnection) {
	err := mysql_query_verbose(conn, "BACKUP STAGE START")
	if err != nil {
		log.Criticalf("Couldn't acquire BACKUP STAGE START: %v", err)
	}
	err = mysql_query_verbose(conn, "BACKUP STAGE BLOCK_DDL")
	if err != nil {
		log.Criticalf("Couldn't acquire BACKUP STAGE BLOCK_DDL: %v", err)
		errors++
	}
}

func send_percona57_backup_locks(conn *DBConnection) {
	err := mysql_query_verbose(conn, "LOCK TABLES FOR BACKUP")
	if err != nil {
		log.Criticalf("Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will not be consistent: %v", err)
		errors++
	}
	err = mysql_query_verbose(conn, "LOCK BINLOG FOR BACKUP")
	if err != nil {
		log.Criticalf("Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will not be consistent: %v", err)
		errors++
	}
}

func send_ddl_lock_instance_backup(conn *DBConnection) {
	err := mysql_query_verbose(conn, "LOCK INSTANCE FOR BACKUP")
	if err != nil {
		log.Criticalf("Couldn't acquire LOCK INSTANCE FOR BACKUP: %v", err)
		errors++
	}
}

func send_unlock_tables(conn *DBConnection) {
	_ = mysql_query_verbose(conn, "UNLOCK TABLES")
}

func send_unlock_binlogs(conn *DBConnection) {
	_ = mysql_query_verbose(conn, "UNLOCK BINLOG")
}

func send_ddl_unlock_instance_backup(conn *DBConnection) {
	_ = mysql_query_verbose(conn, "UNLOCK INSTANCE")
}

func send_backup_stage_end(conn *DBConnection) {
	_ = mysql_query_verbose(conn, "BACKUP STAGE END")

}

func send_flush_table_with_read_lock(conn *DBConnection) {
	err := mysql_query_verbose(conn, "FLUSH NO_WRITE_TO_BINLOG TABLES")
	if err != nil {
		log.Warnf("Flush tables failed, we are continuing anyways: %v", err)
	}
	log.Infof("Acquiring FTWRL")
	err = mysql_query_verbose(conn, "FLUSH TABLES WITH READ LOCK")
	if err != nil {
		log.Criticalf("Couldn't acquire global lock, snapshots will not be consistent: %v", err)
		errors++
	}
}

func default_locking() (acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function) {
	acquire_ddl_lock_function = nil
	release_ddl_lock_function = nil
	acquire_global_lock_function = send_flush_table_with_read_lock
	release_global_lock_function = send_unlock_tables
	release_binlog_function = nil
	return
}

func determine_ddl_lock_function(conn *DBConnection) (acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function) {
	switch Get_product() {
	case SERVER_TYPE_PERCONA:
		switch Get_major() {
		case 8:
			acquire_ddl_lock_function = send_ddl_lock_instance_backup
			release_ddl_lock_function = send_ddl_unlock_instance_backup
			acquire_global_lock_function = send_flush_table_with_read_lock
			release_global_lock_function = send_unlock_tables
		case 5:
			if Get_secondary() == 7 {
				if NoBackupLocks {
					acquire_ddl_lock_function = nil
					release_ddl_lock_function = nil
				} else {
					acquire_ddl_lock_function = send_percona57_backup_locks
					release_ddl_lock_function = send_unlock_tables
				}
				acquire_global_lock_function = send_flush_table_with_read_lock
				release_global_lock_function = send_unlock_tables

				release_binlog_function = send_unlock_binlogs
				conn = create_connection()
			} else {
				acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
			}
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
	case SERVER_TYPE_MYSQL:
		switch Get_major() {
		case 8:
			acquire_ddl_lock_function = send_ddl_lock_instance_backup
			release_ddl_lock_function = send_ddl_unlock_instance_backup
			acquire_global_lock_function = send_flush_table_with_read_lock
			release_global_lock_function = send_unlock_tables
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
	case SERVER_TYPE_MARIADB:
		if (Get_major() == 10 && Get_secondary() >= 5) || Get_major() > 10 {
			acquire_ddl_lock_function = send_mariadb_backup_locks
			release_ddl_lock_function = nil
			acquire_global_lock_function = send_backup_stage_on_block_commit
			release_global_lock_function = send_backup_stage_end
		} else {
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
	default:
		acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
	}
	return
}

func print_dbt_on_metadata_gstring(dbt *DB_Table, data *string) {
	var name string = Newline_protect(dbt.database.name)
	var table_filename = Newline_protect(dbt.table_filename)
	var table = Newline_protect(dbt.table)
	dbt.chunks_mutex.Lock()
	*data += fmt.Sprintf("\n[%s]\n", dbt.key)
	*data += fmt.Sprintf("real_table_name=%s\nrows = %d\n", table, dbt.rows)
	_ = name
	_ = table_filename
	if dbt.is_sequence {
		*data += "is_sequence = 1\n"
	}
	if dbt.data_checksum != "" {
		*data += fmt.Sprintf("data_checksum = %s\n", dbt.data_checksum)
	}
	if dbt.schema_checksum != "" {
		*data += fmt.Sprintf("schema_checksum = %s\n", dbt.schema_checksum)
	}
	if dbt.indexes_checksum != "" {
		*data += fmt.Sprintf("indexes_checksum = %s\n", dbt.indexes_checksum)
	}
	if dbt.triggers_checksum != "" {
		*data += fmt.Sprintf("triggers_checksum = %s\n", dbt.triggers_checksum)
	}
	dbt.chunks_mutex.Unlock()
}

func print_dbt_on_metadata(mdfile *os.File, dbt *DB_Table) {
	var data string
	print_dbt_on_metadata_gstring(dbt, &data)
	fmt.Fprintf(mdfile, data)
	mdfile.Sync()
	if CheckRowCount && (dbt.rows != dbt.rows_total) {
		log.Criticalf("Row count mismatch found for %s.%s: got %d of %d expected", dbt.database.name, dbt.table, dbt.rows, dbt.rows_total)
	}

}

func send_lock_all_tables(conn *DBConnection) {
	// LOCK ALL TABLES
	var query string
	var dbtb string
	var dt []string
	var res *mysql.Result
	var tables_lock []string
	var success bool
	var retry uint
	var lock = true
	var i uint = 0

	if len(Tables) > 0 {
		for _, t := range Tables {
			dt = strings.Split(t, ".")
			query = fmt.Sprintf("SHOW TABLES IN %s LIKE '%s'", dt[0], dt[1])
			res = conn.Execute(query)
			if conn.Err != nil {
				log.Errorf("Error showing tables in: %s - Could not execute query: %v", dt[0], conn.Err)
				errors++
				return
			} else {
				for _, row := range res.Values {
					if TablesSkiplistFile != "" && Check_skiplist(dt[0], string(row[0].AsString())) {
						continue
					}
					if Is_mysql_special_tables(dt[0], string(row[0].AsString())) {
						continue
					}
					if !Eval_regex(dt[0], string(row[0].AsString())) {
						continue
					}
					dbtb = fmt.Sprintf("%s%s%s.%s%s%s", Identifier_quote_character_str, dt[0], Identifier_quote_character_str,
						Identifier_quote_character_str, row[0].AsString(), Identifier_quote_character_str)
					tables_lock = append(tables_lock, dbtb)
				}
			}
		}
		slices.Sort(tables_lock)
	} else {
		if DB != "" {
			var db_quoted_list string
			db_quoted_list += fmt.Sprintf("'%s'", db_items[i])
			i++
			for ; i < uint(len(db_items)); i++ {
				db_quoted_list += fmt.Sprintf(",'%s'", db_items[i])
			}
			query = fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA in (%s) AND TABLE_TYPE ='BASE TABLE'", db_quoted_list)
		} else {
			query = fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE' AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'data_dictionary')")
		}
		res = conn.Execute(query)
		if conn.Err != nil {
			log.Criticalf("Couldn't get table list for lock all tables: %v", conn.Err)
			errors++
		} else {
			for _, row := range res.Values {
				if TablesSkiplistFile != "" && Check_skiplist(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				if Is_mysql_special_tables(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				if !Eval_regex(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				dbtb = fmt.Sprintf("%s%s%s.%s%s%s", Identifier_quote_character_str, row[0].AsString(), Identifier_quote_character_str,
					Identifier_quote_character_str, row[1].AsString(), Identifier_quote_character_str)
				tables_lock = append(tables_lock, dbtb)
			}
		}
	}

	if len(tables_lock) > 0 {
		res = conn.Execute(query)
		if conn.Err != nil {
			log.Criticalf("Couldn't get table list for lock all tables: %v", conn.Err)
			errors++
		} else {
			for _, row := range res.Values {
				lock = true
				if len(Tables) > 0 {
					var table_found bool
					for _, t := range Tables {
						if strings.Compare(t, string(row[1].AsString())) == 0 {
							table_found = true
							break
						}
					}
					if !table_found {
						lock = false
					}
				}

				if lock && TablesSkiplistFile != "" && Check_skiplist(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}

				if lock && !Eval_regex(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}

				if lock {
					dbtb = fmt.Sprintf("`%s`.`%s`", row[0].AsString(), row[1].AsString())
					tables_lock = append(tables_lock, dbtb)
				}
			}
			slices.Sort(tables_lock)
		}
	}

	if len(tables_lock) > 0 {
		// Try three times to get the lock, this is in case of tmp tables
		// disappearing
		for len(tables_lock) > 0 && !success && retry < 4 {
			query = ""
			query += "LOCK TABLE"
			for _, iter := range tables_lock {
				query += fmt.Sprintf("%s READ,", iter)
			}
			query = strings.Trim(query, ",")
			_ = conn.Execute(query)
			if conn.Err != nil {

				var tmp_fail []string = strings.Split(conn.Err.Error(), "'")
				tmp_fail = strings.Split(tmp_fail[1], ".")
				var failed_table string = fmt.Sprintf("`%s`.`%s`", tmp_fail[0], tmp_fail[1])
				var tmp_list []string
				for _, t := range tables_lock {
					// tables_lock = g_list_remove(tables_lock, iter->data);
					if t == failed_table {
						continue
					}
					tmp_list = append(tmp_list, t)
				}
				tables_lock = tmp_list
			} else {
				success = true
			}
			retry += 1
		}
		if !success {
			log.Criticalf("Lock all tables fail: %v", conn.Err)
		}
	} else {
		log.Warnf("No table found to lock")
		//    exit(EXIT_FAILURE);
	}
}

func write_replica_info(conn *DBConnection, file *os.File) {
	var slave *mysql.Result
	var fields []mysql.Field
	var row []mysql.FieldValue
	var slavehost string
	var slavelog string
	var slavepos string
	var slavegtid string
	var channel_name string
	var gtid_title string
	var i uint
	var isms bool
	rest := conn.Execute("SELECT @@default_master_connection")
	if rest != nil && conn.Err == nil {
		log.Infof("Multisource slave detected.")
		isms = true
	}
	var slave_count uint
	if isms {
		M_query(conn, Show_all_replicas_status, M_critical, fmt.Sprintf("Error executing %s", Show_all_replicas_status))
	} else {
		M_query(conn, Show_replica_status, M_critical, fmt.Sprintf("Error executing %s", Show_replica_status))
	}
	slave = conn.Result
	if slave == nil || len(slave.Values) == 0 {
		return
	}
	log.Infof("Stopping replica")
	_ = conn.Execute(Stop_replica_sql_thread)
	if conn.Err != nil {
		log.Warnf("Not able to stop replica: %v", conn.Err)
	}
	if Source_control_command == AWS {
		Discard_mysql_output(conn)
	}
	if isms {
		M_query(conn, Show_all_replicas_status, M_critical, fmt.Sprintf("Error executing %s", Show_all_replicas_status))
	} else {
		M_query(conn, Show_replica_status, M_critical, fmt.Sprintf("Error executing %s", Show_replica_status))
	}
	slave = conn.Result
	var replication_section_str string
	for _, row = range slave.Values {
		if string(fields[i].Name) == "exec_master_log_pos" || string(fields[i].Name) == "exec_source_log_pos" {
			slavepos = string(row[i].AsString())
		} else if string(fields[i].Name) == "relay_master_log_file" || string(fields[i].Name) == "relay_source_log_file" {
			slavelog = string(row[i].AsString())
		} else if string(fields[i].Name) == "master_host" || string(fields[i].Name) == "source_host" {
			slavehost = string(row[i].AsString())
		} else if "Executed_Gtid_Set" == string(fields[i].Name) {
			gtid_title = "Executed_Gtid_Set"
			slavegtid = Remove_new_line(string(row[i].AsString()))
		} else if "Gtid_Slave_Pos" == string(fields[i].Name) || string("Gtid_source_Pos") == string(fields[i].Name) {
			gtid_title = string(fields[i].Name)
			slavegtid = Remove_new_line(string(row[i].AsString()))
		} else if ("connection_name" == string(fields[i].Name) || "Channel_Name" == string(fields[i].Name)) && len(row[i].AsString()) > 1 {
			channel_name = string(row[i].AsString())
		}
		replication_section_str += fmt.Sprintf("# %s = ", fields[i].Name)
		if fields[i].Type != mysql.MYSQL_TYPE_LONG && fields[i].Type != mysql.MYSQL_TYPE_LONGLONG && fields[i].Type != mysql.MYSQL_TYPE_INT24 && fields[i].Type != mysql.MYSQL_TYPE_SHORT {
			replication_section_str += fmt.Sprintf("'%s'\n", Remove_new_line(string(row[i].AsString())))
		} else {
			replication_section_str += fmt.Sprintf("%s\n", string(row[i].AsString()))
		}
	}
	if slavehost != "" {
		slave_count++
		if channel_name != "" {
			fmt.Fprintf(file, "[replication%s%s]", ".", channel_name)
		} else {
			fmt.Fprintf(file, "[replication%s%s]", "", "")
		}
		fmt.Fprintf(file, "\n# relay_master_log_file = '%s'\n# exec_master_log_pos = %s\n# %s = %s\n", slavelog, slavepos, gtid_title, slavegtid)
		fmt.Fprintf(file, "%s", replication_section_str)
		fmt.Fprintf(file, "# myloader_exec_reset_slave = 0 # 1 means execute the command\n# myloader_exec_change_master = 0 # 1 means execute the command\n# myloader_exec_start_slave = 0 # 1 means execute the command\n")
		log.Infof("Written slave status")
	}
	if slave_count > 1 {
		log.Warnf("Multisource replication found. Do not trust in the exec_master_log_pos as it might cause data inconsistencies. Search 'Replication and Transaction Inconsistencies' on MySQL Documentation")
	}
	file.Sync()
}

func StartDump() error {
	if ClearDumpDir {
		clear_dump_directory(dump_directory)
	} else if !DirtyDumpDir && !is_empty_dir(dump_directory) {
		log.Errorf("Directory is not empty (use --clear or --dirty): %s", dump_directory)
	}

	Check_num_threads()
	log.Infof("Using %d dumper threads", NumThreads)
	initialize_start_dump()
	initialize_common()

	Initialize_connection(MYDUMPER)
	initialize_masquerade()

	if TablesList != "" {
		Tables = Get_table_list(TablesList)
	}
	if TablesSkiplistFile != "" {
		_ = Read_tables_skiplist(TablesSkiplistFile)
	}
	InitializeRegex(PartitionRegex)
	var conn *DBConnection
	conn = create_main_connection()
	if conn.Err != nil {
		return conn.Err
	}
	Main_connection = conn
	var second_conn = conn
	var conf *configuration = new(configuration)
	conf.use_any_index = "1"
	var metadata_partial_filename, metadata_filename string
	var u string
	var acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function
	var dbt *DB_Table
	var n uint
	var nufile *os.File

	if DiskLimits != "" {
		conf.pause_resume = G_async_queue_new(BufferSize)
		disk_check_thread = G_thread_new("disk_space_monitor", new(sync.WaitGroup), 0)
		go monitor_disk_space_thread(conf.pause_resume)
	}
	if !DaemonMode {
		sthread = G_thread_new("signal_handler", new(sync.WaitGroup), 0)
		go signal_thread(conf)
		time.Sleep(10 * time.Microsecond)
		if sthread == nil {
			log.Critical("Could not create signal threads")
		}
	}
	if pmm {
		log.Infof("Using PMM resolution %s at %s", PmmResolution, PmmPath)
		pmmthread = G_thread_new("pmm_thread", new(sync.WaitGroup), 0)
		go pmm_thread(conf)
		if pmmthread == nil {
			log.Critical("Could not create pmm thread")
		}
	}
	if Stream != "" {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.header", dump_directory)
	} else {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", dump_directory)
	}
	metadata_filename = fmt.Sprintf("%s/metadata", dump_directory)

	mdfile, err := os.OpenFile(metadata_partial_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		log.Criticalf("Couldn't write metadata file %s (%v)", metadata_partial_filename, err)
	}
	if UpdatedSince > 0 {
		u = fmt.Sprintf("%s/not_updated_tables", dump_directory)
		nufile, err = os.OpenFile(u, os.O_CREATE|os.O_WRONLY, 0660)
		if err != nil {
			log.Criticalf("Couldn't write not_updated_tables file (%v)", err)
		}
		get_not_updated(conn, nufile)
	}

	if !NoLocks && Is_mysql_like() {
		// We check SHOW PROCESSLIST, and if there're queries
		// larger than preset value, we terminate the process.
		// This avoids stalling whole server with flush.
		long_query_wait(conn)
	}
	var datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Started dump at: %s\n", datetimestr)
	log.Infof("Started dump at: %s", datetimestr)

	/* Write dump config into beginning of metadata, stream this first */
	if Identifier_quote_character == BACKTICK || Identifier_quote_character == DOUBLE_QUOTE {
		var qc string
		if Identifier_quote_character == BACKTICK {
			qc = "BACKTICK"
		} else {
			qc = "DOUBLE_QUOTE"
		}
		fmt.Fprintf(mdfile, "[config]\nquote_character = %s\n", qc)
		fmt.Fprintf(mdfile, "\n[myloader_session_variables]")
		fmt.Fprintf(mdfile, "\nSQL_MODE=%s \n\n", Sql_mode)
		mdfile.Sync()
	} else {
		log.Criticalf("--identifier-quote-character not is %s or %s", BACKTICK, DOUBLE_QUOTE)
	}
	if Stream != "" {
		initialize_stream()
		stream_queue_push(nil, metadata_partial_filename)
		mdfile.Close()
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", dump_directory)
		mdfile, err = os.OpenFile(metadata_partial_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
		if err != nil {
			log.Criticalf("Couldn't create metadata file %s (%v)", metadata_partial_filename, err)
		}
	}
	if Detected_server == SERVER_TYPE_TIDB {
		log.Infof("Skipping locks because of TiDB")
		if TidbSnapshot == "" {

			// Generate a @@tidb_snapshot to use for the worker threads since
			// the tidb-snapshot argument was not specified when starting mydumper
			var res *mysql.Result
			M_query(conn, Show_binary_log_status, M_critical, "Couldn't generate @@tidb_snapshot")
			res = conn.Result
			TidbSnapshot = string(res.Values[0][1].AsString())
		}
		// Need to set the @@tidb_snapshot for the master thread
		set_tidb_snapshot(conn)
		log.Infof("Set to tidb_snapshot '%s'", TidbSnapshot)

	} else {
		if !NoLocks {
			// This backup will lock the database
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = determine_ddl_lock_function(second_conn)
			if SkipDdlLocks {
				acquire_ddl_lock_function = nil
				release_ddl_lock_function = nil
			}
			if LockAllTables {
				send_lock_all_tables(conn)
			} else {

				if acquire_ddl_lock_function != nil {
					log.Infof("Acquiring DDL lock")
					acquire_ddl_lock_function(second_conn)
				}

				if acquire_global_lock_function != nil {
					log.Infof("Acquiring Global lock")
					acquire_global_lock_function(conn)
				}
			}
		} else {
			log.Warnf("Executing in no-locks mode, snapshot might not be consistent")
		}
	}

	// TODO: this should be deleted on future releases.
	server_version = Mysql_get_server_version()
	if server_version < 40108 {
		conn.Execute("CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB")
		need_dummy_read = true
	}
	if Get_product() != SERVER_TYPE_MARIADB || server_version < 100300 {
		nroutines = 2
	}
	// tokudb do not support consistent snapshot
	conn.Execute("SELECT @@tokudb_version")
	rest := conn.Result
	if rest != nil {
		log.Infof("TokuDB detected, creating dummy table for CS")
		_ = conn.Execute("CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB")
		if err == nil {
			need_dummy_toku_read = true
		}
	}

	// Do not start a transaction when lock all tables instead of FTWRL,
	// since it can implicitly release read locks we hold
	// TODO: this should be deleted as main connection is not being used for export data
	//  if (!lock_all_tables) {
	//    g_message("Sending start transaction in main connection");
	//    mysql_query(conn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */");
	//  }

	if need_dummy_read {
		_ = conn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy")

	}
	if need_dummy_toku_read {
		_ = conn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy")

	}

	/*if o.Exec.Exec_command != "" {
		initialize_exec_command(o)
		o.Stream.Stream = true
	}*/

	conf.initial_queue = G_async_queue_new(BufferSize)
	conf.schema_queue = G_async_queue_new(BufferSize)
	conf.post_data_queue = G_async_queue_new(BufferSize)
	if conf.innodb == nil {
		conf.innodb = new(table_queuing)
		conf.non_innodb = new(table_queuing)
	}
	conf.innodb.queue = G_async_queue_new(BufferSize)
	conf.innodb.deferQueue = G_async_queue_new(BufferSize)
	if give_me_another_innodb_chunk_step_queue != nil && give_me_another_non_innodb_chunk_step_queue != nil &&
		innodb_table != nil && non_innodb_table != nil {
		log.Debugf("variables ok")
	} else {
		log.Critical("check variables fail")
	}
	conf.innodb.request_chunk = give_me_another_innodb_chunk_step_queue
	conf.innodb.table_list = innodb_table
	conf.innodb.descr = "InnoDB"
	conf.ready = G_async_queue_new(BufferSize)
	conf.non_innodb.queue = G_async_queue_new(BufferSize)
	conf.non_innodb.deferQueue = G_async_queue_new(BufferSize)
	conf.non_innodb.request_chunk = give_me_another_non_innodb_chunk_step_queue
	conf.non_innodb.table_list = non_innodb_table
	conf.non_innodb.descr = "Non-InnoDB"
	conf.ready_non_innodb_queue = G_async_queue_new(BufferSize)
	conf.unlock_tables = G_async_queue_new(BufferSize)
	conf.gtid_pos_checked = G_async_queue_new(BufferSize)
	conf.are_all_threads_in_same_pos = G_async_queue_new(BufferSize)
	conf.db_ready = G_async_queue_new(BufferSize)
	conf.binlog_ready = G_async_queue_new(BufferSize)
	//  ready_database_dump_mutex = g_rec_mutex_new();
	//  g_rec_mutex_lock(ready_database_dump_mutex);
	ready_table_dump_mutex = G_rec_mutex_new()
	ready_table_dump_mutex.Lock()

	log.Infof("conf created")

	if Is_mysql_like() {
		create_job_to_dump_metadata(conf, mdfile)
	}

	// Begin Job Creation

	if DumpTablespaces {
		create_job_to_dump_tablespaces(conf, dump_directory)
	}
	if len(Tables) > 0 {
		create_job_to_dump_table_list(Tables, conf)
	} else if len(db_items) > 0 {
		var i int
		for i = 0; i < len(db_items); i++ {
			var this_db *database = new_database(conn, db_items[i], true)
			create_job_to_dump_database(this_db, conf)
			if !NoSchemas {
				create_job_to_dump_schema(this_db, conf)
			}
		}
	} else {
		create_job_to_dump_all_databases(conf)
	}
	log.Infof("End job creation")

	if !NoData {
		chunk_builder = G_thread_new("ChunkBuilderThread", new(sync.WaitGroup), 0)
		go chunk_builder_thread(conf)
	}

	if LessLocking {
		td = make([]*thread_data, NumThreads*(1+1))
	} else {
		td = make([]*thread_data, NumThreads*(0+1))
	}

	for n = 0; n < NumThreads; n++ {
		td[n] = new(thread_data)
		td[n].conf = conf
		td[n].thread_id = n + 1
		td[n].less_locking_stage = false
		td[n].binlog_snapshot_gtid_executed = ""
		td[n].pause_resume_mutex = nil
		td[n].table_name = ""
		threads[n] = G_thread_new("WorkingThread", new(sync.WaitGroup), n)
		go working_thread(td[n], n)
	}
	// var binlog_snapshot_gtid_executed string
	var binlog_snapshot_gtid_executed_status_local bool
	var start_transaction_retry uint
	for !binlog_snapshot_gtid_executed_status_local && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
		binlog_snapshot_gtid_executed_status_local = true
		for n = 0; n < NumThreads; n++ {
			G_async_queue_pop(conf.gtid_pos_checked)
		}
		binlog_snapshot_gtid_executed = td[0].binlog_snapshot_gtid_executed
		for n = 1; n < NumThreads; n++ {
			binlog_snapshot_gtid_executed_status_local = binlog_snapshot_gtid_executed_status_local && strings.Compare(td[n].binlog_snapshot_gtid_executed, binlog_snapshot_gtid_executed) == 0
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
	for n = 0; n < NumThreads; n++ {
		G_async_queue_pop(conf.ready)
	}

	// IMPORTANT: At this point, all the threads are in sync

	if TrxConsistencyOnly {
		log.Infof("Transactions started, unlocking tables")
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}

		//    mysql_query(conn, "UNLOCK TABLES /* trx-only */");
		if release_binlog_function != nil {
			G_async_queue_pop(conf.binlog_ready)
			log.Infof("Releasing binlog lock")
			release_binlog_function(second_conn)
		}
		if replica_stopped {
			log.Infof("Starting replica")
			_ = conn.Execute(Start_replica_sql_thread)
			if err != nil {
				log.Warnf("Not able to start replica: %v", err)
			}
			if Source_control_command == AWS {
				Discard_mysql_output(conn)
			}
			replica_stopped = false
		}
	}

	log.Infof("Waiting database finish")
	G_async_queue_pop(conf.db_ready)
	no_updated_tables = nil
	for n = 0; n < NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.initial_queue, j)
	}

	for n = 0; n < NumThreads; n++ {
		G_async_queue_pop(conf.ready)
	}

	log.Infof("Shutdown jobs for less locking enqueued")
	for n = 0; n < NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.schema_queue, j)
	}

	if LessLocking {
		build_lock_tables_statement(conf)
	}

	for n = 0; n < NumThreads; n++ {
		G_async_queue_push(conf.ready_non_innodb_queue, 1)
	}

	if !NoLocks && !TrxConsistencyOnly {
		for n = 0; n < NumThreads; n++ {
			G_async_queue_pop(conf.unlock_tables)
		}
		log.Infof("Non-InnoDB dump complete, releasing global locks")
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}
		//    mysql_query(conn, "UNLOCK TABLES /* FTWRL */");
		log.Infof("Global locks released")
		if release_binlog_function != nil {
			G_async_queue_pop(conf.binlog_ready)
			log.Infof("Releasing binlog lock")
			release_binlog_function(second_conn)
		}
	}
	if replica_stopped {
		log.Infof("Starting replica")
		_ = conn.Execute(Start_replica_sql_thread)
		if err != nil {
			log.Warnf("Not able to start replica: %v", err)
		}
		if Source_control_command == AWS {
			Discard_mysql_output(conn)
		}
	}
	G_async_queue_unref(conf.binlog_ready)

	for n = 0; n < NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.post_data_queue, j)
	}

	if !NoData {
		chunk_builder.Thread.Wait()
	}

	log.Infof("Waiting threads to complete")
	for n = 0; n < NumThreads; n++ {
		threads[n].Thread.Wait()
	}
	finalize_working_thread()
	finalize_write()
	if release_ddl_lock_function != nil {
		log.Infof("Releasing DDL lock")
		release_ddl_lock_function(second_conn)
	}
	log.Infof("Queue count: %d %d %d %d %d", G_async_queue_length(conf.initial_queue), G_async_queue_length(conf.schema_queue),
		G_async_queue_length(conf.non_innodb.queue)+G_async_queue_length(conf.non_innodb.deferQueue),
		G_async_queue_length(conf.innodb.queue)+G_async_queue_length(conf.innodb.deferQueue),
		G_async_queue_length(conf.post_data_queue))
	// close main connection
	if conn != second_conn {
		second_conn.Close()
	}
	Execute_gstring(Main_connection, Set_global_back)
	conn.Close()
	log.Infof("Main connection closed")
	wait_close_files()

	for _, dbt = range all_dbts {
		print_dbt_on_metadata(mdfile, dbt)
	}
	write_database_on_disk(mdfile)
	if pmm {
		kill_pmm_thread()
	}
	G_async_queue_unref(conf.innodb.deferQueue)
	conf.innodb.descr = ""
	G_async_queue_unref(conf.innodb.queue)
	conf.innodb.queue = nil
	G_async_queue_unref(conf.non_innodb.deferQueue)
	conf.non_innodb.deferQueue = nil
	G_async_queue_unref(conf.non_innodb.queue)
	conf.non_innodb.queue = nil
	G_async_queue_unref(conf.unlock_tables)
	conf.unlock_tables = nil
	G_async_queue_unref(conf.ready)
	conf.ready = nil
	G_async_queue_unref(conf.schema_queue)
	conf.schema_queue = nil
	G_async_queue_unref(conf.initial_queue)
	conf.initial_queue = nil
	G_async_queue_unref(conf.post_data_queue)
	conf.post_data_queue = nil

	G_async_queue_unref(conf.ready_non_innodb_queue)
	conf.ready_non_innodb_queue = nil

	datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Finished dump at: %s\n", datetimestr)
	mdfile.Close()
	if UpdatedSince > 0 {
		nufile.Close()
	}
	os.Rename(metadata_partial_filename, metadata_filename)
	if Stream != "" {
		stream_queue_push(nil, metadata_filename)
	}
	log.Infof("Finished dump at: %s", datetimestr)
	if Stream != "" {
		stream_queue_push(nil, "")
		wait_stream_to_finish()
		if No_delete == false && OutputDirectoryParam == "" {
			err = os.RemoveAll(output_directory)
			if err != nil {
				log.Criticalf("Backup directory not removed: %s", output_directory)
			}
		}
	}

	free_databases()
	finalize_masquerade()
	Free_regex()
	free_common()
	finalize_masquerade()
	Free_set_names()
	if NoLocks {
		if it_is_a_consistent_backup {
			log.Infof("This is a consistent backup.")
		} else {
			log.Warnf("This is NOT a consistent backup.")
		}
	}
	return nil
}
