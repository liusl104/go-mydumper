package mydumper

import (
	"container/list"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"github.com/shirou/gopsutil/disk"
)

var (
	// SourceDataStr                         int
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
	UpdatedSince                       int
	DumpTablespaces                    bool
	threads                            []*GThread
	thd                                []*thread_data
	all_dbts                           map[string]*db_table
	conf_per_table                     *Configuration_per_table = new(Configuration_per_table)
	it_is_a_consistent_backup          bool
	no_updated_tables                  []string
	identifier_quote_character_protect func(string) string
	db_items                           []string
	table_schemas                      []*db_table
	pause_at                           uint
	resume_at                          uint
	pmm                                bool
	need_dummy_read                    bool
	need_dummy_toku_read               bool
	pause_mutex_per_thread             []*sync.Mutex
	disk_check_thread                  *GThread
	sthread                            *GThread
	pmmthread                          *GThread
	ready_table_dump_mutex             *sync.Mutex
	replica_stopped                    bool
	initial_source_log                 string
	initial_source_pos                 string
	initial_source_gtid                string
	ftwrl_completed                    bool
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
	JOB_WRITE_SOURCE_AND_REPLICA_STATUS
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
	UNSPLITTABLE
	COMPLETED
)

type Configuration struct {
	use_any_index                   string
	initial_queue                   *GAsyncQueue
	initial_completed_queue         *GAsyncQueue
	schema_queue                    *GAsyncQueue
	non_transactional               *table_queuing
	transactional                   *table_queuing
	post_data_queue                 *GAsyncQueue
	ready                           *GAsyncQueue
	ready_non_transactional_queue   *GAsyncQueue
	db_ready                        *GAsyncQueue
	source_and_replica_status_queue *GAsyncQueue
	unlock_tables                   *GAsyncQueue
	pause_resume                    *GAsyncQueue
	gtid_pos_checked                *GAsyncQueue
	are_all_threads_in_same_pos     *GAsyncQueue
	lock_tables_statement           *GString
	mutex                           *sync.Mutex
	loop                            *sync.WaitGroup
	done                            int
}
type MList struct {
	list  *list.List
	mutex *sync.Mutex
}

// NewMList creates a new MList (thread-safe list wrapper).
func NewMList() *MList {
	return &MList{list: list.New(), mutex: new(sync.Mutex)}
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

// new_int_types allocates int_types with empty unsigned_int and signed_int.
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
	rows_in_explain           uint64
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
	free     func(csi *chunk_step_item)
	get_next func(dbt *db_table) *chunk_step_item
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
	dbt             *db_table
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

type table_job_file struct {
	filename string
	file     *file_write
}

type table_job struct {
	partition            string
	part                 uint64
	sub_part             uint
	where                *GString
	chunk_step_item      *chunk_step_item
	dbt                  *db_table
	sql                  *table_job_file
	rows                 *table_job_file
	exec_out_filename    string
	filesize             float64
	st_in_file           uint
	child_process        int
	char_chunk_part      uint
	td                   *thread_data
	num_rows_of_last_run uint64
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

type db_table struct {
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
	is_transactional               bool
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
	columns_on_insert              string
	partition_regex                *regexp.Regexp
	num_threads                    uint
	chunks                         *list.List
	chunks_mutex                   *sync.Mutex
	chunks_queue                   *GAsyncQueue
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
	is_fixed_length                bool
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

// initialize_start_dump sets up all_dbts, table/working-thread state, and conf_per_table; applies lock mode and DB filter.
func initialize_start_dump() {
	all_dbts = make(map[string]*db_table)
	initialize_table()
	initialize_working_thread()
	Initialize_conf_per_table(conf_per_table)

	// until we have an unique option on lock int_types we need to ensure this
	if SyncThreadLockMode == NO_LOCK || SyncThreadLockMode == SAFE_NO_LOCK {
		TrxTables = 1
	}

	// clarify binlog coordinates with trx_consistency_only
	if TrxTables != 0 {
		log.Warnf("Using --trx-tables options, binlog coordinates will not be accurate if you are writing to non transactional tables.")
	}

	if DB != "" {
		db_items = strings.Split(DB, ",")
	}

	/*if o.Stream.Stream && o.Exec.Exec_command != "" {
		log.Fatalf("Stream and execute a command is not supported")
	} */
}

func set_disk_limits(p_at, r_at uint) {
	pause_at = p_at
	resume_at = r_at
}

// is_disk_space_ok returns true if free space on the dump_directory mount (in MB) is greater than val.
func is_disk_space_ok(val uint) bool {
	if !path.IsAbs(dump_directory) {
		pwd, _ := os.Getwd()
		dump_directory = path.Join(pwd, dump_directory)
	}
	partitions, err := disk.Partitions(true)
	if err != nil {
		log.Errorf("Error getting partitions: %s", err.Error())
	}
	// Find the mount partition for the given path
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
	// Get partition usage
	usage, err := disk.Usage(mountPoint)
	if err != nil {
		log.Criticalf("Error getting disk usage: %v", err)
	}
	return usage.Free/1024/1024 > uint64(val)
}

// monitor_disk_space_thread periodically checks disk space and pauses/resumes worker threads via pause_mutex_per_thread.
func monitor_disk_space_thread(c any) {
	queue := c.(*GAsyncQueue)
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

// determine_columns_on_show_processlist sets column indices for Id, User, Command, Time, Info from SHOW PROCESSLIST result.
func determine_columns_on_show_processlist(fields []*sql.ColumnType, num_fields uint, id_col *int, user_col *int, command_col *int, time_col *int, info_col *int) {
	var i int
	for i = 0; i < int(num_fields); i++ {
		if id_col != nil && strings.EqualFold(fields[i].Name(), "Id") {
			*id_col = i
		} else if user_col != nil && strings.EqualFold(fields[i].Name(), "User") {
			*user_col = i
		} else if command_col != nil && strings.EqualFold(fields[i].Name(), "Command") {
			*command_col = i
		} else if time_col != nil && strings.EqualFold(fields[i].Name(), "Time") {
			*time_col = i
		} else if info_col != nil && strings.EqualFold(fields[i].Name(), "Info") {
			*info_col = i
		}
	}
	if (id_col != nil && *id_col < 0) || (command_col != nil && *command_col < 0) || (time_col != nil && *time_col < 0) {
		log.Criticalf("Error obtaining information from processlist")
	}
}

// monitor_ftwrl_thread polls SHOW PROCESSLIST and kills the FTWRL/FLUSH NO WRITE query for the given thread_id if it blocks too long.
func monitor_ftwrl_thread(c any) {
	thread_id := c.(uint32)
	var conn *DBConnection
	var res *MYSQL_RES
	conn = Mysql_init()
	M_connect(conn)
	var query string
	for !ftwrl_completed {
		time.Sleep(time.Duration(ftwrl_max_wait_time) * time.Second)
		res = M_store_result(conn, "SHOW PROCESSLIST", M_warning, "Could not check PROCESSLIST")
		if res == nil {
			break
		} else {
			var row []FieldValue
			var id_col int = -1
			var info_col int = -1
			determine_columns_on_show_processlist(Mysql_fetch_fields(res), Mysql_num_fields(res), &id_col, nil, nil, nil, &info_col)
			for row = Mysql_fetch_row(res); row != nil; row = Mysql_fetch_row(res) {
				if row[id_col].AsInt64() == int64(thread_id) {
					if strings.EqualFold(row[info_col].String(), FLUSH_TABLES_WITH_READ_LOCK) || strings.EqualFold(row[info_col].String(), FLUSH_NO_WRITE_TO_BINLOG_TABLES) {
						query = fmt.Sprintf("KILL QUERY %d", row[id_col].AsInt64())
						M_query_warning(conn, query, "Could not KILL slow query")
					}
				}
			}
		}
		Mysql_free_result(res)
	}
	conn.Close()
}

// sig_triggered handles SIGTERM (sets shutdown_triggered) or SIGINT (prompts Y/N to cancel and optionally pauses workers); returns true to resume, false to exit.
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
		if user_data.(*Configuration).pause_resume == nil {
			user_data.(*Configuration).pause_resume = G_async_queue_new("pause_resume")
		}
		var queue = user_data.(*Configuration).pause_resume
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

// signal_thread runs the signal handler; waits for SIGINT/SIGTERM then calls sig_triggered.
func signal_thread(c any) {
	conf := c.(*Configuration)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	sig_triggered(conf, sig)
	log.Infof("Ending signal thread")
	return
}

// initialize_sql_mode normalizes Sql_mode (removes ORACLE) and stores it in set_session_hash["SQL_MODE"].
func initialize_sql_mode(set_session_hash map[string]string) {
	var str = Sql_mode
	str = strings.ReplaceAll(str, "ORACLE", "")
	str = strings.ReplaceAll(str, ",,", ",")
	set_session_hash["SQL_MODE"] = str
}

// mydumper_initialize_hash_of_session_variables returns the session variables hash with information_schema_stats_expiry and SQL_MODE for mydumper.
func mydumper_initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = Initialize_hash_of_session_variables()
	set_session_hash["information_schema_stats_expiry"] = "0 /*!80003"
	return set_session_hash
}

// create_connection creates a DB connection, runs Set_session, and returns it.
func create_connection() *DBConnection {
	var conn *DBConnection = Mysql_init()
	M_connect(conn)
	Execute_gstring(conn, Set_session)
	return conn
}

// detect_quote_character sets Identifier_quote_character, fields_enclosed_by, and identifier_quote_character_protect from ANSI_QUOTES.
func detect_quote_character(conn *DBConnection) {
	var query = "SELECT FIND_IN_SET('ANSI', @@SQL_MODE) OR FIND_IN_SET('ANSI_QUOTES', @@SQL_MODE)"
	res := M_store_result(conn, query, M_warning, "We were not able to determine ANSI mode")
	if conn.Err != nil {
		Identifier_quote_character = BACKTICK
		Identifier_quote_character_str = "`"
		fields_enclosed_by = "\""
		identifier_quote_character_protect = Backtick_protect
		return
	}
	row := Mysql_fetch_row(res)
	if row != nil && row[0].AsInt64() == 0 {
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

// detect_sql_mode reads @@SQL_MODE, normalizes it for dump (NO_AUTO_VALUE_ON_ZERO, removes several options), and sets global Sql_mode.
func detect_sql_mode(conn *DBConnection) {
	var query = "SELECT @@SQL_MODE"
	var mr *M_ROW = M_store_result_single_row(conn, query, "Error getting SQL_MODE")
	if mr.Res == nil || mr.Row == nil {
		M_store_result_row_free(mr)
		return
	}
	var str string

	if !strings.EqualFold(string(mr.Row[0].AsString()), "NO_AUTO_VALUE_ON_ZERO") {
		str = fmt.Sprintf("'NO_AUTO_VALUE_ON_ZERO,%s'", mr.Row[0].AsString())
	} else {
		str = fmt.Sprintf("'%s'", mr.Row[0].AsString())
	}
	str = strings.ReplaceAll(str, "NO_BACKSLASH_ESCAPES", "")
	str = strings.ReplaceAll(str, ",,", ",")
	/*
	   The below 4 will be returned back if there is ORACLE in SQL_MODE. We can
	   not remove ORACLE from dump files because restoring PACKAGE requires it. But we
	   may remove ORACLE from mydumper session because SHOW CREATE PACKAGE works
	   without ORACLE (see initialize_sql_mode()).
	   The dump must retain all table options, so we cut out NO_TABLE_OPTIONS here:
	   it doesn't play any role in dump files, but we are interested it doesn't
	   appear in mydumpmer session.
	*/
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
	M_store_result_row_free(mr)
}

// create_main_connection creates the main DB connection, detects server, loads session/global vars, sets headers/write, and returns the connection.
func create_main_connection() (conn *DBConnection) {
	conn = Mysql_init()
	M_connect(conn)
	if conn.Err != nil {
		log.Fatalf("Error connecting to database: %v", conn.Err)
	}
	Set_session = G_string_new("")
	Set_global = G_string_new("")
	Set_global_back = G_string_new("")
	Server_detect(conn)
	var set_session_hash = mydumper_initialize_hash_of_session_variables()
	var set_global_hash = make(map[string]string)
	if Key_file != nil {
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_global_hash, "mydumper_global_variables")
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_session_hash, "mydumper_session_variables")
		Load_per_table_info_from_key_file(Key_file, conf_per_table, init_function_pointer)
	}
	Sql_mode = set_session_hash["SQL_MODE"]
	if Sql_mode == "" {
		detect_sql_mode(conn)
		initialize_sql_mode(set_session_hash)
	}
	Refresh_set_session_from_hash(Set_session, set_session_hash)
	Refresh_set_global_from_hash(Set_global, Set_global_back, set_global_hash)
	Free_hash_table(set_session_hash)
	Execute_gstring(conn, Set_session)
	Execute_gstring(conn, Set_global)
	detect_quote_character(conn)
	initialize_headers()
	initialize_write()
	switch Get_product() {
	case SERVER_TYPE_MYSQL:
		set_transaction_isolation_level_repeatable_read(conn)
		break
	case SERVER_TYPE_MARIADB:
		set_transaction_isolation_level_repeatable_read(conn)
		break
	case SERVER_TYPE_TIDB:
		DataChecksums = false
		break
	case SERVER_TYPE_PERCONA:
		set_transaction_isolation_level_repeatable_read(conn)
		break
	case SERVER_TYPE_UNKNOWN:
		set_transaction_isolation_level_repeatable_read(conn)
		break
	case SERVER_TYPE_CLICKHOUSE:
		DataChecksums = false
		break
	case SERVER_TYPE_DOLT:
		set_transaction_isolation_level_repeatable_read(conn)
		break
	default:
		log.Criticalf("Cannot detect server type")
	}
	log.Infof("Connected to %s %d.%d.%d", Get_product_name(), Get_major(), Get_secondary(), Get_revision())
	return conn
}

// get_not_updated fills no_updated_tables with tables not updated in UpdatedSince days and writes them to file.
func get_not_updated(conn *DBConnection, file *os.File) {
	var query string
	var row []FieldValue
	query = fmt.Sprintf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND UPDATE_TIME < NOW() - INTERVAL '%d' DAY", UpdatedSince)
	var res = M_store_result(conn, query, M_warning, "Updated since query failed")
	if res == nil {
		return
	}
	for {
		row = Mysql_fetch_row(res)
		if row == nil {
			break
		}
		no_updated_tables = append(no_updated_tables, string(row[0].AsString()))
		_, _ = file.WriteString(fmt.Sprintf("%s\n", row[0].AsString()))
	}
	slices.Sort(no_updated_tables)
	_ = file.Sync()
	return
}

// long_query_wait waits until no queries in SHOW PROCESSLIST exceed Longquery seconds; optionally kills them (Killqueries) or retries.
func long_query_wait(conn *DBConnection) {
	var p3 string
	for {
		var longquery_count int
		res := M_store_result(conn, "SHOW PROCESSLIST", M_warning, "Could not check PROCESSLIST, no long query guard enabled")
		if res == nil {
			break
		} else {
			var row []FieldValue
			/* Just in case PROCESSLIST output column order changes */
			var tcol = -1
			var ccol = -1
			var icol = -1
			var ucol = -1
			determine_columns_on_show_processlist(Mysql_fetch_fields(res), Mysql_num_fields(res), &icol, &ucol, &ccol, &tcol, nil)
			for row = Mysql_fetch_row(res); row != nil; row = Mysql_fetch_row(res) {
				if row[ccol].Value() != nil && row[ccol].String() != "Query" {
					continue
				}
				if row[ucol].Value() != nil && (row[ucol].String() == "system user" || row[ucol].String() == "event_scheduler") {
					continue
				}
				if row[tcol].Value() != nil && row[tcol].AsUint64() > Longquery {
					if Killqueries {
						p3 = fmt.Sprintf("KILL %d", row[icol].AsUint64())
						if M_query_warning(conn, p3, "Could not KILL slow query") {
							longquery_count++
						} else {
							log.Warnf("Killed a query that was running for %ds", row[tcol].AsUint64())
						}
					} else {
						longquery_count++
					}
				}
			}
			Mysql_free_result(res)
			if longquery_count == 0 {
				break
			} else {
				if LongqueryRetries == 0 {
					log.Criticalf("There are queries in PROCESSLIST running longer than %ds, aborting dump,\n\t use --long-query-guard to change the guard value, kill queries (--kill-long-queries) or use \n\tdifferent server for dump", Longquery)
				}
				LongqueryRetries--
				log.Warnf("There are queries in PROCESSLIST running longer than %ds, retrying in %d seconds (%d left).", Longquery, LongqueryRetryInterval, LongqueryRetries)
				time.Sleep(time.Duration(LongqueryRetryInterval) * time.Second)
			}
		}
	}
}

// send_backup_stage_on_block_commit sends BACKUP STAGE BLOCK_COMMIT on the connection.
func send_backup_stage_on_block_commit(conn *DBConnection) {
	M_query_verbose(conn, "BACKUP STAGE BLOCK_COMMIT", M_critical, "Could not send BACKUP STAGE BLOCK_COMMIT")
}

// send_mariadb_backup_locks sends BACKUP STAGE START and BLOCK_DDL (MariaDB backup locks).
func send_mariadb_backup_locks(conn *DBConnection) {
	M_query_verbose(conn, "BACKUP STAGE START", M_critical, "Couldn't acquire BACKUP STAGE START")
	M_query_verbose(conn, "BACKUP STAGE BLOCK_DDL", M_critical, "Couldn't acquire BACKUP STAGE BLOCK_DDL")
}

// send_percona57_backup_locks sends LOCK TABLES FOR BACKUP and LOCK BINLOG FOR BACKUP (Percona 5.7).
func send_percona57_backup_locks(conn *DBConnection) {
	M_query_verbose(conn, "LOCK TABLES FOR BACKUP", M_critical, "Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will not be consistent")
	M_query_verbose(conn, "LOCK BINLOG FOR BACKUP", M_critical, "Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will not be consistent")
}

// send_ddl_lock_instance_backup sends LOCK INSTANCE FOR BACKUP (MySQL 8 / Percona 8).
func send_ddl_lock_instance_backup(conn *DBConnection) {
	M_query_verbose(conn, "LOCK INSTANCE FOR BACKUP", M_critical, "Couldn't acquire LOCK INSTANCE FOR BACKUP")
}

// send_unlock_tables sends UNLOCK TABLES on the connection.
func send_unlock_tables(conn *DBConnection) {
	M_query_verbose(conn, "UNLOCK TABLES", M_warning, "Failed to UNLOCK TABLES")
}

// send_unlock_binlogs sends UNLOCK BINLOG on the connection.
func send_unlock_binlogs(conn *DBConnection) {
	M_query_verbose(conn, "UNLOCK BINLOG", M_warning, "Failed to UNLOCK BINLOG")
}

// send_ddl_unlock_instance_backup sends UNLOCK INSTANCE on the connection.
func send_ddl_unlock_instance_backup(conn *DBConnection) {
	M_query_verbose(conn, "UNLOCK INSTANCE", M_warning, "Failed to UNLOCK INSTANCE")
}

// send_backup_stage_end sends BACKUP STAGE END on the connection.
func send_backup_stage_end(conn *DBConnection) {
	M_query_verbose(conn, "BACKUP STAGE END", M_warning, "Failed to BACKUP STAGE END")

}

// send_flush_table_with_read_lock runs FLUSH TABLES WITH READ LOCK (and FLUSH NO WRITE TO BINLOG) with retries and starts the FTWRL monitor thread.
func send_flush_table_with_read_lock(conn *DBConnection) {
	var id = conn.GetConnectionID()
	M_thread_new("mon_ftwrl", monitor_ftwrl_thread, id, "FTWRL monitor thread could not be created")
	var i = 0
try_FLUSH_NO_WRITE_TO_BINLOG_TABLES:
	i++
	if M_query_verbose(conn, FLUSH_NO_WRITE_TO_BINLOG_TABLES, M_warning, "Flush tables failed, we are continuing anyways") &&
		(ftwrl_timeout_retries == 0 || (i < ftwrl_timeout_retries)) {
		goto try_FLUSH_NO_WRITE_TO_BINLOG_TABLES
	}
try_FLUSH_TABLES_WITH_READ_LOCK:
	if M_query_verbose(conn, FLUSH_TABLES_WITH_READ_LOCK, M_critical, "Couldn't acquire global lock, snapshots will not be consistent") &&
		(ftwrl_timeout_retries == 0 || (i < ftwrl_timeout_retries)) {
		goto try_FLUSH_TABLES_WITH_READ_LOCK
	}
	ftwrl_completed = true

}

// initialize_tidb_snapshot sets TidbSnapshot from binary log status if not set, then sets @@tidb_snapshot on the connection.
func initialize_tidb_snapshot(conn *DBConnection) {
	if TidbSnapshot != "" {
		// Generate a @@tidb_snapshot to use for the worker threads since
		// the tidb-snapshot argument was not specified when starting mydumper
		var mr *M_ROW = M_store_result_row(conn, Show_binary_log_status, M_critical, M_warning, "Couldn't generate @@tidb_snapshot")
		TidbSnapshot = string(mr.Row[1].AsString())
		M_store_result_row_free(mr)
	}
	// Need to set the @@tidb_snapshot for the master thread
	set_tidb_snapshot(conn)
	log.Infof("Set to tidb_snapshot '%s'", TidbSnapshot)
}

// default_locking returns the default lock functions: FTWRL for acquire, UNLOCK TABLES for release, no DDL/binlog lock.
func default_locking() (acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function *lock_function) {
	*acquire_ddl_lock_function = nil
	*release_ddl_lock_function = nil
	*acquire_global_lock_function = send_flush_table_with_read_lock
	*release_global_lock_function = send_unlock_tables
	*release_binlog_function = nil
	return
}

// determine_ddl_lock_function sets lock functions based on server type and version (Percona, MySQL, MariaDB, TiDB, etc.).
func determine_ddl_lock_function(conn **DBConnection, acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function *lock_function) {
	switch Get_product() {
	case SERVER_TYPE_PERCONA:
		switch Get_major() {
		case 8:
			*acquire_ddl_lock_function = send_ddl_lock_instance_backup
			*release_ddl_lock_function = send_ddl_unlock_instance_backup
			*acquire_global_lock_function = send_flush_table_with_read_lock
			*release_global_lock_function = send_unlock_tables
			break
		case 5:
			if Get_secondary() == 7 {
				if NoBackupLocks {
					*acquire_ddl_lock_function = nil
					*release_ddl_lock_function = nil
				} else {
					*acquire_ddl_lock_function = send_percona57_backup_locks
					*release_ddl_lock_function = send_unlock_tables
				}
				*acquire_global_lock_function = send_flush_table_with_read_lock
				*release_global_lock_function = send_unlock_tables

				*release_binlog_function = send_unlock_binlogs
				*conn = create_connection()
			} else {
				acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
			}
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
		break
	case SERVER_TYPE_MYSQL:
		switch Get_major() {
		case 8:
			*acquire_ddl_lock_function = send_ddl_lock_instance_backup
			*release_ddl_lock_function = send_ddl_unlock_instance_backup
			*acquire_global_lock_function = send_flush_table_with_read_lock
			*release_global_lock_function = send_unlock_tables
			break
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
		break
	case SERVER_TYPE_MARIADB:
		if (Get_major() == 10 && Get_secondary() >= 5) || Get_major() > 10 {
			*acquire_ddl_lock_function = send_mariadb_backup_locks
			*release_ddl_lock_function = nil
			*acquire_global_lock_function = send_backup_stage_on_block_commit
			*release_global_lock_function = send_backup_stage_end
		} else {
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
		break
	case SERVER_TYPE_TIDB:
		*acquire_global_lock_function = initialize_tidb_snapshot
		break
	default:
		acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		break
	}
	return
}

// print_dbt_on_metadata_gstring appends [key], rows, checksums, and is_sequence to data for the table metadata.
func print_dbt_on_metadata_gstring(dbt *db_table, data *GString) {
	var name string = Newline_protect(dbt.database.name)
	var table_filename = Newline_protect(dbt.table_filename)
	var table = Newline_protect(dbt.table)
	dbt.chunks_mutex.Lock()
	G_string_append_printf(data, "\n[%s]\n", dbt.key)
	G_string_append_printf(data, "real_table_name=%s\nrows = %d\n", table, dbt.rows)
	_ = name
	_ = table_filename
	if dbt.is_sequence {
		G_string_append_printf(data, "is_sequence = 1\n")
	}
	if dbt.data_checksum != "" {
		G_string_append_printf(data, "data_checksum = %s\n", dbt.data_checksum)
	}
	if dbt.schema_checksum != "" {
		G_string_append_printf(data, "schema_checksum = %s\n", dbt.schema_checksum)
	}
	if dbt.indexes_checksum != "" {
		G_string_append_printf(data, "indexes_checksum = %s\n", dbt.indexes_checksum)
	}
	if dbt.triggers_checksum != "" {
		G_string_append_printf(data, "triggers_checksum = %s\n", dbt.triggers_checksum)
	}
	dbt.chunks_mutex.Unlock()
}

// print_dbt_on_metadata writes table metadata (key, rows, checksums) to mdfile and optionally checks row count.
func print_dbt_on_metadata(mdfile *os.File, dbt *db_table) {
	var data *GString = G_string_sized_new(100)
	print_dbt_on_metadata_gstring(dbt, data)
	fmt.Fprintf(mdfile, data.Str.String())
	// mdfile.Sync()
	if CheckRowCount && !dbt.object_to_export.No_data && (dbt.rows != dbt.rows_total) {
		log.Criticalf("Row count mismatch found for %s.%s: got %d of %d expected", dbt.database.name, dbt.table, dbt.rows, dbt.rows_total)
	}

}

// send_lock_all_tables builds the list of tables (from Tables or information_schema), then executes LOCK TABLE ... READ with retries.
func send_lock_all_tables(conn *DBConnection) {
	// LOCK ALL TABLES
	var query string
	var dbtb string
	var dt []string
	var row []FieldValue
	var res *MYSQL_RES
	var tables_lock []string
	var success bool
	var retry uint
	var i uint = 0

	if len(Tables) > 0 {
		for _, t := range Tables {
			dt = strings.Split(t, ".")
			query = fmt.Sprintf("SHOW TABLES IN %s LIKE '%s'", dt[0], dt[1])
			res = M_store_result_critical(conn, query, "Error showing tables in: %s - Could not execute query", dt[0])
			if res != nil {
				for {
					row = Mysql_fetch_row(res)
					if row == nil {
						break
					}
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
		res = M_store_result_critical(conn, query, "Couldn't get table list for lock all tables")
		if res != nil {
			for {
				row = Mysql_fetch_row(res)
				if row == nil {
					break
				}
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
		// Try three times to get the lock, this is in case of tmp tables
		// disappearing
		log.Infof("Initialing Lock All tables")
		for len(tables_lock) > 0 && !success && retry < 4 {
			query = ""
			query += "LOCK TABLE"
			for _, iter := range tables_lock {
				query += fmt.Sprintf("%s READ,", iter)
			}
			query = strings.Trim(query, ",")
			if M_query_warning(conn, query, "Lock Table failed") {
				var tmp_fail []string = strings.Split(Mysql_error(conn), "'")
				tmp_fail = strings.Split(tmp_fail[1], ".")
				var failed_table string = fmt.Sprintf("`%s`.`%s`", tmp_fail[0], tmp_fail[1])
				var tmp_list []string
				for _, t := range tables_lock {
					// tables_lock = g_list_remove(tables_lock, iter.data);
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
	Mysql_free_result(res)
}

// m_stop_replica stops the replica SQL thread (or all replicas if multisource) and sets replica_stopped.
func m_stop_replica(conn *DBConnection) {
	var slave *MYSQL_RES
	var rest *MYSQL_RES
	if Get_product() == SERVER_TYPE_MARIADB {
		rest = M_store_result(conn, "SELECT @@default_master_connection", M_warning, "Variable @@default_master_connection not found")
		if rest != nil && Mysql_num_rows(rest) != 0 {
			Mysql_free_result(rest)
			log.Infof("Multisource slave detected.")
			isms = true
		}
	}

	if isms {
		M_query_critical(conn, Show_all_replicas_status, "Error executing %s", Show_all_replicas_status)
	} else {
		M_query_critical(conn, Show_replica_status, "Error executing %s", Show_replica_status)
	}
	slave = Mysql_store_result(conn)

	if slave == nil || Mysql_num_rows(slave) == 0 {
		goto cleanup
	}
	log.Infof("Stopping replica")
	replica_stopped = !M_query_warning(conn, Stop_replica_sql_thread, "Not able to stop replica")
	if Source_control_command == AWS {
		Discard_mysql_output(conn)
	}

cleanup:
	if slave != nil {
		Mysql_free_result(slave)
	}
}

// StartDump runs the full dump: creates directories, connections, acquires locks, builds job queues, and processes jobs until completion.
func StartDump(conf *Configuration) error {
	var conn, second_conn *DBConnection
	var metadata_partial_filename, metadata_filename string
	var u string
	var acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function
	var dbt *db_table
	var n uint
	var err error
	var nufile *os.File
	var mdfile *os.File
	// var disk_check_thread *GThread
	// var sthread *GThread

	if ClearDumpDir {
		clear_dump_directory(dump_directory)
	} else if !(DirtyDumpDir || MergeDumpDir) && !is_empty_dir(dump_directory) {
		log.Errorf("Directory is not empty (use --clear, --dirty or --merge): %s", dump_directory)
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
		Read_tables_skiplist(TablesSkiplistFile, &Errors)
	}
	InitializeRegex(PartitionRegex)

	conn = create_main_connection()
	Main_connection = conn
	second_conn = conn
	conf.use_any_index = "1"

	if DiskLimits != "" {
		conf.pause_resume = G_async_queue_new("conf.pause_resume")
		disk_check_thread = M_thread_new("mon_disk", monitor_disk_space_thread, conf.pause_resume, "Monitor thread could not be created")
	}
	if Throttle_variable != "" {
		M_thread_new("mon_thro", Monitor_throttling_thread, nil, "Monitor throttling thread could not be created")
	}
	if !DaemonMode {
		sthread = M_thread_new("signal", signal_thread, conf, "Signal thread could not be created")
	}
	if Stream != "" {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.header", dump_directory)
	} else {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", dump_directory)
	}
	metadata_filename = fmt.Sprintf("%s/metadata", dump_directory)
	if MergeDumpDir {
		if err := os.Rename(metadata_filename, metadata_partial_filename); err != nil {
			log.Criticalf("We were not able to rename metadata (%s) file to %s", metadata_filename, metadata_partial_filename)
		}
	}
	mdfile, err = os.OpenFile(metadata_partial_filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		log.Criticalf("Couldn't create metadata file %s (%v)", metadata_partial_filename, err)
	}
	if UpdatedSince > 0 {
		u = fmt.Sprintf("%s/not_updated_tables", dump_directory)
		nufile, err = os.OpenFile(u, os.O_CREATE|os.O_WRONLY, 0660)
		if err != nil {
			log.Criticalf("Couldn't write not_updated_tables file (%v)", err)
		}
		get_not_updated(conn, nufile)
	}

	// If we are locking, we need to be sure there is no long running queries
	if SyncThreadLockMode != NO_LOCK && SyncThreadLockMode != SAFE_NO_LOCK && Is_mysql_like() {
		// We check SHOW PROCESSLIST, and if there're queries
		// larger than preset value, we terminate the process.
		// This avoids stalling whole server with flush.
		long_query_wait(conn)
	}
	var datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Started dump at: %s\n", datetimestr)
	log.Infof("Started dump at: %s", datetimestr)
	G_assert(Identifier_quote_character == BACKTICK || Identifier_quote_character == DOUBLE_QUOTE)
	/* Write dump config into beginning of metadata, stream this first */
	var qc string
	if Identifier_quote_character == BACKTICK {
		qc = "BACKTICK"
	} else {
		qc = "DOUBLE_QUOTE"
	}
	fmt.Fprintf(mdfile, "[config]\nquote-character = %s\n", qc)
	if LoadData || Csv {
		fmt.Fprintf(mdfile, "local-infile = 1\n")
	}
	fmt.Fprintf(mdfile, "\n[myloader_session_variables]")
	fmt.Fprintf(mdfile, "\nSQL_MODE=%s /*!40101\n", Sql_mode)
	mdfile.Sync()

	if Stream != "" {
		if Exec_command != "" {
			log.Errorf("--exec and --stream are not comptabile, use --exec-per-thread instead as file extension is needed to stream the out file")
		}
		initialize_stream()
		mdfile.Close()
		stream_queue_push(nil, metadata_partial_filename)
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", dump_directory)
		mdfile, err = os.OpenFile(metadata_partial_filename, os.O_WRONLY, 0660)
		if err != nil {
			log.Criticalf("Couldn't create metadata file %s (%v)", metadata_partial_filename, err)
		}
	}
	// Initializing exec command
	if Exec_command != "" {
		initialize_exec_command()
	}
	// Write replica information
	if Get_product() != SERVER_TYPE_TIDB {
		if ReplicaData.Enabled {
			m_stop_replica(conn)
		}
	}
	// Determine the locking mechanisim that is going to be used
	// and send locks to database if needed
	switch SyncThreadLockMode {
	case NO_LOCK:
		log.Infof("Executing in NO_LOCK mode, we are not able to ensure that backup will be consistent")
		break
	case SAFE_NO_LOCK:
		log.Infof("Executing in SAFE_NO_LOCK mode. This backup will fail if all threads are not in the same point in time, which garanty consitency")
		break
	case LOCK_ALL:
		send_lock_all_tables(conn)
		break
	case AUTO:
		determine_ddl_lock_function(&second_conn, &acquire_global_lock_function, &release_global_lock_function, &acquire_ddl_lock_function, &release_ddl_lock_function, &release_binlog_function)
		break
	case FTWRL:
		determine_ddl_lock_function(&second_conn, &acquire_global_lock_function, &release_global_lock_function, &acquire_ddl_lock_function, &release_ddl_lock_function, &release_binlog_function)
		acquire_global_lock_function = send_flush_table_with_read_lock
		release_global_lock_function = send_unlock_tables
		break
	case GTID:
		log.Infof("Using binlog_snapshot_gtid_executed which doesn't lock the database but uses best effort to sync the threads")
		break
	}
	if SkipDdlLocks {
		acquire_ddl_lock_function = nil
		release_ddl_lock_function = nil
	}
	if acquire_ddl_lock_function != nil {
		log.Infof("Acquiring DDL lock")
		acquire_ddl_lock_function(second_conn)
	}
	if acquire_global_lock_function != nil {
		log.Infof("Acquiring Global lock")
		acquire_global_lock_function(conn)
	}
	// TODO: this should be deleted on future releases.
	server_version = Mysql_get_server_version()
	if server_version < 40108 {
		M_query_warning(conn, "CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB", "Not able to create dummy table for InnoDB")
		need_dummy_read = true
	}
	if Get_product() != SERVER_TYPE_MARIADB || server_version < 100300 {
		nroutines = 2
	}

	// tokudb do not support consistent snapshot
	var rest *MYSQL_RES = M_store_result(conn, "SELECT @@tokudb_version", M_message, "@@tokudb_version not found")
	if rest != nil {
		if Mysql_num_rows(rest) != 0 {
			Mysql_free_result(rest)
			log.Infof("TokuDB detected, creating dummy table for CS")
			M_query_warning(conn, "CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB", "Not able to create dummy table for TokuDB")
			need_dummy_toku_read = true
		}
		Mysql_free_result(rest)
	}

	if need_dummy_read {
		rest = M_store_result(conn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy", M_warning, "Select on mysql.mydumperdummy has failed")
		if rest != nil {
			Mysql_free_result(rest)
		}

	}
	if need_dummy_toku_read {
		rest = M_store_result(conn, "SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy", M_warning, "Select on mysql.tokudbdummy has failed")
		if rest != nil {
			Mysql_free_result(rest)
		}
	}
	log.Tracef("Initilizing the Configuration")

	conf.initial_queue = G_async_queue_new("conf.initial_queue")
	conf.initial_completed_queue = G_async_queue_new("conf.initial_completed_queue")
	conf.schema_queue = G_async_queue_new("conf.schema_queue")
	conf.post_data_queue = G_async_queue_new("conf.post_data_queue")
	if conf.transactional == nil {
		conf.transactional = new(table_queuing)
		conf.non_transactional = new(table_queuing)
	}
	conf.transactional.queue = G_async_queue_new("conf.transactional.queue")
	conf.transactional.deferQueue = G_async_queue_new("conf.transactional.deferQueue")
	// These are initialized in the guts of initialize_start_dump() above
	G_assert(give_me_another_transactional_chunk_step_queue != nil && give_me_another_non_transactional_chunk_step_queue != nil && transactional_table != nil && non_transactional_table != nil)
	conf.transactional.request_chunk = give_me_another_transactional_chunk_step_queue
	conf.transactional.table_list = transactional_table
	conf.transactional.descr = "transactional"
	conf.ready = G_async_queue_new("conf.ready")
	conf.non_transactional.queue = G_async_queue_new("conf.non_transactional.queue")
	conf.non_transactional.deferQueue = G_async_queue_new("conf.non_transactional.deferQueue")
	conf.non_transactional.request_chunk = give_me_another_non_transactional_chunk_step_queue
	conf.non_transactional.table_list = non_transactional_table
	conf.non_transactional.descr = "non-transactional"
	conf.ready_non_transactional_queue = G_async_queue_new("conf.ready_non_transactional_queue")
	conf.unlock_tables = G_async_queue_new("conf.unlock_tables")
	conf.gtid_pos_checked = G_async_queue_new("conf.gtid_pos_checked")
	conf.are_all_threads_in_same_pos = G_async_queue_new("conf.are_all_threads_in_same_pos")
	conf.db_ready = G_async_queue_new("conf.db_ready")
	conf.source_and_replica_status_queue = G_async_queue_new("conf.source_and_replica_status_queue")
	//  ready_database_dump_mutex = g_rec_mutex_new();
	//  g_rec_mutex_lock(ready_database_dump_mutex);
	ready_table_dump_mutex = G_rec_mutex_new()
	ready_table_dump_mutex.Lock()

	log.Tracef("Begin Job Creation")

	if Is_mysql_like() {
		create_job_to_write_source_and_replica_status(conf, mdfile)
	} else {
		G_async_queue_push(conf.source_and_replica_status_queue, 1)
	}
	log.Tracef("Create tablespace jobs")
	// Begin Job Creation
	if DumpTablespaces {
		create_job_to_dump_tablespaces(conf)
	}

	if Tables != nil && len(Tables) > 0 {
		log.Tracef("Specific tables")
		create_job_to_dump_table_list(Tables, conf)
	} else if db_items != nil && len(db_items) > 0 {
		log.Tracef("Specific databases")
		var i int
		for i = 0; i < len(db_items); i++ {
			var this_db *database = new_database(conn, db_items[i], true)
			create_job_to_dump_database(this_db, conf)
			if !NoSchemas {
				create_job_to_dump_schema(this_db, conf)
			}
		}
	} else {
		log.Tracef("All databases")
		create_job_to_dump_all_databases(conf)
	}
	log.Infof("End job creation")
	start_chunk_builder(conf)
	start_working_thread(conf)
	G_async_queue_pop(conf.source_and_replica_status_queue)
	G_async_queue_unref(conf.source_and_replica_status_queue)
	var source_log, source_pos, source_gtid string
	get_binlog_position(conn, &source_log, &source_pos, &source_gtid)
	if strings.EqualFold(source_log, initial_source_log) ||
		strings.EqualFold(source_pos, initial_source_pos) ||
		strings.EqualFold(source_gtid, initial_source_gtid) {
		if SyncThreadLockMode == NO_LOCK {
			log.Warnf("There are differences in the binlog position at the beginning of the backup and after syncing threads, so we cannot guarantee the backup to be consistent due to the use of NO_LOCK. Continues anyway, use SAFE_NO_LOCK otherwise.")
			log.Tracef("Backup will be inconsistent %s %s %d || %s %s %d || %s %s %d", source_log, initial_source_log, strings.Compare(source_log, initial_source_log), source_pos, initial_source_pos, strings.Compare(source_pos, initial_source_pos), source_gtid, initial_source_gtid, strings.Compare(source_gtid, initial_source_gtid))
		} else if SyncThreadLockMode == SAFE_NO_LOCK {
			log.Debugf("Backup will be inconsistent %s %s %d || %s %s %d || %s %s %d", source_log, initial_source_log, strings.Compare(source_log, initial_source_log), source_pos, initial_source_pos, strings.Compare(source_pos, initial_source_pos), source_gtid, initial_source_gtid, strings.Compare(source_gtid, initial_source_gtid))
			log.Errorf("There are differences in the binlog position at the beginning of the backup and after syncing threads, so we cannot guarantee the backup to be consistent. Stopping backup due to the use of SAFE_NO_LOCK.")
		}
	} else {
		log.Infof("Backup will be consistent")
	}
	// IMPORTANT: At this point, all the threads are in sync
	if TrxTables != 0 {
		// Releasing locks as user instructed that all tables are transactional
		log.Infof("Transactions started, unlocking tables")
		if release_binlog_function != nil {
			log.Infof("Releasing binlog lock")
			release_binlog_function(second_conn)
		}
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}
		if Is_mysql_like() && replica_stopped {
			log.Infof("Starting replica")
			M_query_warning(conn, Start_replica_sql_thread, "Not able to start replica")

			if Source_control_command == AWS {
				Discard_mysql_output(conn)
			}
			replica_stopped = false
		}
	}

	// Every time a schema job is created a counter increases
	// Every time that a schema jobs is completed, the counter decreases
	// When the counter reaches to 0, it releases conf.db_ready
	log.Infof("Waiting database finish")
	G_async_queue_pop(conf.db_ready)
	// At this point all schema jobs are completed
	no_updated_tables = nil
	// We let working threads know that initial_queue has been completed
	// sending them a JOB_SHUTDOWN job.
	for n = 0; n < NumThreads; n++ {
		var j *job = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.initial_queue, j)
	}
	for n = 0; n < NumThreads; n++ {
		G_async_queue_pop(conf.initial_completed_queue)
	}
	// at this point initial jobs has been completed
	// which means that all schema jobs has been created
	// we are able to send the JOB_SHUTDOWN to schema_queue
	log.Infof("Shutdown schema jobs")
	for n = 0; n < NumThreads; n++ {
		var j *job = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.schema_queue, j)
	}
	// In case that we are NOT exporting transactional table, we need to
	// build the lock table statement, at this stage, before
	// let workers to start dumping data
	if TrxTables == 0 {
		build_lock_tables_statement(conf)
	}
	// Allowing workers to start dumping Non-Transactional tables
	for n = 0; n < NumThreads; n++ {
		G_async_queue_push(conf.ready_non_transactional_queue, 1)
	}
	// Releasing locks if possible
	if SyncThreadLockMode != NO_LOCK && SyncThreadLockMode != SAFE_NO_LOCK && TrxTables == 0 {
		for n = 0; n < NumThreads; n++ {
			G_async_queue_pop(conf.unlock_tables)
		}
		if release_binlog_function != nil {
			log.Infof("Releasing binlog lock")
			release_binlog_function(second_conn)
		}
		log.Infof("Non-InnoDB dump complete, releasing global locks")
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}

		log.Infof("Global locks released")
	}

	// At this point, we can start the replica if it was stopped
	if Is_mysql_like() && replica_stopped {
		log.Infof("Starting replica")
		M_query_warning(conn, Start_replica_sql_thread, "Not able to start replica")

		if Source_control_command == AWS {
			Discard_mysql_output(conn)
		}
	}

	// All the jobs related to post data has been created and enquequed
	// so, we can send the JOB_SHUTDOWN
	for n = 0; n < NumThreads; n++ {
		var j *job = new(job)
		j.types = JOB_SHUTDOWN
		G_async_queue_push(conf.post_data_queue, j)
	}
	// At this point the main process, needs to wait the working threads to finish
	wait_working_thread_to_finish()
	// Backup is done
	// Starting to finalize it
	finalize_working_thread()
	finalize_chunk()
	finalize_write()
	// Releasing DDL lock if possible
	if release_ddl_lock_function != nil {
		log.Infof("Releasing DDL lock")
		release_ddl_lock_function(second_conn)
	}
	log.Infof("Queue count: %d %d %d %d %d", G_async_queue_length(conf.initial_queue),
		G_async_queue_length(conf.schema_queue),
		G_async_queue_length(conf.non_transactional.queue)+G_async_queue_length(conf.non_transactional.deferQueue),
		G_async_queue_length(conf.transactional.queue)+G_async_queue_length(conf.transactional.deferQueue),
		G_async_queue_length(conf.post_data_queue))
	// close main connection
	if conn != second_conn {
		second_conn.Close()
	}
	Execute_gstring(Main_connection, Set_global_back)
	conn.Close()
	log.Infof("Main connection closed")
	wait_close_files()
	var keys = maps.Keys(all_dbts)
	for _, key := range slices.Sorted(keys) {
		dbt = all_dbts[key]
		G_assert(dbt != nil)
		print_dbt_on_metadata(mdfile, dbt)
	}
	// There are scenarios where we need to wait files to flush to disk
	write_database_on_disk(mdfile)

	table_schemas = nil
	G_async_queue_unref(conf.transactional.deferQueue)
	conf.transactional.descr = ""
	G_async_queue_unref(conf.transactional.queue)
	conf.transactional.queue = nil
	G_async_queue_unref(conf.non_transactional.deferQueue)
	conf.non_transactional.deferQueue = nil
	G_async_queue_unref(conf.non_transactional.queue)
	conf.non_transactional.queue = nil
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

	G_async_queue_unref(conf.ready_non_transactional_queue)
	conf.ready_non_transactional_queue = nil
	fmt.Fprintf(mdfile, "[config]\nmax-statement-size = %d\n", max_statement_size)
	datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Finished dump at: %s\n", datetimestr)
	mdfile.Close()
	if UpdatedSince > 0 {
		nufile.Close()
	}
	if err = os.Rename(metadata_partial_filename, metadata_filename); err != nil {
		log.Criticalf("We were not able to rename metadata file")
	}
	if Stream != "" {
		stream_queue_push(nil, metadata_filename)
		if Exec_command != "" {
			wait_exec_command_to_finish()
		} else {
			stream_queue_push(nil, "")
			wait_exec_command_to_finish()
		}
		if No_delete == false && OutputDirectoryStr == "" {
			if err = os.RemoveAll(output_directory); err != nil {
				log.Criticalf("Backup directory not removed: %s", output_directory)
			}
		}
	}
	for _, key := range slices.Sorted(keys) {
		dbt = all_dbts[key]
		G_assert(dbt != nil)
		free_db_table(dbt)
	}
	keys = nil
	log.Infof("Finished dump at: %s", datetimestr)
	if sthread != nil {
		G_thread_unref(sthread)
	}
	free_databases()
	if disk_check_thread != nil {
		DiskLimits = ""
	}
	Set_session = nil
	Set_global = nil
	Set_global_back = nil
	G_hash_table_unref(conf_per_table.All_anonymized_function)
	G_hash_table_unref(conf_per_table.All_where_per_table)
	G_hash_table_unref(conf_per_table.All_limit_per_table)
	G_hash_table_unref(conf_per_table.All_num_threads_per_table)
	finalize_masquerade()
	G_async_queue_unref(conf.gtid_pos_checked)
	G_async_queue_unref(conf.are_all_threads_in_same_pos)
	G_async_queue_unref(conf.db_ready)
	Free_regex()
	free_common()
	finalize_masquerade()
	Free_set_names()
	return nil
}
