package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/shirou/gopsutil/disk"
	log "github.com/sirupsen/logrus"
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
	initial_queue               *asyncQueue
	schema_queue                *asyncQueue
	non_innodb                  *table_queuing
	innodb                      *table_queuing
	post_data_queue             *asyncQueue
	ready                       *asyncQueue
	ready_non_innodb_queue      *asyncQueue
	db_ready                    *asyncQueue
	binlog_ready                *asyncQueue
	unlock_tables               *asyncQueue
	pause_resume                *asyncQueue
	gtid_pos_checked            *asyncQueue
	are_all_threads_in_same_pos *asyncQueue
	lock_tables_statement       string
	mutex                       *sync.Mutex
	done                        int
}
type MList struct {
	list  []*db_table
	mutex *sync.Mutex
}
type thread_data struct {
	conf                          *configuration
	thread_id                     uint
	table_name                    string
	thrconn                       *client.Conn
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
	process  func(o *OptionEntries, tj *table_job, csi *chunk_step_item)
	get_next func(o *OptionEntries, dbt *db_table) *chunk_step_item
}
type table_queuing struct {
	queue         *asyncQueue
	deferQueue    *asyncQueue
	request_chunk *asyncQueue
	table_list    *MList
	descr         string
}

type fifo struct {
	filename        string
	stdout_filename string
	queue           *asyncQueue
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
	dbt             *db_table
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

type db_table struct {
	key                            string
	database                       *database
	table                          string
	table_filename                 string
	escaped_table                  string
	min                            string
	max                            string
	object_to_export               *object_to_export
	select_fields                  string
	complete_insert                bool
	insert_statement               *strings.Builder
	load_data_header               *strings.Builder
	load_data_suffix               *strings.Builder
	is_innodb                      bool
	is_sequence                    bool
	has_json_fields                bool
	character_set                  string
	rows_total                     uint64
	rows                           uint64
	estimated_remaining_steps      uint64
	rows_lock                      *sync.Mutex
	anonymized_function            []*function_pointer
	where                          string
	limit                          string
	columns_on_select              string
	columns_on_insert              string
	partition_regex                *regexp.Regexp
	num_threads                    uint
	chunks                         []any
	chunks_queue                   *asyncQueue
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

type lock_function func(conn *client.Conn)

func initialize_start_dump(o *OptionEntries) {
	o.global.all_dbts = make(map[string]*db_table)
	initialize_set_names(o)
	initialize_working_thread(o)
	if o.global.conf_per_table == nil {
		o.global.conf_per_table = new(configuration_per_table)
	}
	initialize_conf_per_table(o.global.conf_per_table)

	// until we have an unique option on lock int_types we need to ensure this
	if o.Lock.NoLocks || o.Lock.TrxConsistencyOnly {
		o.Lock.LessLocking = false
	}

	// clarify binlog coordinates with trx_consistency_only
	if o.Lock.TrxConsistencyOnly {
		log.Warnf("Using trx_consistency_only, binlog coordinates will not be  accurate if you are writing to non transactional tables.")
	}

	if o.Filter.DB != "" {
		o.global.db_items = strings.Split(o.Filter.DB, ",")
	}

	if o.Pmm.PmmPath != "" {
		o.global.pmm = true
		if o.Pmm.PmmResolution == "" {
			o.Pmm.PmmResolution = "high"
		}
	} else if o.Pmm.PmmResolution != "" {
		o.global.pmm = true
		o.Pmm.PmmPath = fmt.Sprintf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution", o.Pmm.PmmResolution)
	}

	/*if o.Stream.Stream && o.Exec.Exec_command != "" {
		log.Fatalf("Stream and execute a command is not supported")
	} */
}

func (o *OptionEntries) set_disk_limits(p_at, r_at uint) {
	o.global.pause_at = p_at
	o.global.resume_at = r_at
}

func (o *OptionEntries) is_disk_space_ok(val uint) bool {
	if !path.IsAbs(o.global.dump_directory) {
		pwd, _ := os.Getwd()
		o.global.dump_directory = path.Join(pwd, o.global.dump_directory)
	}
	partitions, err := disk.Partitions(true)
	if err != nil {
		log.Errorf("Error getting partitions: %s", err.Error())
	}
	// 找到指定路径的挂载分区
	var mountPoint string
	for _, partition := range partitions {
		if o.global.dump_directory == partition.Mountpoint || (len(o.global.dump_directory) > len(partition.Mountpoint) && o.global.dump_directory[:len(partition.Mountpoint)] == partition.Mountpoint) {
			mountPoint = partition.Mountpoint
			break
		}
	}

	if mountPoint == "" {
		log.Fatalf("No partition found for path: %s", o.global.dump_directory)
	}
	// 获取分区使用情况
	usage, err := disk.Usage(mountPoint)
	if err != nil {
		log.Fatalf("Error getting disk usage: %v", err)
	}
	return usage.Free/1024/1024 > uint64(val)
}

func monitor_disk_space_thread(o *OptionEntries, queue *asyncQueue) {
	if o.global.disk_check_thread == nil {
		o.global.disk_check_thread = new(sync.WaitGroup)
	}
	o.global.disk_check_thread.Add(1)
	defer o.global.disk_check_thread.Done()
	o.global.pause_mutex_per_thread = make([]*sync.Mutex, o.Common.NumThreads)
	var i uint
	for i = 0; i < o.Common.NumThreads; i++ {
		o.global.pause_mutex_per_thread[i] = g_mutex_new()
	}
	var previous_state = true
	var current_state = true
	for o.CommonOptionEntries.DiskLimits != "" {
		if previous_state {
			current_state = o.is_disk_space_ok(o.global.pause_at)
		} else {
			current_state = o.is_disk_space_ok(o.global.resume_at)
		}
		if previous_state != current_state {
			if !current_state {
				log.Warnf("Pausing backup disk space lower than %dMB. You need to free up to %dMB to resume", o.global.pause_at, o.global.resume_at)
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Lock()
					queue.push(o.global.pause_mutex_per_thread[i])
				}
			} else {
				log.Warnf("Resuming backup")
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
			}
			previous_state = current_state
		}
		time.Sleep(10 * time.Second)
	}
	// return
}

func sig_triggered(o *OptionEntries, user_data any, signal os.Signal) bool {
	if signal == syscall.SIGTERM {
		o.global.shutdown_triggered = true
	} else {
		var i uint
		if len(o.global.pause_mutex_per_thread) == 0 {
			o.global.pause_mutex_per_thread = make([]*sync.Mutex, o.Common.NumThreads)
			for i = 0; i < o.Common.NumThreads; i++ {
				o.global.pause_mutex_per_thread[i] = g_mutex_new()
			}
		}
		if user_data.(*configuration).pause_resume == nil {
			user_data.(*configuration).pause_resume = g_async_queue_new(o.CommonOptionEntries.BufferSize)
		}
		var queue = user_data.(*configuration).pause_resume
		if !o.Daemon.DaemonMode {
			var datetimestr = m_date_time_new_now_local()
			fmt.Printf("%s: Ctrl+c detected! Are you sure you want to cancel(Y/N)?", datetimestr)
			for i = 0; i < o.Common.NumThreads; i++ {
				o.global.pause_mutex_per_thread[i].Lock()
				queue.push(o.global.pause_mutex_per_thread[i])
			}
			var c string
			_, _ = fmt.Scanln(&c)
			if strings.ToUpper(c) == "N" {
				datetimestr = m_date_time_new_now_local()
				fmt.Printf("%s: Resuming backup\n", datetimestr)
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				return true
			}
			if strings.ToUpper(c) == "Y" {
				datetimestr = m_date_time_new_now_local()
				fmt.Printf("%s: Backup cancelled\n", datetimestr)
				o.global.shutdown_triggered = true
				for i = 0; i < o.Common.NumThreads; i++ {
					o.global.pause_mutex_per_thread[i].Unlock()
				}
				log.Infof("Shutting down gracefully")
				return false
			}
		}
	}
	return false
}

func signal_thread(o *OptionEntries, conf *configuration) {
	if o.global.sthread == nil {
		o.global.sthread = new(sync.WaitGroup)
	}
	o.global.sthread.Add(1)
	defer o.global.sthread.Done()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	sig := <-signalChan
	sig_triggered(o, conf, sig)
	log.Infof("Ending signal thread")
	return
}

func initialize_sql_mode(o *OptionEntries) {
	var str = o.global.sql_mode
	str = strings.ReplaceAll(str, "ORACLE", "")
	str = strings.ReplaceAll(str, ",,", ",")
	o.global.set_session_hash["SQL_MODE"] = str
}

func mydumper_initialize_hash_of_session_variables(o *OptionEntries) map[string]string {
	o.global.set_session_hash = o.initialize_hash_of_session_variables()
	o.global.set_session_hash["information_schema_stats_expiry"] = "0 /*!80003"
	return o.global.set_session_hash
}

func create_connection(o *OptionEntries) *client.Conn {
	var conn *client.Conn
	conn, _ = m_connect(o)
	execute_gstring(conn, o.global.set_session)
	return conn
}

func detect_quote_character(o *OptionEntries, conn *client.Conn) {
	var res *mysql.Result
	var row []mysql.FieldValue
	var err error
	var query = "SELECT FIND_IN_SET('ANSI', @@SQL_MODE) OR FIND_IN_SET('ANSI_QUOTES', @@SQL_MODE)"
	res, err = conn.Execute(query)
	if err != nil {
		log.Warnf("We were not able to determine ANSI mode: %v", err)
		o.global.identifier_quote_character = BACKTICK
		o.global.identifier_quote_character_str = "`"
		o.global.fields_enclosed_by = "\""
		o.global.identifier_quote_character_protect = backtick_protect
		return
	}
	if len(res.Values) == 0 {
		log.Warnf("We were not able to determine ANSI mode")
		return
	}
	for _, row = range res.Values {
		if row[0].AsInt64() == 0 {
			o.global.identifier_quote_character = BACKTICK
			o.global.identifier_quote_character_str = "`"
			o.global.fields_enclosed_by = "\""
			o.global.identifier_quote_character_protect = backtick_protect
		} else {
			o.global.identifier_quote_character = DOUBLE_QUOTE
			o.global.identifier_quote_character_str = "\""
			o.global.fields_enclosed_by = "'"
			o.global.identifier_quote_character_protect = double_quoute_protect
		}
	}
}

func detect_sql_mode(o *OptionEntries, conn *client.Conn) {
	var res *mysql.Result
	var row []mysql.FieldValue
	var err error
	var query = "SELECT @@SQL_MODE"
	res, err = conn.Execute(query)
	if err != nil {
		log.Fatalf("Error getting SQL_MODE: %v", err)
	}
	if len(res.Values) == 0 {
		log.Fatalf("Error getting SQL_MODE")
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
		o.global.sql_mode = str
	}
}

func create_main_connection(o *OptionEntries) (conn *client.Conn, err error) {
	conn, err = m_connect(o)
	if err != nil {
		return
	}
	o.global.set_session = ""
	o.global.set_global = ""
	o.global.set_global_back = ""
	_ = detect_server_version(o, conn)
	o.global.detected_server = o.get_product()
	o.global.set_session_hash = mydumper_initialize_hash_of_session_variables(o)
	o.global.set_global_hash = make(map[string]string)
	if o.global.key_file != nil {
		o.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(o.global.key_file, o, o.global.set_global_hash, mydumper_global_variables)
		o.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(o.global.key_file, o, o.global.set_session_hash, mydumper_session_variables)
		load_per_table_info_from_key_file(o.global.key_file, o.global.conf_per_table)
	}
	o.global.sql_mode = o.global.set_session_hash["SQL_MODE"]
	if o.global.sql_mode == "" {
		detect_sql_mode(o, conn)
		initialize_sql_mode(o)
	}
	refresh_set_session_from_hash(o)
	refresh_set_global_from_hash(&o.global.set_global, &o.global.set_global_back, o.global.set_global_hash)
	// free_hash_table(set_session_hash)
	execute_gstring(conn, o.global.set_session)
	execute_gstring(conn, o.global.set_global)
	detect_quote_character(o, conn)
	initialize_headers(o)
	initialize_write(o)
	switch o.global.detected_server {
	case SERVER_TYPE_MYSQL:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_MARIADB:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_TIDB:
		o.Checksum.DataChecksums = false
	case SERVER_TYPE_PERCONA:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_UNKNOWN:
		set_transaction_isolation_level_repeatable_read(conn)
	case SERVER_TYPE_CLICKHOUSE:
		o.Checksum.DataChecksums = false
	default:
		log.Fatalf("Cannot detect server type")
	}
	log.Infof("Connected to %s %d.%d.%d", o.get_product_name(), o.get_major(), o.get_secondary(), o.get_revision())
	return conn, nil
}

func get_not_updated(o *OptionEntries, conn *client.Conn, file *os.File) {
	var res *mysql.Result
	var err error
	var query string
	query = fmt.Sprintf("SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND UPDATE_TIME < NOW() - INTERVAL '%d' DAY", o.Filter.UpdatedSince)
	res, err = conn.Execute(query)
	if err != nil {
		return
	}
	for _, row := range res.Values {
		o.global.no_updated_tables = append(o.global.no_updated_tables, string(row[0].AsString()))
		_, _ = file.WriteString(fmt.Sprintf("%s\n", row[0].AsString()))
	}
	slices.Sort(o.global.no_updated_tables)
	_ = file.Sync()
	return
}

func long_query_wait(o *OptionEntries, conn *client.Conn) {
	var p3 string
	for {
		var longquery_count int
		res, err := conn.Execute("SHOW PROCESSLIST")
		if err != nil {
			log.Warnf("Could not check PROCESSLIST, no long query guard enabled: %v", err)
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
				log.Fatalf("Error obtaining information from processlist")
			}
			for _, row := range res.Values {
				if string(row[ccol].AsString()) == "Query" || strings.Contains(string(row[ccol].AsString()), "Dump") {
					continue
				}
				if string(row[ucol].AsString()) == "system user" || string(row[ucol].AsString()) == "event_scheduler" {
					continue
				}
				if row[tcol].AsUint64() > o.QueryRunning.Longquery {
					if o.QueryRunning.Killqueries {
						p3 = fmt.Sprintf("KILL %d", row[icol].AsUint64())
						_, err = conn.Execute(p3)
						if err != nil {
							log.Warnf("Could not KILL slow query: %v", err)
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
				if o.QueryRunning.LongqueryRetries == 0 {
					log.Fatalf("There are queries in PROCESSLIST running longer than %ds, aborting dump, use --long-query-guard to change the guard value, kill queries (--kill-long-queries) or use different server for dump", o.QueryRunning.Longquery)
				}
				o.QueryRunning.LongqueryRetries--
				log.Warnf("There are queries in PROCESSLIST running longer than %ds, retrying in %d seconds (%d left).", o.QueryRunning.Longquery, o.QueryRunning.LongqueryRetryInterval, o.QueryRunning.LongqueryRetries)
				time.Sleep(time.Duration(o.QueryRunning.LongqueryRetryInterval) * time.Second)
			}
		}
	}
}
func mysql_query_verbose(conn *client.Conn, q string) error {
	_, err := conn.Execute(q)
	if err == nil {
		log.Infof("%s: OK", q)

	} else {
		log.Errorf("%s: %v", q, err)
	}
	return err
}

func send_backup_stage_on_block_commit(conn *client.Conn) {
	err := mysql_query_verbose(conn, "BACKUP STAGE BLOCK_COMMIT")
	if err != nil {
		log.Fatalf("Couldn't acquire BACKUP STAGE BLOCK_COMMIT: %v", err)
	}
}

func send_mariadb_backup_locks(conn *client.Conn) {
	err := mysql_query_verbose(conn, "BACKUP STAGE START")
	if err != nil {
		log.Fatalf("Couldn't acquire BACKUP STAGE START: %v", err)
	}
	err = mysql_query_verbose(conn, "BACKUP STAGE BLOCK_DDL")
	if err != nil {
		log.Fatalf("Couldn't acquire BACKUP STAGE BLOCK_DDL: %v", err)
	}
}

func send_percona57_backup_locks(conn *client.Conn) {
	err := mysql_query_verbose(conn, "LOCK TABLES FOR BACKUP")
	if err != nil {
		log.Fatalf("Couldn't acquire LOCK TABLES FOR BACKUP, snapshots will not be consistent: %v", err)
	}
	err = mysql_query_verbose(conn, "LOCK BINLOG FOR BACKUP")
	if err != nil {
		log.Fatalf("Couldn't acquire LOCK BINLOG FOR BACKUP, snapshots will not be consistent: %v", err)
	}
}

func send_ddl_lock_instance_backup(conn *client.Conn) {
	err := mysql_query_verbose(conn, "LOCK INSTANCE FOR BACKUP")
	if err != nil {
		log.Fatalf("Couldn't acquire LOCK INSTANCE FOR BACKUP: %v", err)
	}
}

func send_unlock_tables(conn *client.Conn) {
	_ = mysql_query_verbose(conn, "UNLOCK TABLES")
}

func send_unlock_binlogs(conn *client.Conn) {
	_ = mysql_query_verbose(conn, "UNLOCK BINLOG")
}

func send_ddl_unlock_instance_backup(conn *client.Conn) {
	_ = mysql_query_verbose(conn, "UNLOCK INSTANCE")
}

func send_backup_stage_end(conn *client.Conn) {
	_ = mysql_query_verbose(conn, "BACKUP STAGE END")

}

func send_flush_table_with_read_lock(conn *client.Conn) {
	err := mysql_query_verbose(conn, "FLUSH NO_WRITE_TO_BINLOG TABLES")
	if err != nil {
		log.Warnf("Flush tables failed, we are continuing anyways: %v", err)
	}
	log.Infof("Acquiring FTWRL")
	err = mysql_query_verbose(conn, "FLUSH TABLES WITH READ LOCK")
	if err != nil {
		log.Fatalf("Couldn't acquire global lock, snapshots will not be consistent: %v", err)
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

func determine_ddl_lock_function(o *OptionEntries, conn *client.Conn) (acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function) {
	switch o.get_product() {
	case SERVER_TYPE_PERCONA:
		switch o.get_major() {
		case 8:
			acquire_ddl_lock_function = send_ddl_lock_instance_backup
			release_ddl_lock_function = send_ddl_unlock_instance_backup
			acquire_global_lock_function = send_flush_table_with_read_lock
			release_global_lock_function = send_unlock_tables
		case 5:
			if o.get_secondary() == 7 {
				if o.Lock.NoBackupLocks {
					acquire_ddl_lock_function = nil
					release_ddl_lock_function = nil
				} else {
					acquire_ddl_lock_function = send_percona57_backup_locks
					release_ddl_lock_function = send_unlock_tables
				}
				acquire_global_lock_function = send_flush_table_with_read_lock
				release_global_lock_function = send_unlock_tables

				release_binlog_function = send_unlock_binlogs
				conn = create_connection(o)
			} else {
				acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
			}
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
	case SERVER_TYPE_MYSQL:
		switch o.get_major() {
		case 8:
			acquire_ddl_lock_function = send_ddl_lock_instance_backup
			release_ddl_lock_function = send_ddl_unlock_instance_backup
			acquire_global_lock_function = send_flush_table_with_read_lock
			release_global_lock_function = send_unlock_tables
		default:
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = default_locking()
		}
	case SERVER_TYPE_MARIADB:
		if (o.get_major() == 10 && o.get_secondary() >= 5) || o.get_major() > 10 {
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

func print_dbt_on_metadata_gstring(dbt *db_table, data *string) {
	var name string = newline_protect(dbt.database.name)
	var table_filename = newline_protect(dbt.table_filename)
	var table = newline_protect(dbt.table)
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

func print_dbt_on_metadata(o *OptionEntries, mdfile *os.File, dbt *db_table) {
	var data string
	print_dbt_on_metadata_gstring(dbt, &data)
	fmt.Fprintf(mdfile, data)
	mdfile.Sync()
	if o.Extra.CheckRowCount && (dbt.rows != dbt.rows_total) {
		log.Fatalf("Row count mismatch found for %s.%s: got %d of %d expected", dbt.database.name, dbt.table, dbt.rows, dbt.rows_total)
	}

}

func send_lock_all_tables(o *OptionEntries, conn *client.Conn) {
	// LOCK ALL TABLES
	var query string
	var dbtb string
	var dt []string
	var res *mysql.Result
	var tables_lock []string
	var err error
	var success bool
	var retry uint
	var lock = true
	var i uint = 0

	if len(o.global.tables) > 0 {
		for _, t := range o.global.tables {
			dt = strings.Split(t, ".")
			query = fmt.Sprintf("SHOW TABLES IN %s LIKE '%s'", dt[0], dt[1])
			res, err = conn.Execute(query)
			if err != nil {
				log.Fatalf("Error showing tables in: %s - Could not execute query: %v", dt[0], err)
			} else {
				for _, row := range res.Values {
					if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, dt[0], string(row[0].AsString())) {
						continue
					}
					if is_mysql_special_tables(dt[0], string(row[0].AsString())) {
						continue
					}
					if !eval_regex(o, dt[0], string(row[0].AsString())) {
						continue
					}
					dbtb = fmt.Sprintf("%s%s%s.%s%s%s", o.global.identifier_quote_character_str, dt[0], o.global.identifier_quote_character_str,
						o.global.identifier_quote_character_str, row[0].AsString(), o.global.identifier_quote_character_str)
					tables_lock = append(tables_lock, dbtb)
				}
			}
		}
		slices.Sort(tables_lock)
	} else {
		if o.Filter.DB != "" {
			var db_quoted_list string
			db_quoted_list += fmt.Sprintf("'%s'", o.global.db_items[i])
			i++
			for ; i < uint(len(o.global.db_items)); i++ {
				db_quoted_list += fmt.Sprintf(",'%s'", o.global.db_items[i])
			}
			query = fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA in (%s) AND TABLE_TYPE ='BASE TABLE'", db_quoted_list)
		} else {
			query = fmt.Sprintf("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE' AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'data_dictionary')")
		}
		res, err = conn.Execute(query)
		if err != nil {
			log.Fatalf("Couldn't get table list for lock all tables: %v", err)
		} else {
			for _, row := range res.Values {
				if o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				if is_mysql_special_tables(string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				if !eval_regex(o, string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}
				dbtb = fmt.Sprintf("%s%s%s.%s%s%s", o.global.identifier_quote_character_str, row[0].AsString(), o.global.identifier_quote_character_str,
					o.global.identifier_quote_character_str, row[1].AsString(), o.global.identifier_quote_character_str)
				tables_lock = append(tables_lock, dbtb)
			}
		}
	}

	if len(tables_lock) > 0 {
		res, err = conn.Execute(query)
		if err != nil {
			log.Fatalf("Couldn't get table list for lock all tables: %v", err)
		} else {
			for _, row := range res.Values {
				lock = true
				if len(o.global.tables) > 0 {
					var table_found bool
					for _, t := range o.global.tables {
						if strings.Compare(t, string(row[1].AsString())) == 0 {
							table_found = true
							break
						}
					}
					if !table_found {
						lock = false
					}
				}

				if lock && o.CommonFilter.TablesSkiplistFile != "" && check_skiplist(o, string(row[0].AsString()), string(row[1].AsString())) {
					continue
				}

				if lock && !eval_regex(o, string(row[0].AsString()), string(row[1].AsString())) {
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
			_, err = conn.Execute(query)
			if err != nil {

				var tmp_fail []string = strings.Split(err.Error(), "'")
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
			log.Fatalf("Lock all tables fail: %v", err)
		}
	} else {
		log.Warnf("No table found to lock")
		//    exit(EXIT_FAILURE);
	}
}

func write_replica_info(o *OptionEntries, conn *client.Conn, file *os.File) {
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
	rest, err := conn.Execute("SELECT @@default_master_connection")
	if rest != nil && err == nil {
		log.Infof("Multisource slave detected.")
		isms = true
	}
	var slave_count uint
	if isms {
		slave = m_query(o, conn, o.global.show_all_replicas_status, m_critical, fmt.Sprintf("Error executing %s", o.global.show_all_replicas_status))
	} else {
		slave = m_query(o, conn, o.global.show_replica_status, m_critical, fmt.Sprintf("Error executing %s", o.global.show_replica_status))
	}
	if slave == nil || len(slave.Values) == 0 {
		return
	}
	log.Infof("Stopping replica")
	_, err = conn.Execute(o.global.stop_replica_sql_thread)
	if err != nil {
		log.Warnf("Not able to stop replica: %v", err)
	}
	if o.global.source_control_command == AWS {
		discard_mysql_output(conn)
	}
	if isms {
		slave = m_query(o, conn, o.global.show_all_replicas_status, m_critical, fmt.Sprintf("Error executing %s", o.global.show_all_replicas_status))
	} else {
		slave = m_query(o, conn, o.global.show_replica_status, m_critical, fmt.Sprintf("Error executing %s", o.global.show_replica_status))
	}
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
			slavegtid = remove_new_line(string(row[i].AsString()))
		} else if "Gtid_Slave_Pos" == string(fields[i].Name) || string("Gtid_source_Pos") == string(fields[i].Name) {
			gtid_title = string(fields[i].Name)
			slavegtid = remove_new_line(string(row[i].AsString()))
		} else if ("connection_name" == string(fields[i].Name) || "Channel_Name" == string(fields[i].Name)) && len(row[i].AsString()) > 1 {
			channel_name = string(row[i].AsString())
		}
		replication_section_str += fmt.Sprintf("# %s = ", fields[i].Name)
		if fields[i].Type != mysql.MYSQL_TYPE_LONG && fields[i].Type != mysql.MYSQL_TYPE_LONGLONG && fields[i].Type != mysql.MYSQL_TYPE_INT24 && fields[i].Type != mysql.MYSQL_TYPE_SHORT {
			replication_section_str += fmt.Sprintf("'%s'\n", remove_new_line(string(row[i].AsString())))
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

func StartDump(o *OptionEntries) error {
	if o.Common.ClearDumpDir {
		clear_dump_directory(o.global.dump_directory)
	} else if !o.Common.DirtyDumpDir && !is_empty_dir(o.global.dump_directory) {
		log.Errorf("Directory is not empty (use --clear or --dirty): %s\n", o.global.dump_directory)
	}

	check_num_threads(o)
	log.Infof("Using %d dumper threads", o.Common.NumThreads)
	initialize_start_dump(o)
	initialize_common(o)

	initialize_connection(o, MYDUMPER)
	initialize_masquerade(o)

	if o.CommonFilter.TablesList != "" {
		o.global.tables = get_table_list(o, o.CommonFilter.TablesList)
	}
	if o.CommonFilter.TablesSkiplistFile != "" {
		read_tables_skiplist(o, o.CommonFilter.TablesSkiplistFile)
	}
	err := initialize_regex(o, o.Filter.PartitionRegex)
	var conn *client.Conn
	conn, err = create_main_connection(o)
	if err != nil {
		return err
	}
	o.global.main_connection = conn
	o.global.second_conn = conn
	var conf *configuration = new(configuration)
	conf.use_any_index = "1"
	var metadata_partial_filename, metadata_filename string
	var u string
	var acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function lock_function
	var dbt *db_table
	var n uint
	var nufile *os.File

	if o.CommonOptionEntries.DiskLimits != "" {
		conf.pause_resume = g_async_queue_new(o.CommonOptionEntries.BufferSize)
		go monitor_disk_space_thread(o, conf.pause_resume)
	}
	if !o.Daemon.DaemonMode {
		go signal_thread(o, conf)
		time.Sleep(10 * time.Microsecond)
		if o.global.sthread == nil {
			log.Fatalf("Could not create signal threads")
		}
	}
	if o.global.pmm {
		log.Infof("Using PMM resolution %s at %s", o.Pmm.PmmResolution, o.Pmm.PmmPath)
		// go pmm_thread(o, conf)
		if o.global.pmmthread == nil {
			log.Fatalf("Could not create pmm thread")
		}
	}
	if o.Stream.Stream {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.header", o.global.dump_directory)
	} else {
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", o.global.dump_directory)
	}
	metadata_filename = fmt.Sprintf("%s/metadata", o.global.dump_directory)

	mdfile, err := os.OpenFile(metadata_partial_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		log.Fatalf("Couldn't write metadata file %s (%v)", metadata_partial_filename, err)
	}
	if o.Filter.UpdatedSince > 0 {
		u = fmt.Sprintf("%s/not_updated_tables", o.global.dump_directory)
		nufile, err = os.OpenFile(u, os.O_CREATE|os.O_WRONLY, 0660)
		if err != nil {
			log.Fatalf("Couldn't write not_updated_tables file (%v)", err)
		}
		get_not_updated(o, conn, nufile)
	}

	if !o.Lock.NoLocks && o.is_mysql_like() {
		// We check SHOW PROCESSLIST, and if there're queries
		// larger than preset value, we terminate the process.
		// This avoids stalling whole server with flush.
		long_query_wait(o, conn)
	}
	var datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Started dump at: %s\n", datetimestr)
	log.Infof("Started dump at: %s", datetimestr)

	/* Write dump config into beginning of metadata, stream this first */
	if o.Common.IdentifierQuoteCharacter == BACKTICK || o.Common.IdentifierQuoteCharacter == DOUBLE_QUOTE {
		var qc string
		if o.Common.IdentifierQuoteCharacter == BACKTICK {
			qc = "BACKTICK"
		} else {
			qc = "DOUBLE_QUOTE"
		}
		fmt.Fprintf(mdfile, "[config]\nquote_character = %s\n", qc)
		fmt.Fprintf(mdfile, "\n[myloader_session_variables]")
		fmt.Fprintf(mdfile, "\nSQL_MODE=%s \n\n", o.global.sql_mode)
		mdfile.Sync()
	} else {
		log.Fatalf("--identifier-quote-character not is %s or %s", BACKTICK, DOUBLE_QUOTE)
	}
	if o.Stream.Stream {
		initialize_stream(o)
		stream_queue_push(o, nil, metadata_partial_filename)
		mdfile.Close()
		metadata_partial_filename = fmt.Sprintf("%s/metadata.partial", o.global.dump_directory)
		mdfile, err = os.OpenFile(metadata_partial_filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
		if err != nil {
			log.Fatalf("Couldn't create metadata file %s (%v)", metadata_partial_filename, err)
		}
	}
	if o.global.detected_server == SERVER_TYPE_TIDB {
		log.Infof("Skipping locks because of TiDB")
		if o.Lock.TidbSnapshot == "" {

			// Generate a @@tidb_snapshot to use for the worker threads since
			// the tidb-snapshot argument was not specified when starting mydumper
			var res *mysql.Result
			res = m_query(o, conn, o.global.show_binary_log_status, m_critical, "Couldn't generate @@tidb_snapshot")
			o.Lock.TidbSnapshot = string(res.Values[0][1].AsString())
		}
		// Need to set the @@tidb_snapshot for the master thread
		set_tidb_snapshot(o, conn)
		log.Infof("Set to tidb_snapshot '%s'", o.Lock.TidbSnapshot)

	} else {
		if !o.Lock.NoLocks {
			// This backup will lock the database
			acquire_global_lock_function, release_global_lock_function, acquire_ddl_lock_function, release_ddl_lock_function, release_binlog_function = determine_ddl_lock_function(o, o.global.second_conn)
			if o.Lock.SkipDdlLocks {
				acquire_ddl_lock_function = nil
				release_ddl_lock_function = nil
			}
			if o.Lock.LockAllTables {
				send_lock_all_tables(o, conn)
			} else {

				if acquire_ddl_lock_function != nil {
					log.Infof("Acquiring DDL lock")
					acquire_ddl_lock_function(o.global.second_conn)
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
	o.global.server_version = mysql_get_server_version(conn)
	if o.global.server_version < 40108 {
		conn.Execute("CREATE TABLE IF NOT EXISTS mysql.mydumperdummy (a INT) ENGINE=INNODB")
		o.global.need_dummy_read = true
	}
	if o.get_product() != SERVER_TYPE_MARIADB || o.global.server_version < 100300 {
		o.global.nroutines = 2
	}
	// tokudb do not support consistent snapshot
	rest, _ := conn.Execute("SELECT @@tokudb_version")

	if rest != nil {
		log.Infof("TokuDB detected, creating dummy table for CS")
		_, err = conn.Execute("CREATE TABLE IF NOT EXISTS mysql.tokudbdummy (a INT) ENGINE=TokuDB")
		if err == nil {
			o.global.need_dummy_toku_read = true
		}
	}

	// Do not start a transaction when lock all tables instead of FTWRL,
	// since it can implicitly release read locks we hold
	// TODO: this should be deleted as main connection is not being used for export data
	//  if (!lock_all_tables) {
	//    g_message("Sending start transaction in main connection");
	//    mysql_query(conn, "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */");
	//  }

	if o.global.need_dummy_read {
		_, err = conn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.mydumperdummy")

	}
	if o.global.need_dummy_toku_read {
		_, err = conn.Execute("SELECT /*!40001 SQL_NO_CACHE */ * FROM mysql.tokudbdummy")

	}

	/*if o.Exec.Exec_command != "" {
		initialize_exec_command(o)
		o.Stream.Stream = true
	}*/

	conf.initial_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.schema_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.post_data_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	if conf.innodb == nil {
		conf.innodb = new(table_queuing)
		conf.non_innodb = new(table_queuing)
	}
	conf.innodb.queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.innodb.deferQueue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	if o.global.give_me_another_innodb_chunk_step_queue != nil && o.global.give_me_another_non_innodb_chunk_step_queue != nil &&
		o.global.innodb_table != nil && o.global.non_innodb_table != nil {
		log.Debugf("variables ok")
	} else {
		log.Fatalf("check variables fail")
	}
	conf.innodb.request_chunk = o.global.give_me_another_innodb_chunk_step_queue
	conf.innodb.table_list = o.global.innodb_table
	conf.innodb.descr = "InnoDB"
	conf.ready = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.non_innodb.queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.non_innodb.deferQueue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.non_innodb.request_chunk = o.global.give_me_another_non_innodb_chunk_step_queue
	conf.non_innodb.table_list = o.global.non_innodb_table
	conf.non_innodb.descr = "Non-InnoDB"
	conf.ready_non_innodb_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.unlock_tables = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.gtid_pos_checked = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.are_all_threads_in_same_pos = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.db_ready = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	conf.binlog_ready = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	//  ready_database_dump_mutex = g_rec_mutex_new();
	//  g_rec_mutex_lock(ready_database_dump_mutex);
	o.global.ready_table_dump_mutex = g_rec_mutex_new()
	o.global.ready_table_dump_mutex.Lock()

	log.Infof("conf created")

	if o.is_mysql_like() {
		create_job_to_dump_metadata(conf, mdfile)
	}

	// Begin Job Creation

	if o.Objects.DumpTablespaces {
		create_job_to_dump_tablespaces(conf, o.global.dump_directory)
	}
	if len(o.global.tables) > 0 {
		create_job_to_dump_table_list(o, o.global.tables, conf)
	} else if len(o.global.db_items) > 0 {
		var i int
		for i = 0; i < len(o.global.db_items); i++ {
			var this_db *database = new_database(o, conn, o.global.db_items[i], true)
			create_job_to_dump_database(o, this_db, conf)
			if !o.Objects.NoSchemas {
				create_job_to_dump_schema(o, this_db, conf)
			}
		}
	} else {
		create_job_to_dump_all_databases(o, conf)
	}
	log.Infof("End job creation")

	if !o.Objects.NoData {
		go chunk_builder_thread(o, conf)
	}
	var td []*thread_data
	if o.Lock.LessLocking {
		td = make([]*thread_data, o.Common.NumThreads*(1+1))
	} else {
		td = make([]*thread_data, o.Common.NumThreads*(0+1))
	}

	for n = 0; n < o.Common.NumThreads; n++ {
		td[n] = new(thread_data)
		td[n].conf = conf
		td[n].thread_id = n + 1
		td[n].less_locking_stage = false
		td[n].binlog_snapshot_gtid_executed = ""
		td[n].pause_resume_mutex = nil
		td[n].table_name = ""
		go working_thread(o, td[n])
	}
	var binlog_snapshot_gtid_executed string
	var binlog_snapshot_gtid_executed_status_local bool
	var start_transaction_retry uint
	for !binlog_snapshot_gtid_executed_status_local && start_transaction_retry < MAX_START_TRANSACTION_RETRIES {
		binlog_snapshot_gtid_executed_status_local = true
		for n = 0; n < o.Common.NumThreads; n++ {
			g_async_queue_pop(conf.gtid_pos_checked)
		}
		binlog_snapshot_gtid_executed = td[0].binlog_snapshot_gtid_executed
		for n = 1; n < o.Common.NumThreads; n++ {
			binlog_snapshot_gtid_executed_status_local = binlog_snapshot_gtid_executed_status_local && strings.Compare(td[n].binlog_snapshot_gtid_executed, binlog_snapshot_gtid_executed) == 0
		}
		for n = 0; n < o.Common.NumThreads; n++ {
			if binlog_snapshot_gtid_executed_status_local {
				g_async_queue_push(conf.are_all_threads_in_same_pos, 1)
			} else {
				g_async_queue_push(conf.are_all_threads_in_same_pos, 2)
			}
		}
		start_transaction_retry++
	}
	for n = 0; n < o.Common.NumThreads; n++ {
		g_async_queue_pop(conf.ready)
	}

	// IMPORTANT: At this point, all the threads are in sync

	if o.Lock.TrxConsistencyOnly {
		log.Infof("Transactions started, unlocking tables")
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}

		//    mysql_query(conn, "UNLOCK TABLES /* trx-only */");
		if release_binlog_function != nil {
			g_async_queue_pop(conf.binlog_ready)
			log.Infof("Releasing binlog lock")
			release_binlog_function(o.global.second_conn)
		}
		if o.global.replica_stopped {
			log.Infof("Starting replica")
			_, err = conn.Execute(o.global.start_replica_sql_thread)
			if err != nil {
				log.Warnf("Not able to start replica: %v", err)
			}
			if o.global.source_control_command == AWS {
				discard_mysql_output(conn)
			}
			o.global.replica_stopped = false
		}
	}

	log.Infof("Waiting database finish")
	g_async_queue_pop(conf.db_ready)
	o.global.no_updated_tables = nil
	for n = 0; n < o.Common.NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		g_async_queue_push(conf.initial_queue, j)
	}

	for n = 0; n < o.Common.NumThreads; n++ {
		g_async_queue_pop(conf.ready)
	}

	log.Infof("Shutdown jobs for less locking enqueued")
	for n = 0; n < o.Common.NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		g_async_queue_push(conf.schema_queue, j)
	}

	if o.Lock.LessLocking {
		build_lock_tables_statement(o, conf)
	}

	for n = 0; n < o.Common.NumThreads; n++ {
		g_async_queue_push(conf.ready_non_innodb_queue, 1)
	}

	if !o.Lock.NoLocks && !o.Lock.TrxConsistencyOnly {
		for n = 0; n < o.Common.NumThreads; n++ {
			g_async_queue_pop(conf.unlock_tables)
		}
		log.Infof("Non-InnoDB dump complete, releasing global locks")
		if release_global_lock_function != nil {
			release_global_lock_function(conn)
		}
		//    mysql_query(conn, "UNLOCK TABLES /* FTWRL */");
		log.Infof("Global locks released")
		if release_binlog_function != nil {
			g_async_queue_pop(conf.binlog_ready)
			log.Infof("Releasing binlog lock")
			release_binlog_function(o.global.second_conn)
		}
	}
	if o.global.replica_stopped {
		log.Infof("Starting replica")
		_, err = conn.Execute(o.global.start_replica_sql_thread)
		if err != nil {
			log.Warnf("Not able to start replica: %v", err)
		}
		if o.global.source_control_command == AWS {
			discard_mysql_output(conn)
		}
	}
	g_async_queue_unref(conf.binlog_ready)

	for n = 0; n < o.Common.NumThreads; n++ {
		var j = new(job)
		j.types = JOB_SHUTDOWN
		g_async_queue_push(conf.post_data_queue, j)
	}

	if !o.Objects.NoData {
		o.global.chunk_builder.Wait()
	}

	log.Infof("Waiting threads to complete")
	o.global.threads.Wait()
	finalize_working_thread(o)
	finalize_write(o)
	if release_ddl_lock_function != nil {
		log.Infof("Releasing DDL lock")
		release_ddl_lock_function(o.global.second_conn)
	}
	log.Infof("Queue count: %d %d %d %d %d", conf.initial_queue.length, conf.schema_queue.length,
		conf.non_innodb.queue.length+conf.non_innodb.deferQueue.length,
		conf.innodb.queue.length+conf.innodb.deferQueue.length,
		conf.post_data_queue.length)
	// close main connection
	if conn != o.global.second_conn {
		o.global.second_conn.Close()
	}
	execute_gstring(o.global.main_connection, o.global.set_global_back)
	conn.Close()
	log.Infof("Main connection closed")
	wait_close_files(o)

	for _, dbt = range o.global.all_dbts {
		print_dbt_on_metadata(o, mdfile, dbt)
	}
	write_database_on_disk(o, mdfile)
	if o.global.pmm {
		kill_pmm_thread(o)
	}
	g_async_queue_unref(conf.innodb.deferQueue)
	conf.innodb.descr = ""
	g_async_queue_unref(conf.innodb.queue)
	conf.innodb.queue = nil
	g_async_queue_unref(conf.non_innodb.deferQueue)
	conf.non_innodb.deferQueue = nil
	g_async_queue_unref(conf.non_innodb.queue)
	conf.non_innodb.queue = nil
	g_async_queue_unref(conf.unlock_tables)
	conf.unlock_tables = nil
	g_async_queue_unref(conf.ready)
	conf.ready = nil
	g_async_queue_unref(conf.schema_queue)
	conf.schema_queue = nil
	g_async_queue_unref(conf.initial_queue)
	conf.initial_queue = nil
	g_async_queue_unref(conf.post_data_queue)
	conf.post_data_queue = nil

	g_async_queue_unref(conf.ready_non_innodb_queue)
	conf.ready_non_innodb_queue = nil

	datetimestr = time.Now().Format(time.DateTime)
	fmt.Fprintf(mdfile, "# Finished dump at: %s\n", datetimestr)
	mdfile.Close()
	if o.Filter.UpdatedSince > 0 {
		nufile.Close()
	}
	os.Rename(metadata_partial_filename, metadata_filename)
	if o.Stream.Stream {
		stream_queue_push(o, nil, metadata_filename)
	}
	log.Infof("Finished dump at: %s", datetimestr)
	if o.Stream.Stream {
		stream_queue_push(o, nil, "")
		wait_stream_to_finish(o)
		if o.global.no_delete == false && o.CommonOptionEntries.OutputDirectoryParam == "" {
			err = os.RemoveAll(o.global.output_directory)
			if err != nil {
				log.Errorf("Backup directory not removed: %s", o.global.output_directory)
			}
		}
	}

	free_databases(o)
	finalize_masquerade(o)
	free_regex(o)
	free_common(o)
	finalize_masquerade(o)
	o.free_set_names()
	if o.Lock.NoLocks {
		if o.global.it_is_a_consistent_backup {
			log.Infof("This is a consistent backup.")
		} else {
			log.Warnf("This is NOT a consistent backup.")
		}
	}
	return nil
}
