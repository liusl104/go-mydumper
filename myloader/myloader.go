package myloader

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"github.com/spf13/pflag"
)

const (
	G_TIME_SPAN_SECOND = 1000000
	G_TIME_SPAN_MINUTE = 60000000
	G_TIME_SPAN_HOUR   = 3600000000
	G_TIME_SPAN_DAY    = 86400000000
	DIRECTORY          = "import"
	MYLOADER           = "myloader"
)

var (
	innodb_optimize_keys     bool = true
	optimize_keys_per_table  bool = true
	optimize_keys            bool = true
	optimize_keys_all_tables bool
	quote_character_cli      bool
	directory                string
	detailed_errors          *restore_errors = &restore_errors{}
	sequences_processed      uint
	sequences                uint
	sequences_mutex          *sync.Mutex = G_mutex_new()
	errors                   uint
	max_errors               uint
	retry_count              uint = 10
	load_data_list           map[string]*sync.Mutex
	load_data_list_mutex     *sync.Mutex
	conf_per_table           *Configuration_per_table = new(Configuration_per_table)
	set_session_hash         map[string]string
	set_global_hash          map[string]string
	pmm                      bool
)

type schema_status int
type thread_states int
type file_type int

const (
	METADATA_GLOBAL file_type = iota
	SCHEMA_TABLESPACE
	SCHEMA_SEQUENCE
	SCHEMA_CREATE
	SCHEMA_TABLE
	DATA
	LOAD_DATA
	SCHEMA_VIEW
	SCHEMA_TRIGGER
	SCHEMA_POST
	IGNORED
	INIT
	CJT_RESUME
	RESUME
	SHUTDOWN
	DO_NOT_ENQUEUE
	REQUEST_DATA_JOB
	INTERMEDIATE_ENDED
)

const (
	WAITING thread_states = iota
	STARTED
	COMPLETED
)
const (
	NOT_FOUND schema_status = iota
	NOT_FOUND_2
	NOT_CREATED
	CREATING
	CREATED
	DATA_DONE
	INDEX_ENQUEUED
	ALL_DONE
)

type io_restore_result struct {
	restore *GAsyncQueue
	result  *GAsyncQueue
}

type restore_errors struct {
	data_errors        uint64
	data_warnings      uint64
	index_errors       uint64
	schema_errors      uint64
	trigger_errors     uint64
	view_errors        uint64
	sequence_errors    uint64
	tablespace_errors  uint64
	post_errors        uint64
	constraints_errors uint64
	retries            uint64
}

type connection_data struct {
	thrconn          *DBConnection
	current_database *database
	connection_id    int
	thread_id        uint64
	queue            *io_restore_result
	ready            *GAsyncQueue
	transaction      bool
	in_use           *sync.Mutex
}
type database struct {
	name              string // aka: the logical schema name, that could be different of the filename.
	real_database     string // aka: the output schema name this can change when use -B.
	filename          string // aka: the key of the schema. Useful if you have mydumper_ filenames.
	schema_state      schema_status
	sequence_queue    *GAsyncQueue
	queue             *GAsyncQueue
	mutex             *sync.Mutex // TODO: use g_mutex_init() instead of g_mutex_new()
	schema_checksum   string
	post_checksum     string
	triggers_checksum string
}
type configuration struct {
	database_queue   *GAsyncQueue
	table_queue      *GAsyncQueue
	retry_queue      *GAsyncQueue
	data_queue       *GAsyncQueue
	post_table_queue *GAsyncQueue
	view_queue       *GAsyncQueue
	post_queue       *GAsyncQueue
	ready            *GAsyncQueue
	pause_resume     *GAsyncQueue
	stream_queue     *GAsyncQueue
	table_list       []*db_table
	table_list_mutex *sync.Mutex
	table_hash       map[string]*db_table
	table_hash_mutex *sync.Mutex
	checksum_list    []string
	mutex            *sync.Mutex
	index_queue      *GAsyncQueue
	done             bool
}
type thread_data struct {
	conf                *configuration
	thread_id           uint
	status              thread_states
	granted_connections uint
	dbt                 *db_table
}
type function_pointer struct {
	function func(string)
}

// ft2str returns the string name of the file_type for logging.
func ft2str(ft file_type) string {
	switch ft {
	case INIT:
		return "INIT"
	case SCHEMA_TABLESPACE:
		return "SCHEMA_TABLESPACE"
	case SCHEMA_CREATE:
		return "SCHEMA_CREATE"
	case CJT_RESUME:
		return "CJT_RESUME"
	case SCHEMA_TABLE:
		return "SCHEMA_TABLE"
	case DATA:
		return "DATA"
	case SCHEMA_VIEW:
		return "SCHEMA_VIEW"
	case SCHEMA_SEQUENCE:
		return "SCHEMA_SEQUENCE"
	case SCHEMA_TRIGGER:
		return "SCHEMA_TRIGGER"
	case SCHEMA_POST:
		return "SCHEMA_POST"
	case METADATA_GLOBAL:
		return "METADATA_GLOBAL"
	case RESUME:
		return "RESUME"
	case IGNORED:
		return "IGNORED"
	case LOAD_DATA:
		return "LOAD_DATA"
	case SHUTDOWN:
		return "SHUTDOWN"
	case DO_NOT_ENQUEUE:
		return "DO_NOT_ENQUEUE"
	case REQUEST_DATA_JOB:
		return "REQUEST_DATA_JOB"
	case INTERMEDIATE_ENDED:
		return "INTERMEDIATE_ENDED"
	}
	return ""
}

type db_table struct {
	database                *database
	table                   string
	real_table              string
	object_to_export        *Object_to_export
	rows                    uint64
	rows_inserted           uint64
	restore_job_list        *list.List
	current_threads         uint
	max_threads             uint
	max_connections_per_job uint
	retry_count             uint
	mutex                   *sync.Mutex
	indexes                 *GString
	constraints             *GString
	count                   uint
	schema_state            schema_status
	index_enqueued          bool
	start_data_time         time.Time
	finish_data_time        time.Time
	start_index_time        time.Time
	finish_time             time.Time
	remaining_jobs          int64
	data_checksum           string
	schema_checksum         string
	indexes_checksum        string
	triggers_checksum       string
	is_view                 bool
	is_sequence             bool
}

// myloader_initialize_hash_of_session_variables builds the session variables hash (AUTOCOMMIT, SQL_LOG_BIN) for the restore.
func myloader_initialize_hash_of_session_variables() map[string]string {
	var _set_session_hash = Initialize_hash_of_session_variables()

	if CommitCount > 1 {
		_set_session_hash["AUTOCOMMIT"] = "0"
	}
	if !EnableBinlog {
		_set_session_hash["SQL_LOG_BIN"] = "0"
	}
	return _set_session_hash
}

// detect_group_replication_transaction_size_limit queries group_replication_transaction_size_limit and sets MaxTransactionSize if higher.
func detect_group_replication_transaction_size_limit(conn *DBConnection) {
	var _max_transaction_size uint64
	var mr *M_ROW = M_store_result_row(conn, "SELECT @@group_replication_transaction_size_limit / 1024 / 1024", M_message, M_message, "Using default transaction limit")
	if mr.Row != nil {
		_max_transaction_size = mr.Row[0].AsUint64()
	}
	if _max_transaction_size > MaxTransactionSize {
		MaxTransactionSize = _max_transaction_size
	}
	M_store_result_row_free(mr)
}

// print_time returns a formatted duration string (days:hours:minutes:seconds) since timespan.
func print_time(timespan time.Time) string {
	var now_time = time.Now().UnixMicro()
	var days = (now_time - timespan.UnixMicro()) / G_TIME_SPAN_DAY
	var hours = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY)) / G_TIME_SPAN_HOUR
	var minutes = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR)) / G_TIME_SPAN_MINUTE
	var seconds = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR) - (minutes * G_TIME_SPAN_MINUTE)) / G_TIME_SPAN_SECOND
	return fmt.Sprintf("%02d:%02d:%02d:%02d", days, hours, minutes, seconds)
}

// compare_by_time returns true if a's elapsed time (finish_time - start_data_time) is greater than b's (for sorting).
func compare_by_time(a *db_table, b *db_table) bool {
	return a.finish_time.Sub(a.start_data_time).Microseconds() > b.finish_time.Sub(b.start_data_time).Microseconds()
}

// initialize_directories sets directory and FifoDirectory from InputDirectory/Stream/current dir; validates backup dir and metadata file.
func initialize_directories() {
	var current_dir string
	current_dir, _ = os.Getwd()
	if InputDirectory == "" {
		if Stream != "" {
			var datetimestr = time.Now().Format("20060102-150405")
			directory = fmt.Sprintf("%s/%s-%s", current_dir, DIRECTORY, datetimestr)
		} else {
			if !Help {
				log.Criticalf("a directory needs to be specified, see --help\n")
			}
		}

	} else {
		if strings.HasPrefix(InputDirectory, "/") {
			directory = InputDirectory
		} else {
			directory = fmt.Sprintf("%s/%s", current_dir, InputDirectory)
		}
		if Stream != "" {
			if G_file_test(InputDirectory) && !No_stream {
				log.Criticalf("Backup directory (-d) must not exist when --stream / --stream=TRADITIONAL")
			}
		} else {
			if !G_file_test(InputDirectory) {
				log.Criticalf("the specified directory doesn't exists")
			}
			var p = fmt.Sprintf("%s/metadata", directory)
			if !G_file_test(p) {
				log.Criticalf("the specified directory %s is not a mydumper backup as metadata file was not found in it", directory)
			}
		}
	}
	if FifoDirectory != "" {
		if !filepath.IsAbs(FifoDirectory) {
			var tmp_fifo_directory = FifoDirectory
			FifoDirectory = fmt.Sprintf("%s/%s", current_dir, tmp_fifo_directory)
		}
	} else {
		// Set fifo temporary director
		FifoDirectory = Build_tmp_dir_name()
	}
}

// show_dbt logs the table key (callback for iteration).
func show_dbt(key any, dbt any, total any) {
	_ = key
	_ = dbt
	_ = total
	log.Infof("Table %s", key.(string))
}

// create_database runs restore_data_from_file for the schema-create file or executes CREATE DATABASE IF NOT EXISTS via restore_data_in_gstring_extended.
func create_database(td *thread_data, database string) {
	var filename = fmt.Sprintf("%s-schema-create.sql%s", database, ExecPerThreadExtension)
	var filepath = fmt.Sprintf("%s/%s-schema-create.sql%s", directory, database, ExecPerThreadExtension)
	if DropDatabase {
		execute_drop_database(td, database)
	}
	if G_file_test(filepath) {
		atomic.AddUint64(&detailed_errors.schema_errors, uint64(restore_data_from_file(td, filename, true, nil)))
	} else {
		var data *GString = G_string_new("CREATE DATABASE IF NOT EXISTS ")
		G_string_append_printf(data, "%s%s%s", Identifier_quote_character, database, Identifier_quote_character)
		if restore_data_in_gstring_extended(td, data, true, nil, M_critical, "Failed to create database: %s", database) {
			atomic.AddUint64(&detailed_errors.schema_errors, 1)
		}
		data = nil
	}
	return
}

// print_errors logs a summary of detailed_errors (tablespace, schema, data, view, sequence, index, trigger, constraint, post, warnings, retries).
func print_errors() {
	if detailed_errors.tablespace_errors == 0 &&
		detailed_errors.schema_errors == 0 &&
		detailed_errors.data_errors == 0 &&
		detailed_errors.view_errors == 0 &&
		detailed_errors.sequence_errors == 0 &&
		detailed_errors.index_errors == 0 &&
		detailed_errors.trigger_errors == 0 &&
		detailed_errors.constraints_errors == 0 &&
		detailed_errors.post_errors == 0 &&
		detailed_errors.retries == 0 {
		return
	}
	log.Infof("Errors found:\n"+
		"- Tablespace:\t%d\n"+
		"- Schema:    \t%d\n"+
		"- Data:      \t%d\n"+
		"- View:      \t%d\n"+
		"- Sequence:  \t%d\n"+
		"- Index:     \t%d\n"+
		"- Trigger:   \t%d\n"+
		"- Constraint:\t%d\n"+
		"- Post:      \t%d\n"+
		"Warnings found:\n"+
		"- Data:\t%d\n"+
		"Retries:\t%d",
		detailed_errors.tablespace_errors,
		detailed_errors.schema_errors,
		detailed_errors.data_errors,
		detailed_errors.view_errors,
		detailed_errors.sequence_errors,
		detailed_errors.index_errors,
		detailed_errors.trigger_errors,
		detailed_errors.constraints_errors,
		detailed_errors.post_errors,
		detailed_errors.data_warnings,
		detailed_errors.retries)
}

// StartLoad is the main entry point: parses flags, initializes directories/queues/workers, runs schema/data/post/index/checksum, then cleans up.
func StartLoad() {
	var err error
	load_contex_entries()
	if ProgramVersion {
		Print_version(MYLOADER)
		os.Exit(EXIT_SUCCESS)
	}
	if Help {
		print_help()
	}
	conf = new(configuration)
	Initialize_common_options(MYLOADER)
	if DB == "" && SourceDb != "" {
		DB = SourceDb
	}

	if OverwriteUnsafe {
		OverwriteTables = true
	}
	Check_num_threads()
	if NumThreads > MaxThreadsPerTable {
		log.Infof("Using %d loader threads (%d per table)", NumThreads, MaxThreadsPerTable)
	} else {
		log.Infof("Using %d loader threads", NumThreads)
	}

	log.Infof("MyDumper restore version: %s", VERSION)
	Hide_password()
	Ask_password()

	Initialize_pmm()

	initialize_restore_job()
	initialize_directories()

	initialize_restore()
	if Stream != "" && !No_stream {
		Create_dir(directory)
	}
	// Create_dir(FifoDirectory)
	log.Infof("Using %s as FIFO directory, please remove it if restoration fails", FifoDirectory)
	Start_pmm_thread(conf)
	err = os.Chdir(directory)
	if err != nil {
		log.Criticalf("Unable to change directory to %s: %v", directory, err)
	}
	/* Process list of tables to omit if specified */
	if TablesSkiplistFile != "" {
		Read_tables_skiplist(TablesSkiplistFile, &Errors)
	}
	initialize_process(conf)
	initialize_common()
	Initialize_connection(MYLOADER)
	InitializeRegex("")
	if !KillAtOnce {
		M_thread_new("myloader_signal", signal_thread, conf, "Signal thread could not be created")
	}
	var conn *DBConnection
	conn = Mysql_init()
	M_connect(conn)
	Set_session = G_string_new("")
	Set_global = G_string_new("")
	Set_global_back = G_string_new("")
	Server_detect(conn)
	set_session_hash = myloader_initialize_hash_of_session_variables()
	set_global_hash = make(map[string]string)
	if Key_file != nil {
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_global_hash, "myloader_global_variables")
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_session_hash, "myloader_session_variables")
	}
	Initialize_conf_per_table(conf_per_table)
	Load_per_table_info_from_key_file(Key_file, conf_per_table, nil)
	if MaxTransactionSize == DEFAULT_MAX_TRANSACTION_SIZE {
		detect_group_replication_transaction_size_limit(conn)
	}
	conf.database_queue = G_async_queue_new("conf.database_queue")
	conf.table_queue = G_async_queue_new("conf.table_queue")
	conf.retry_queue = G_async_queue_new("conf.retry_queue")
	conf.data_queue = G_async_queue_new("conf.data_queue")
	conf.post_table_queue = G_async_queue_new("conf.post_table_queue")
	conf.post_queue = G_async_queue_new("conf.post_queue")
	conf.index_queue = G_async_queue_new("conf.index_queue")
	conf.view_queue = G_async_queue_new("conf.view_queue")
	conf.ready = G_async_queue_new("conf.ready")
	conf.pause_resume = G_async_queue_new("conf.pause_resume")
	conf.table_list_mutex = G_mutex_new()
	// conf.stream_queue = G_async_queue_new("conf.stream_queue")
	conf.table_hash = make(map[string]*db_table)
	conf.table_hash_mutex = G_mutex_new()
	if G_file_test("resume") {
		if !Resume {
			log.Criticalf("Resume file found but --resume has not been provided")
		}
	} else {
		if Resume {
			log.Criticalf("Resume file not found")
		}
	}
	initialize_connection_pool()
	var t *thread_data = new(thread_data)
	initialize_thread_data(t, conf, WAITING, 0, nil)

	if TablesList != "" {
		Tables = Get_table_list(TablesList)
	}
	if SerialTblCreation {
		MaxThreadsForSchemaCreation = 1
	}
	initialize_worker_schema(conf)
	initialize_worker_index(conf)
	initialize_intermediate_queue(conf)
	if Stream != "" {
		if Resume {
			log.Criticalf("We don't expect to find resume files in a stream scenario")
		}
		initialize_stream(conf)
	} else {
		initialize_directory()
		M_thread_new("myloader_directory", process_directory, conf, "Directory thread could not be created")
	}
	if Stream != "" {
		wait_stream_to_process_metadata_header()
	} else {
		wait_directory_to_process_metadata()
	}
	remove_ignore_set_session_from_hash()
	Refresh_set_session_from_hash(Set_session, set_session_hash)
	Refresh_set_global_from_hash(Set_global, Set_global_back, set_global_hash)
	Execute_gstring(conn, Set_session)
	Execute_gstring(conn, Set_global)
	if replicationStatements.start_replica_until != nil {
		log.Infof("Sending start replica until")
		execute_replication_commands(conn, replicationStatements.start_replica_until.Str.String())
	}
	start_connection_pool()

	if DisableRedoLog {
		if Get_major() == 8 && Get_secondary() == 0 && Get_revision() > 21 {
			log.Infof("Disabling redologs")
			M_query_critical(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", "DISABLE INNODB REDO LOG failed")
		} else {
			log.Errorf("Disabling redologs is not supported for version %d.%d.%d", Get_major(), Get_secondary(), Get_revision())
		}
	}
	if database_db != nil {
		if !NoSchemas {
			create_database(t, database_db.real_database)
		}
		database_db.schema_state = CREATED
	}
	start_worker_schema()
	initialize_loader_threads(conf)

	if Throttle_variable != "" {
		M_thread_new("mon_thro", Monitor_throttling_thread, nil, "Monitor throttling thread could not be created")
	}
	if Stream != "" {
		wait_stream_to_finish()
	}
	var tl = conf.table_list
	for _, dbt := range tl {
		if dbt.max_connections_per_job == 1 {
			dbt.max_connections_per_job = 0
		}
	}
	wait_schema_worker_to_finish()
	wait_loader_threads_to_finish()
	wait_control_job()
	create_index_shutdown_job(conf)
	wait_index_worker_to_finish()
	initialize_post_loding_threads(conf)
	create_post_shutdown_job(conf)
	wait_post_worker_to_finish()
	//  wait_control_job();
	G_async_queue_unref(conf.ready)
	conf.ready = nil
	G_async_queue_unref(conf.data_queue)
	conf.data_queue = nil
	if DisableRedoLog {
		M_query_critical(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", "ENABLE INNODB REDO LOG failed")
	}
	var checksum_ok bool = true
	tl = conf.table_list
	for _, dbt := range tl {
		checksum_ok = checksum_ok && checksum_dbt(dbt, conn)
	}
	if checksum_mode != CHECKSUM_SKIP {
		var d *database
		for _, d = range db_hash {
			if d.schema_checksum != "" && !NoSchemas {
				checksum_ok = checksum_ok && checksum_database_template(d.real_database, d.schema_checksum, conn, "Schema create checksum", Checksum_database_defaults)
			}
			if d.post_checksum != "" && !SkipPost {
				checksum_ok = checksum_ok && checksum_database_template(d.real_database, d.post_checksum, conn, "Post checksum", Checksum_process_structure)
			}
			if d.triggers_checksum != "" && !SkipTriggers {
				checksum_ok = checksum_ok && checksum_database_template(d.real_database, d.triggers_checksum, conn, "Triggers checksum", Checksum_trigger_structure_from_database)
			}
		}
	}
	wait_restore_threads_to_close()
	if !checksum_ok {
		if checksum_mode == CHECKSUM_WARN {
			log.Warnf("Checksum failed")
		} else {
			log.Errorf("Checksum failed")
		}
	}

	if Stream != "" && No_delete == false {
		err = os.RemoveAll(directory)
		if err != nil {
			log.Warnf("Restore directory not removed: %s (%v)", directory, err)
		}
	}
	if replicationStatements.reset_replica != nil {
		log.Infof("Sending reset replica")
		execute_replication_commands(conn, replicationStatements.reset_replica.Str.String())
	}
	if replicationStatements.change_replication_source != nil {
		log.Infof("Sending change source")
		execute_replication_commands(conn, replicationStatements.change_replication_source.Str.String())
	}
	if replicationStatements.start_replica != nil {
		log.Infof("Sending start replica")
		execute_replication_commands(conn, replicationStatements.start_replica.Str.String())
	}

	G_async_queue_unref(conf.database_queue)
	G_async_queue_unref(conf.table_queue)
	G_async_queue_unref(conf.retry_queue)
	G_async_queue_unref(conf.pause_resume)
	G_async_queue_unref(conf.post_table_queue)
	G_async_queue_unref(conf.post_queue)
	set_session_hash = nil
	Execute_gstring(conn, Set_global_back)
	conn.Close()
	free_loader_threads()
	conf.table_hash = nil
	conf.checksum_list = nil
	Free_set_names()
	print_errors()
	stop_signal_thread()

	os.RemoveAll(FifoDirectory)
	log.Infof("Restore completed")
	if Logger != nil {
		Logger.Close()
	}
	if errors > 0 {
		os.Exit(EXIT_FAILURE)
	} else {
		os.Exit(EXIT_SUCCESS)
	}
}

// print_help prints myloader usage and option defaults to stdout.
func print_help() {
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [OPTION…] multi-threaded MySQL dumping\n", MYLOADER)
	pflag.PrintDefaults()
	Print_string("host", Hostname)
	Print_string("user", Username)
	Print_string("password", Password)
	Print_bool("ask-password", AskPassword)
	Print_int("port", Port)
	Print_string("socket", SocketPath)
	Print_string("protocol", Protocol_str)
	Print_bool("compress-protocol", Compress_protocol)
	Print_bool("ssl", Ssl)
	Print_string("ssl-mode", Ssl_mode)
	Print_string("key", Key)
	Print_string("cert", Cert)
	Print_string("ca", Ca)
	Print_string("capath", Capath)
	Print_string("cipher", Cipher)
	Print_string("tls-version", Tls_version)
	Print_list("regex", Regex_list)
	Print_string("source-db", SourceDb)

	Print_bool("skip-triggers", SkipTriggers)
	Print_bool("skip-constraints", SkipConstraints)
	Print_bool("skip-indexes", SkipIndexes)
	Print_bool("skip-post", SkipPost)
	Print_bool("no-data", NoData)

	Print_string("omit-from-file", TablesSkiplistFile)
	Print_string("tables-list", TablesList)
	Print_string("pmm-path", PmmPath)
	Print_string("pmm-resolution", PmmResolution)
	if EnableBinlog {
		Print_bool("enable-binlog", EnableBinlog)
	}
	if !optimize_keys {
		Print_string("optimize-keys", SKIP)
	} else if optimize_keys_per_table {
		Print_string("optimize-keys", AFTER_IMPORT_PER_TABLE)
	} else if optimize_keys_all_tables {
		Print_string("optimize-keys", AFTER_IMPORT_ALL_TABLES)
	} else {
		Print_string("optimize-keys", "")
	}

	Print_bool("no-schemas", NoSchemas)

	// Print_string("purge-mode", PurgeModeStr)
	Print_bool("local-infile", LocalInFile)
	Print_bool("disable-redo-log", DisableRedoLog)
	Print_string("checksum", checksum_str)
	Print_bool("overwrite-tables", OverwriteTables)
	Print_bool("overwrite-unsafe", OverwriteUnsafe)
	Print_uint("retry-count", RetryCount)
	Print_bool("serialized-table-creation", SerialTblCreation)
	Print_bool("stream", Stream != "")

	Print_uint("max-threads-per-table", MaxThreadsPerTable)
	Print_uint("max-threads-for-index-creation", MaxThreadsForIndexCreation)
	Print_uint("max-threads-for-post-actions", MaxThreadsForPostCreation)
	Print_uint("max-threads-for-schema-creation", MaxThreadsForSchemaCreation)
	Print_string("exec-per-thread", ExecPerThread)
	Print_string("exec-per-thread-extension", ExecPerThreadExtension)

	Print_int("rows", Rows)
	Print_uint("queries-per-transaction", CommitCount)
	Print_bool("append-if-not-exist", append_if_not_exist)
	Print_string("set-names", Set_names_in_conn_by_default)

	Print_bool("skip-definer", SkipDefiner)
	Print_bool("help", Help)

	Print_string("directory", InputDirectory)
	Print_string("logfile", LogFile)

	Print_string("database", DB)
	Print_string("quote-character", Identifier_quote_character_str)
	Print_bool("resume", Resume)
	Print_uint("threads", NumThreads)
	Print_bool("version", ProgramVersion)
	Print_bool("verbose", Verbose != 0)
	Print_bool("debug", Debug)
	Print_string("defaults-file", DefaultsFile)
	Print_string("defaults-extra-file", DefaultsExtraFile)
	Print_string("fifodir", FifoDirectory)
	os.Exit(EXIT_SUCCESS)
	// os.Exit(EXIT_SUCCESS)
}
