package myloader

import (
	"container/list"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/spf13/pflag"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	innodb_optimize_keys            bool = true
	innodb_optimize_keys_per_table  bool = true
	innodb_optimize_keys_all_tables bool
	quote_character_cli             bool
	directory                       string
	detailed_errors                 *restore_errors = &restore_errors{}
	sequences_processed             uint
	sequences                       uint
	sequences_mutex                 *sync.Mutex
	errors                          uint
	max_errors                      uint
	retry_count                     uint = 10
	load_data_list                  map[string]*sync.Mutex
	load_data_list_mutex            *sync.Mutex
	conf_per_table                  *Configuration_per_table
	set_session_hash                map[string]string
	set_global_hash                 map[string]string
	pmm                             bool
)

type schema_status int
type thread_states int
type file_type int

const (
	INIT file_type = iota
	SCHEMA_TABLESPACE
	SCHEMA_CREATE
	CJT_RESUME
	SCHEMA_TABLE
	DATA
	SCHEMA_VIEW
	SCHEMA_SEQUENCE
	SCHEMA_TRIGGER
	SCHEMA_POST
	CHECKSUM
	//  METADATA_TABLE
	METADATA_GLOBAL
	RESUME
	IGNORED
	LOAD_DATA
	SHUTDOWN
	INCOMPLETE
	DO_NOT_ENQUEUE
	THREAD
	INDEX
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
type db_connection struct {
	conn    *client.Conn
	err     error
	code    uint16
	warning uint16
}
type restore_errors struct {
	data_errors        uint64
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

type db_table struct {
	database                *database
	table                   string
	real_table              string
	object_to_export        *Object_to_export
	rows                    uint64
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

func myloader_initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = Initialize_hash_of_session_variables()
	if !EnableBinlog {
		set_session_hash["SQL_LOG_BIN"] = "0"
	}
	if CommitCount > 1 {
		set_session_hash["AUTOCOMMIT"] = "0"
	}
	return set_session_hash
}

func print_time(timespan time.Time) string {
	var now_time = time.Now().UnixMicro()
	var days = (now_time - timespan.UnixMicro()) / G_TIME_SPAN_DAY
	var hours = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY)) / G_TIME_SPAN_HOUR
	var minutes = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR)) / G_TIME_SPAN_MINUTE
	var seconds = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR) - (minutes * G_TIME_SPAN_MINUTE)) / G_TIME_SPAN_SECOND
	return fmt.Sprintf("%02d:%02d:%02d:%02d", days, hours, minutes, seconds)
}

func compare_by_time(a *db_table, b *db_table) bool {
	return a.finish_time.Sub(a.start_data_time).Microseconds() > b.finish_time.Sub(b.start_data_time).Microseconds()
}

func show_dbt(key any, dbt any, total any) {
	_ = key
	_ = dbt
	_ = total
	log.Infof("Table %s", key.(string))
}

func create_database(td *thread_data, database string) {
	var filename = fmt.Sprintf("%s-schema-create.sql%s", database, ExecPerThreadExtension)
	var filepath = fmt.Sprintf("%s/%s-schema-create.sql%s", directory, database, ExecPerThreadExtension)
	if G_file_test(filepath) {
		atomic.AddUint64(&detailed_errors.schema_errors, uint64(restore_data_from_file(td, filename, true, nil)))
	} else {
		var data *GString = G_string_new("CREATE DATABASE IF NOT EXISTS %s%s%s", Identifier_quote_character, database, Identifier_quote_character)
		if restore_data_in_gstring_extended(td, data, true, nil, M_critical, "Failed to create database: %s", database) != 0 {
			atomic.AddUint64(&detailed_errors.schema_errors, 1)
		}
		data = nil
	}
	return
}

func print_errors() {
	log.Infof("Errors found:")
	log.Infof("- Tablespace: %d", detailed_errors.tablespace_errors)
	log.Infof("- Schema:     %d", detailed_errors.schema_errors)
	log.Infof("- Data:       %d", detailed_errors.data_errors)
	log.Infof("- View:       %d", detailed_errors.view_errors)
	log.Infof("- Sequence:   %d", detailed_errors.sequence_errors)
	log.Infof("- Index:      %d", detailed_errors.index_errors)
	log.Infof("- Trigger:    %d", detailed_errors.trigger_errors)
	log.Infof("- Constraint: %d", detailed_errors.constraints_errors)
	log.Infof("- Post:       %d", detailed_errors.post_errors)
	log.Infof("Retries: %d", detailed_errors.retries)
}

func StartLoad() {
	var err error
	load_contex_entries()
	var conf = new(configuration)
	Initialize_share_common()
	if DB == "" && SourceDb != "" {
		DB = SourceDb
	}

	if Debug {
		Set_debug()
		err = Set_verbose()
	} else {
		err = Set_verbose()
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

	Initialize_common_options(MYLOADER)
	if ProgramVersion {
		Print_version(MYLOADER)
		os.Exit(EXIT_SUCCESS)
	}
	Hide_password()
	Ask_password()
	Initialize_set_names()
	load_data_list_mutex = G_mutex_new()
	load_data_list = make(map[string]*sync.Mutex)
	if PmmPath != "" {
		pmm = true
		if PmmResolution == "" {
			PmmResolution = "high"
		}
	} else if PmmPath != "" {
		pmm = true
		PmmPath = fmt.Sprintf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution", PmmResolution)
	}
	if pmm {
		// TODO
	}
	initialize_restore_job(PurgeModeStr)
	var current_dir = G_get_current_dir()
	if InputDirectory == "" {
		if Stream != "" {
			var datetime = time.Now()
			var datetimestr string
			datetimestr = datetime.Format("20060102-150405")
			directory = fmt.Sprintf("%s/%s-%s", current_dir, DIRECTORY, datetimestr)
			Create_backup_dir(directory, "")
		} else {
			if !Help {
				log.Fatalf("a directory needs to be specified, see --help")
			}
		}
	} else {
		if strings.HasPrefix(InputDirectory, "/") {
			directory = InputDirectory
		} else {
			directory = fmt.Sprintf("%s/%s", current_dir, InputDirectory)
		}
		if Stream != "" {
			if !G_file_test(InputDirectory) {
				Create_backup_dir(directory, "")

			} else {
				if !No_stream {
					log.Fatalf("Backup directory (-d) must not exist when --stream / --stream=TRADITIONAL")
				}
			}
		} else {
			if !G_file_test(InputDirectory) {
				log.Fatalf("the specified directory doesn't exists")
			}
			var p = fmt.Sprintf("%s/metadata", directory)
			if !G_file_test(p) {
				log.Fatalf("the specified directory %s is not a mydumper backup", directory)
			}
		}
	}
	/*if FifoDirectory != "" {
		if !strings.HasPrefix(FifoDirectory, "/") {
			var tmp_fifo_directory = FifoDirectory
			FifoDirectory = fmt.Sprintf("%s/%s", current_dir, tmp_fifo_directory)
		}
		create_fifo_dir(FifoDirectory)
	}*/
	if Help {
		print_help()
	}
	err = os.Chdir(directory)
	if TablesSkiplistFile != "" {
		err = Read_tables_skiplist(TablesSkiplistFile)
	}
	initialize_process(conf)
	initialize_common()
	Initialize_connection(MYLOADER)
	InitializeRegex("")

	go signal_thread(conf)
	var conn *DBConnection
	conn = Mysql_init()
	M_connect(conn)
	Set_session = G_string_new("")
	Set_global = G_string_new("")
	Set_global_back = G_string_new("")
	err = Detect_server_version(conn)
	Detected_server = Get_product()
	set_session_hash = myloader_initialize_hash_of_session_variables()
	set_global_hash = make(map[string]string)
	if Key_file != nil {
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_global_hash, "myloader_global_variables")
		Load_hash_of_all_variables_perproduct_from_key_file(Key_file, set_global_hash, "myloader_session_variables")
	}
	Refresh_set_session_from_hash(Set_session, set_session_hash)
	Refresh_set_global_from_hash(Set_global, Set_global_back, set_global_hash)
	Execute_gstring(conn, Set_session)
	Execute_gstring(conn, Set_global)
	Initialize_conf_per_table(conf_per_table)
	Load_per_table_info_from_key_file(Key_file, conf_per_table, nil)
	// identifier_quote_character_str = IdentifierQuoteCharacter
	if DisableRedoLog {
		if Get_major() == 8 && Get_secondary() == 0 && Get_revision() > 21 {
			log.Infof("Disabling redologs")
			m_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", M_critical, "DISABLE INNODB REDO LOG failed")
		} else {
			log.Errorf("Disabling redologs is not supported for version %d.%d.%d", Get_major(), Get_secondary(), Get_revision())
		}
	}
	conf.database_queue = G_async_queue_new(BufferSize)
	conf.table_queue = G_async_queue_new(BufferSize)
	conf.retry_queue = G_async_queue_new(BufferSize)
	conf.data_queue = G_async_queue_new(BufferSize)
	conf.post_table_queue = G_async_queue_new(BufferSize)
	conf.post_queue = G_async_queue_new(BufferSize)
	conf.index_queue = G_async_queue_new(BufferSize)
	conf.view_queue = G_async_queue_new(BufferSize)
	conf.ready = G_async_queue_new(BufferSize)
	conf.pause_resume = G_async_queue_new(BufferSize)
	conf.table_list_mutex = G_mutex_new()
	conf.stream_queue = G_async_queue_new(BufferSize)
	conf.table_hash = make(map[string]*db_table)
	conf.table_hash_mutex = G_mutex_new()
	if G_file_test("resume") {
		if !Resume {
			log.Fatalf("Resume file found but --resume has not been provided")
		}
	} else {
		if Resume {
			log.Fatalf("Resume file not found")
		}
	}
	initialize_connection_pool(conn)
	var t *thread_data = new(thread_data)
	initialize_thread_data(t, conf, WAITING, 0, nil)
	if database_db != nil {
		if !NoSchemas {
			create_database(t, database_db.real_database)
		}
		database_db.schema_state = CREATED
	}
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
			log.Fatalf("We don't expect to find resume files in a stream scenario")
		}
		initialize_stream(conf)
	}
	initialize_loader_threads(conf)
	if Stream != "" {
		wait_stream_to_finish()
	} else {
		process_directory(conf)
	}
	var dbt *db_table
	for _, dbt = range conf.table_list {
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
	G_async_queue_unref(conf.ready)
	conf.ready = nil
	G_async_queue_unref(conf.data_queue)
	conf.data_queue = nil
	var cd *connection_data = close_restore_thread(true)
	cd.in_use.Lock()
	if DisableRedoLog {
		m_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", M_critical, "ENABLE INNODB REDO LOG failed")
	}
	var checksum_ok bool = true
	// G_async_queue_unref(conf.data_queue)
	for _, dbt = range conf.table_list {
		checksum_ok = checksum_dbt(dbt, conn)
	}

	if checksum_mode != CHECKSUM_SKIP {
		var d *database
		for _, d = range db_hash {
			if d.schema_checksum != "" && !NoSchemas {
				checksum_ok = checksum_database_template(d.real_database, d.schema_checksum, cd.thrconn, "Schema create checksum", Checksum_database_defaults)
			}
			if d.post_checksum != "" && !SkipPost {
				checksum_ok = checksum_database_template(d.real_database, d.post_checksum, cd.thrconn, "Post checksum", Checksum_process_structure)
			}
			if d.triggers_checksum != "" && !SkipTriggers {
				checksum_ok = checksum_database_template(d.real_database, d.triggers_checksum, cd.thrconn, "Triggers checksum", Checksum_trigger_structure_from_database)
			}
		}
	}
	var i uint
	for i = 1; i < NumThreads; i++ {
		close_restore_thread(false)
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
			log.Fatalf("Restore directory not removed: %s", directory)
		}
	}
	if change_master_statement != nil {
		var i int
		var line = strings.Split(change_master_statement.Str.String(), ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				var str = G_string_new(line[i])
				G_string_append(str, ";")
				m_query(cd.thrconn, str.Str.String(), M_warning, fmt.Sprintf("Sending CHANGE MASTER: %s", str.Str.String()))
				G_string_free(str, true)
			}
		}
	}
	G_async_queue_unref(conf.database_queue)
	G_async_queue_unref(conf.table_queue)
	G_async_queue_unref(conf.retry_queue)
	G_async_queue_unref(conf.pause_resume)
	G_async_queue_unref(conf.post_table_queue)
	G_async_queue_unref(conf.post_queue)
	Execute_gstring(conn, Set_global_back)
	err = cd.thrconn.Close()
	free_loader_threads()
	if pmm {
		kill_pmm_thread()
	}
	print_errors()
	stop_signal_thread()

	if Logger != nil {
		Logger.Close()
	}
	if errors > 0 {
		os.Exit(EXIT_FAILURE)
	} else {
		os.Exit(EXIT_SUCCESS)
	}
}

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

	Print_bool("enable-binlog", EnableBinlog)
	if !innodb_optimize_keys {
		Print_string("innodb-optimize-keys", SKIP)
	} else if innodb_optimize_keys_per_table {
		Print_string("innodb-optimize-keys", AFTER_IMPORT_PER_TABLE)
	} else if innodb_optimize_keys_all_tables {
		Print_string("innodb-optimize-keys", AFTER_IMPORT_ALL_TABLES)
	} else {
		Print_string("innodb-optimize-keys", "")
	}

	Print_bool("no-schemas", NoSchemas)

	Print_string("purge-mode", PurgeModeStr)
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
	Print_string("set-names", SetNamesStr)

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
