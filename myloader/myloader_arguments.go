package myloader

import (
	"container/list"
	"github.com/go-ini/ini"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"os"
	"regexp"
	"strings"
	"sync"
)

type server_type int

const (
	SERVER_TYPE_UNKNOWN server_type = iota
	SERVER_TYPE_MYSQL
	SERVER_TYPE_TIDB
	SERVER_TYPE_MARIADB
	SERVER_TYPE_PERCONA
)

type OptionEntries struct {
	*FilterEntries           `ini:"myloader"`
	*StatementEntries        `ini:"myloader"`
	*PmmEntries              `ini:"myloader"`
	*ExecutionEntries        `ini:"myloader"`
	*ThreadsEntries          `ini:"myloader"`
	*CommonEntries           `ini:"myloader"`
	*ConnectionEntries       `int:"myloader"`
	*RegexEntries            `ini:"myloader"`
	*CommonConnectionEntries `ini:"myloader"`
	global                   *loadGlobal
}
type PmmEntries struct {
	PmmPath       string `json:"pmm_path,omitempty"  ini:"pmm_path"`            // which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
	PmmResolution string `json:"pmm_Resolution,omitempty" ini:"pmm_resolution"` // which default will be high
}
type checksum_modes int

const (
	CHECKSUM_SKIP checksum_modes = iota
	CHECKSUM_WARN
	CHECKSUM_FAIL
)

type loadGlobal struct {
	innodb_optimize_keys_per_table        bool
	innodb_optimize_keys_all_tables       bool
	innodb_optimize_keys                  bool
	quote_character_cli                   bool
	innodb_optimize_keys_str              string
	identifier_quote_character            string
	identifier_quote_character_str        string
	checksum_str                          string
	regex_list                            []string
	conf_per_table                        *configuration_per_table
	source_control_command                int
	checksum_mode                         checksum_modes
	db_hash_mutex                         *sync.Mutex
	db_hash                               map[string]*database
	tbl_hash                              map[string]string
	tables                                []string
	source_gtid                           string
	no_stream                             bool
	start_replica                         string
	stop_replica                          string
	start_replica_sql_thread              string
	stop_replica_sql_thread               string
	reset_replica                         string
	show_replica_status                   string
	show_all_replicas_status              string
	show_binary_log_status                string
	change_replication_source             string
	all_jobs_are_enqueued                 bool
	control_job_ended                     bool
	checksum_ok                           bool
	retry_count                           uint // 10
	sequences_mutex                       *sync.Mutex
	cjt_mutex                             *sync.Mutex
	cjt_cond                              *sync.WaitGroup
	cjt_paused                            bool
	log_output                            *os.File
	tables_skiplist                       []string
	errors                                int
	max_errors                            int
	re                                    *regexp.Regexp
	pmm                                   bool
	intermediate_queue_ended_local        bool
	dont_wait_for_schema_create           bool
	detected_server                       server_type
	refresh_db_queue                      *asyncQueue
	here_is_your_job                      *asyncQueue
	data_queue                            *asyncQueue
	connection_pool                       *asyncQueue
	restore_queues                        *asyncQueue
	free_results_queue                    *asyncQueue
	restore_threads                       []*GThreadFunc
	control_job_t                         *sync.WaitGroup
	signal_thread                         *sync.WaitGroup
	last_wait                             int64
	file_list_to_do                       *asyncQueue
	refresh_db_queue2                     *asyncQueue
	single_threaded_create_table          *sync.Mutex
	progress_mutex                        *sync.Mutex
	shutdown_triggered_mutex              *sync.Mutex
	purge_mode                            purge_mode
	shutdown_triggered                    bool
	ignore_set_list                       *list.List
	refresh_table_list_counter            int64
	database_db                           *database
	pause_mutex_per_thread                []*sync.Mutex
	threads                               []*GThreadFunc
	detailed_errors                       *restore_errors
	loader_td                             []*thread_data
	done_filename                         []string
	init_mutex                            *sync.Mutex
	progress                              int
	ignore_errors                         string
	ignore_errors_list                    []uint16
	total_data_sql_files                  int
	directory                             string
	set_session                           *GString
	set_names_statement                   string
	program_name                          string
	connection_defaults_file              string
	connection_default_file_group         string
	print_connection_details              int64
	start_intermediate_thread             *sync.Mutex
	metadata_mutex                        *sync.Mutex
	exec_process_id_mutex                 *sync.Mutex
	exec_per_thread_cmd                   []string
	exec_process_id                       map[string]string
	first_metadata                        bool
	stream_intermediate_thread            *sync.WaitGroup
	intermediate_queue                    *asyncQueue
	intermediate_queue_ended              bool
	schema_counter                        uint64
	intermediate_conf                     *configuration
	conf                                  *configuration
	schema_sequence_fix                   bool
	change_master_statement               string
	fifo_hash                             map[*os.File]*fifo
	fifo_table_mutex                      *sync.Mutex
	post_threads                          []*GThreadFunc
	schema_threads                        []*GThreadFunc
	index_threads                         []*GThreadFunc
	schema_td                             []*thread_data
	post_td                               []*thread_data
	index_td                              []*thread_data
	init_connection_mutex                 *sync.Mutex
	sync_mutex                            *sync.Mutex
	sync_mutex1                           *sync.Mutex
	sync_mutex2                           *sync.Mutex
	load_data_list_mutex                  *sync.Mutex
	load_data_list                        map[string]*sync.Mutex
	sync_threads_remaining                int64
	sync_threads_remaining1               int64
	sync_threads_remaining2               int64
	second_round                          bool
	innodb_optimize_keys_all_tables_queue *asyncQueue
	stream_thread                         *sync.WaitGroup
	set_session_hash                      map[string]string
	key_file                              *ini.File
	pwd                                   string
	filename_re                           *regexp.Regexp
	partition_re                          *regexp.Regexp
	set_global                            *GString
	set_global_back                       *GString
	product                               server_type
	major                                 int
	secondary                             int
	revision                              int
	set_global_hash                       map[string]string
	no_delete                             bool
	sequences_processed                   uint
	sequences                             uint
	end_restore_thread                    *io_restore_result // { NULL, NULL}
	release_connection_statement          *statement         //{0, 0, NULL, NULL, CLOSE, FALSE, NULL, 0};
	append_if_not_exist                   bool
}

type CommonConnectionEntries struct {
	// Compress_protocol bool   // Use compression on the MySQL connection
	Ssl bool `json:"ssl,omitempty" ini:"ssl"` // Connect using SSL
	//	Ssl_mode    string // Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
	Key  string `json:"key,omitempty" ini:"key"`   // The path name to the key file
	Cert string `json:"cert,omitempty" ini:"cert"` // The path name to the certificate file
	Ca   string `json:"ca,omitempty" ini:"ca"`     // The path name to the certificate authority file
	//	Capath      string // The path name to a directory that contains trusted SSL CA certificates in PEM format
	//	Cipher      string // A list of permissible ciphers to use for SSL encryption
	//	Tls_version string // Which protocols the server permits for encrypted connections
}
type ConnectionEntries struct {
	Hostname    string `json:"hostname,omitempty" ini:"host"`             // The host to connect to
	Username    string `json:"username,omitempty" ini:"user"`             // Username with the necessary privileges
	Password    string `json:"password,omitempty" ini:"password"`         // User password
	AskPassword bool   `json:"ask_password,omitempty" ini:"ask_password"` // Prompt For User password
	Port        int    `json:"port,omitempty" ini:"port"`                 // TCP/IP port to connect to
	SocketPath  string `json:"socket_path,omitempty" ini:"socket_path"`   // UNIX domain socket file to use for connection
	Protocol    string `json:"protocol,omitempty" ini:"protocol"`         // The protocol to use for connection (tcp, socket)
}

type CommonEntries struct {
	Help           bool   `json:"help" ini:"help"`
	InputDirectory string `json:"input_directory" ini:"input_directory"`
	LogFile        string `json:"log_file" ini:"log_file"`
	DB             string `json:"db" ini:"db"`
	QuoteCharacter string `json:"quote_character" ini:"quote_character"`
	ShowWarnings   bool   `json:"show_warnings" ini:"show_warnings"`
	Resume         bool   `json:"resume" ini:"resume"`
	KillAtOnce     bool   `json:"kill_at_once" ini:"kill_at_once"`
	BufferSize     uint   `json:"buffer_size" ini:"buffer_size"`
	NumThreads     uint   `json:"num_threads,omitempty" ini:"num_threads"`         // "Number of threads to use, default 4
	ProgramVersion bool   `json:"program_version,omitempty" ini:"program_version"` // Show the program version and exit
	// IdentifierQuoteCharacter string   `json:"identifier_quote_character,omitempty" ini:"identifier_quote_character"` // This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK
	Verbose           int    `json:"verbose,omitempty" ini:"verbose"`                         // Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2
	Debug             bool   `json:"debug,omitempty" ini:"debug"`                             // (automatically sets verbosity to 3),print more info
	DefaultsFile      string `json:"defaults_file,omitempty" ini:"defaults_file"`             // Use a specific defaults file. Default: /etc/cnf
	DefaultsExtraFile string `json:"defaults_extra_file,omitempty" ini:"defaults_extra_file"` // Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
	// FifoDirectory        string   `json:"fifo_directory"`                                                // Directory where the FIFO files will be created when needed. Default: Same as backup
	SourceControlCommand string   `json:"source_control_command,omitempty" ini:"source_control_command"` // Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS
	Logger               *os.File `json:"logger"`
}

type ThreadsEntries struct {
	MaxThreadsPerTable          uint   `json:"max_threads_per_table,omitempty" ini:"max_threads_per_table"`
	MaxThreadsForIndexCreation  uint   `json:"max_threads_for_index_creation,omitempty" ini:"max_threads_for_index_creation"`
	MaxThreadsForPostCreation   uint   `json:"max_threads_for_post_creation,omitempty" ini:"max_threads_for_post_creation"`
	MaxThreadsForSchemaCreation uint   `json:"max_threads_for_schema_creation,omitempty" ini:"max_threads_for_schema_creation"`
	ExecPerThread               string `json:"exec_per_thread,omitempty" ini:"exec_per_thread"`
	ExecPerThreadExtension      string `json:"exec_per_thread_extension,omitempty" ini:"exec_per_thread_extension"`
}
type RegexEntries struct {
	Regex string `json:"regex,omitempty" ini:"regex"` // Regular expression for 'db.table' matching
}
type FilterEntries struct {
	SourceDb           string `json:"source_db,omitempty" ini:"source_db"`
	SkipTriggers       bool   `json:"skip_triggers,omitempty" ini:"skip_triggers"`
	SkipPost           bool   `json:"skip_post,omitempty" ini:"skip_post"`
	SkipConstraints    bool   `json:"skip_constraints,omitempty" ini:"skip_constraints"`
	SkipIndexes        bool   `json:"skip_indexes,omitempty" ini:"skip_indexes"`
	NoData             bool   `json:"no_data,omitempty" ini:"no_data"`
	TablesSkiplistFile string `json:"tables_skiplist_file,omitempty" ini:"tables_skiplist_file" ini:"tables_skiplist_file"` // File containing a list of database.table entries to skip, one per line (skips before applying regex option)
	TablesList         string `json:"tables_list,omitempty" ini:"tables_list" ini:"tables_list"`                            // Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
}

type StatementEntries struct {
	Rows             int    `json:"rows,omitempty" ini:"rows"`
	CommitCount      uint   `json:"commit_count,omitempty" ini:"commit_count"`
	AppendIfNotExist bool   `json:"append_if_not_exist,omitempty" ini:"append_if_not_exist"`
	SetNamesStr      string `json:"set_names_str,omitempty" ini:"set_names_str"`
	SkipDefiner      bool   `json:"skip_definer,omitempty" ini:"skip_definer"`
	IgnoreSet        string `json:"ignore_set,omitempty" ini:"ignore_set"`
}

type ExecutionEntries struct {
	EnableBinlog             bool   `json:"enable_binlog" ini:"enable_binlog"`
	InnodbOptimizeKeys       string `json:"innodb_optimize_keys" ini:"innodb_optimize_keys"`
	NoSchemas                bool   `json:"no_schemas" ini:"no_schemas"`
	PurgeModeStr             string `json:"purge_mode_str" ini:"purge_mode"`
	DisableRedoLog           bool   `json:"disable_redo_log" ini:"disable_redo_log"`
	CheckSum                 string `json:"checksum" ini:"checksum"`
	OverwriteTables          bool   `json:"overwrite_tables"`
	OverwriteUnsafe          bool   `json:"overwrite_unsafe" ini:"overwrite_unsafe"`
	RetryCount               uint   `json:"retry_count" ini:"retry_count"`
	SerialTblCreation        bool   `json:"serial_tbl_creation"`
	Stream                   bool   `json:"stream"`
	StreamOpt                string `json:"stream_opt"`
	RefreshTableListInterval int64  `json:"refresh_table_list_interval" ini:"refresh_table_list_interval"`
	IgnoreErrors             string `json:"ignore_errors" ini:"ignore_errors"`
	SetGtidPurge             bool   `json:"set_gtid_purge" ini:"set_gtid_purge"`
}

func arguments_callback(o *OptionEntries) bool {
	if o.InnodbOptimizeKeys != "" {
		o.global.innodb_optimize_keys_str = o.InnodbOptimizeKeys
		if strings.ToUpper(o.InnodbOptimizeKeys) == SKIP {
			o.global.innodb_optimize_keys = false
			o.global.innodb_optimize_keys_per_table = false
			o.global.innodb_optimize_keys_all_tables = false
			return true
		} else if strings.ToUpper(o.InnodbOptimizeKeys) == AFTER_IMPORT_PER_TABLE {
			o.global.innodb_optimize_keys_per_table = true
			o.global.innodb_optimize_keys_all_tables = false
			return true
		} else if strings.ToUpper(o.InnodbOptimizeKeys) == AFTER_IMPORT_ALL_TABLES {
			o.global.innodb_optimize_keys_all_tables = true
			o.global.innodb_optimize_keys_per_table = false
			return true
		} else {
			log.Fatalf("--innodb-optimize-keys accepts: after_import_per_table (default value), after_import_all_tables")

		}
	}
	if o.QuoteCharacter != "" {
		o.global.quote_character_cli = true
		if strings.ToUpper(o.QuoteCharacter) == "BACKTICK" || strings.ToUpper(o.QuoteCharacter) == "BT" || o.QuoteCharacter == "`" {
			o.global.identifier_quote_character = BACKTICK

		} else if strings.ToUpper(o.QuoteCharacter) == "DOUBLE_QUOTE" || strings.ToUpper(o.QuoteCharacter) == "DT" || o.QuoteCharacter == "\"" {
			o.global.identifier_quote_character = DOUBLE_QUOTE
			return true
		} else {
			log.Fatalf("--quote-character accepts: backtick, bt, `, double_quote, dt, \"")
		}
	}
	if o.CheckSum != "" {
		o.global.checksum_str = o.CheckSum
		if strings.ToUpper(o.CheckSum) == "FAIL" {
			o.global.checksum_mode = CHECKSUM_FAIL
		} else if strings.ToUpper(o.CheckSum) == "WARN" {
			o.global.checksum_mode = CHECKSUM_WARN
			return true
		} else if strings.ToUpper(o.CheckSum) == "SKIP" {
			o.global.checksum_mode = CHECKSUM_SKIP
			return true
		} else {
			log.Fatalf("--checksum accepts: fail (default), warn, skip")
		}
	}
	if o.InnodbOptimizeKeys == "" {
		o.global.innodb_optimize_keys_per_table = true
		o.global.innodb_optimize_keys_all_tables = false
	}
	if o.RefreshTableListInterval == 0 {
		o.RefreshTableListInterval = 100
	}

	return common_arguments_callback(o)
}

func newOptionEntries() *OptionEntries {
	o := new(OptionEntries)
	o.FilterEntries = new(FilterEntries)
	o.ConnectionEntries = new(ConnectionEntries)
	o.CommonEntries = new(CommonEntries)
	o.StatementEntries = new(StatementEntries)
	o.ThreadsEntries = new(ThreadsEntries)
	o.ExecutionEntries = new(ExecutionEntries)
	o.PmmEntries = new(PmmEntries)
	o.CommonConnectionEntries = new(CommonConnectionEntries)
	o.RegexEntries = new(RegexEntries)
	o.global = new(loadGlobal)
	return o
}

func (o *OptionEntries) loadOptionContext() {
	// Common
	pflag.BoolVar(&o.Help, "help", false, "Show help options")
	pflag.UintVarP(&o.BufferSize, "buffer-size", "b", 200000, "Queue buffer size")
	pflag.StringVarP(&o.InputDirectory, "directory", "d", "", "Directory of the dump to import")
	pflag.StringVarP(&o.LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVarP(&o.DB, "database", "B", "", "An alternative database to restore into")
	pflag.StringVarP(&o.QuoteCharacter, "quote-character", "Q", "", "Identifier quote character used in INSERT statements. "+
		"Posible values are: BACKTICK, bt, ` for backtick and DOUBLE_QUOTE, dt, \" for double quote. "+
		"Default: detect from dump if possible, otherwise BACKTICK")
	pflag.BoolVar(&o.ShowWarnings, "show-warnings", false, "If enabled, during INSERT IGNORE the warnings will be printed")
	pflag.BoolVar(&o.Resume, "resume", false, "Expect to find resume file in backup dir and will only process those files")
	pflag.BoolVarP(&o.KillAtOnce, "kill-at-once", "k", false, "When Ctrl+c is pressed it immediately terminates the process")

	pflag.UintVarP(&o.NumThreads, "threads", "t", 0, "Number of threads to use, 0 means to use number of CPUs. Default: 4")
	pflag.BoolVarP(&o.ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.IntVarP(&o.Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info")
	pflag.BoolVar(&o.Debug, "debug", false, "(automatically sets verbosity to 4),Print more info")
	pflag.StringVar(&o.DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/cnf")
	pflag.StringVar(&o.DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
	// pflag.StringVar(&o.FifoDirectory, "fifodir", "", "Directory where the FIFO files will be created when needed. Default: Same as backup")
	pflag.StringVar(&o.SourceControlCommand, "source-control-command", "", "Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS")
	// Threads
	pflag.UintVar(&o.MaxThreadsPerTable, "max-threads-per-table", 0, "Maximum number of threads per table to use, defaults to --threads")
	pflag.UintVar(&o.MaxThreadsForIndexCreation, "max-threads-for-index-creation", 0, "Maximum number of threads for index creation, default 4")
	pflag.UintVar(&o.MaxThreadsForPostCreation, "max-threads-for-post-actions", 0, "Maximum number of threads for post action like: constraints, procedure, views and triggers, default 1")
	pflag.UintVar(&o.MaxThreadsForSchemaCreation, "max-threads-for-schema-creation", 4, "Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4")
	pflag.StringVar(&o.ExecPerThread, "exec-per-thread", "", "Set the command that will receive by STDIN from the input file and write in the STDOUT")
	pflag.StringVar(&o.ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored")

	// Execution
	pflag.BoolVarP(&o.EnableBinlog, "enable-binlog", "e", false, "Enable binary logging of the restore data")
	pflag.StringVar(&o.InnodbOptimizeKeys, "innodb-optimize-keys", "", "Creates the table without the indexes unless SKIP is selected.\nIt will add the indexes right after complete the table restoration by default or after import all the tables.\nOptions: AFTER_IMPORT_PER_TABLE, AFTER_IMPORT_ALL_TABLES and SKIP. Default: AFTER_IMPORT_PER_TABLE")
	pflag.BoolVar(&o.NoSchemas, "no-schema", false, "Do not import table schemas and triggers")
	pflag.StringVar(&o.PurgeModeStr, "purge-mode", "", "This specify the truncate mode which can be: FAIL, NONE, DROP, TRUNCATE and DELETE. Default if not set: FAIL")
	pflag.BoolVar(&o.DisableRedoLog, "disable-redo-log", false, "Disables the REDO_LOG and enables it after, doesn't check initial status")
	pflag.StringVar(&o.CheckSum, "checksum", "", "Treat checksums: skip, fail(default), warn.")
	pflag.BoolVarP(&o.OverwriteTables, "overwrite-tables", "o", false, "Drop tables if they already exist")
	pflag.BoolVar(&o.OverwriteUnsafe, "overwrite-unsafe", false, "Same as --overwrite-tables but starts data load as soon as possible. May cause InnoDB deadlocks for foreign keys.")
	pflag.UintVar(&o.RetryCount, "retry-count", 0, "Lock wait timeout exceeded retry count, default 10 (currently only for DROP TABLE)")
	pflag.Int64Var(&o.RefreshTableListInterval, "metadata-refresh-interval", 0, "Every this amount of tables the internal metadata will be refreshed. If the amount of tables you have in your metadata file is high, then you should increase this value. Default: 100")

	pflag.BoolVar(&o.SerialTblCreation, "serialized-table-creation", false, "Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases")
	pflag.BoolVar(&o.Stream, "stream", false, "It will receive the stream from STDIN and creates the file in the disk before start processing")
	pflag.StringVar(&o.StreamOpt, "stream-opt", "", "Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given")
	pflag.StringVar(&o.IgnoreErrors, "ignore-errors", "", "Not increment error count and Warning instead of Critical in case of any of the comman separated error number list")
	pflag.BoolVar(&o.SetGtidPurge, "set-gtid-purged", false, "After import, it will execute the SET GLOBAL gtid_purged with the value found on source section of the metadata file")
	// pmm
	pflag.StringVar(&o.PmmPath, "pmm-path", "", "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution")
	pflag.StringVar(&o.PmmResolution, "pmm-resolution", "", "which default will be high")
	// Filter
	pflag.StringVarP(&o.SourceDb, "source-db", "s", "", "Database to restore")
	pflag.BoolVar(&o.SkipTriggers, "skip-triggers", false, "Do not import triggers. By default, it imports triggers")
	pflag.BoolVar(&o.SkipPost, "skip-post", false, "Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions")
	pflag.BoolVar(&o.SkipConstraints, "skip-constraints", false, "Do not import constraints. By default, it imports contraints")
	pflag.BoolVar(&o.SkipIndexes, "skip-indexes", false, "Do not import secondary index on InnoDB tables. By default, it import the indexes")
	pflag.BoolVar(&o.NoData, "no-data", false, "Do not dump or import table data")

	pflag.StringVarP(&o.TablesSkiplistFile, "omit-from-file", "O", "", "File containing a list of database.table entries to skip, one per line (skips before applying regex option)")
	pflag.StringVarP(&o.TablesList, "tables-list", "T", "", "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2")

	// Statement
	pflag.IntVarP(&o.Rows, "Rows", "r", 0, "Split the INSERT statement into this many Rows.")
	pflag.UintVarP(&o.CommitCount, "queries-per-transaction", "q", 0, "Number of queries per transaction, default 1000")
	pflag.BoolVar(&o.AppendIfNotExist, "append-if-not-exist", false, "Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented")
	pflag.StringVar(&o.SetNamesStr, "set-names", "", "Sets the names, use it at your own risk, default binary")
	pflag.BoolVar(&o.SkipDefiner, "skip-define", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")
	pflag.StringVar(&o.IgnoreSet, "ignore-set", "", "List of variables that will be ignored from the header of SET")

	// Connection
	pflag.StringVarP(&o.Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&o.Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&o.Password, "password", "p", "", "User password")
	pflag.BoolVarP(&o.AskPassword, "ask-password", "a", false, "Prompt For User password")
	pflag.IntVarP(&o.Port, "port", "P", 3306, "TCP/IP port to connect to")
	pflag.StringVarP(&o.SocketPath, "socket", "S", "", "UNIX domain socket file to use for connection")
	pflag.StringVar(&o.Protocol, "protocol", "tcp", "The protocol to use for connection (tcp, socket)")

	// regex_entries
	pflag.StringVarP(&o.Regex, "regex", "x", "", "Regular expression for 'db.table' matching")
	pflag.Parse()

}

func NewDefaultOption() *OptionEntries {
	o := new(OptionEntries)
	o.Port = 3306
	o.CommitCount = 1000
	o.InnodbOptimizeKeys = AFTER_IMPORT_PER_TABLE
	o.Verbose = 2
	o.NumThreads = 4
	o.MaxThreadsPerTable = 4
	o.MaxThreadsForIndexCreation = 4
	o.MaxThreadsForPostCreation = 4
	o.MaxThreadsForSchemaCreation = 4
	return o
}
