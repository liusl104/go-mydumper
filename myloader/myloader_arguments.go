package myloader

import (
	"github.com/go-ini/ini"
	"github.com/spf13/pflag"
	"os"
	"regexp"
	"strings"
	"sync"
)

const (
	AFTER_IMPORT_PER_TABLE       = "AFTER_IMPORT_PER_TABLE"
	AFTER_IMPORT_ALL_TABLES      = "AFTER_IMPORT_ALL_TABLES"
	ZSTD_EXTENSION               = ".zst"
	GZIP_EXTENSION               = ".gz"
	VERSION                      = "0.15.1-3"
	DB_LIBRARY                   = "MySQL"
	MYSQL_VERSION_STR            = "8.0.31"
	MIN_THREAD_COUNT             = 2
	DEFAULTS_FILE                = "/etc/mydumper.cnf"
	SEQUENCE                     = "sequence"
	TRIGGER                      = "trigger"
	POST                         = "post"
	TABLESPACE                   = "tablespace"
	CREATE_DATABASE              = "create database"
	VIEW                         = "view"
	INDEXES                      = "indexes"
	CONSTRAINTS                  = "constraints"
	RESTORE_JOB_RUNNING_INTERVAL = 10
)

const (
	BINARY = "binary"
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
	Filter           *FilterEntries           `ini:"myloader"`
	Statement        *StatementEntries        `ini:"myloader"`
	Pmm              *PmmEntries              `ini:"myloader"`
	Execution        *ExecutionEntries        `ini:"myloader"`
	Threads          *ThreadsEntries          `ini:"myloader"`
	Common           *CommonEntries           `ini:"myloader"`
	Connection       *ConnectionEntries       `int:"myloader"`
	Regex            *RegexEntries            `ini:"myloader"`
	CommonConnection *CommonConnectionEntries `ini:"myloader"`
	global           *loadGlobal
}
type PmmEntries struct {
	PmmPath       string `json:"pmm_path,omitempty"  ini:"pmm_path"`            // which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
	PmmResolution string `json:"pmm_Resolution,omitempty" ini:"pmm_resolution"` // which default will be high
}
type loadGlobal struct {
	innodb_optimize_keys_per_table        bool
	innodb_optimize_keys_all_tables       bool
	innodb_optimize_keys                  bool
	db_hash_mutex                         *sync.Mutex
	db_hash                               map[string]*database
	tbl_hash                              map[string]string
	tables                                []string
	log_output                            *os.File
	tables_skiplist                       []string
	re                                    *regexp.Regexp
	pmm                                   bool
	intermediate_queue_ended_local        bool
	dont_wait_for_schema_create           bool
	detected_server                       server_type
	refresh_db_queue                      *asyncQueue
	here_is_your_job                      *asyncQueue
	data_queue                            *asyncQueue
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
	pause_mutex_per_thread                []*sync.Mutex
	threads                               []*sync.WaitGroup
	detailed_errors                       *restore_errors
	loader_td                             []*thread_data
	init_mutex                            *sync.Mutex
	progress                              int
	total_data_sql_files                  int
	directory                             string
	set_session                           string
	set_names_statement                   string
	program_name                          string
	connection_defaults_file              string
	connection_default_file_group         string
	print_connection_details              int64
	start_intermediate_thread             *sync.Mutex
	metadata_mutex                        *sync.Mutex
	exec_process_id_mutex                 *sync.Mutex
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
	post_threads                          []*sync.WaitGroup
	schema_threads                        []*sync.WaitGroup
	index_threads                         []*sync.WaitGroup
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
	set_global                            string
	set_global_back                       string
	product                               server_type
	major                                 int
	secondary                             int
	revision                              int
	set_global_hash                       map[string]string
	identifier_quote_character_str        string
	no_delete                             bool
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
	Socket      string `json:"socket,omitempty" ini:"socket"`             // UNIX domain socket file to use for connection
	Protocol    string `json:"protocol,omitempty" ini:"protocol"`         // The protocol to use for connection (tcp, socket)
}

type CommonEntries struct {
	Help                     bool     `json:"help" ini:"help"`
	InputDirectory           string   `json:"input_directory" ini:"input_directory"`
	LogFile                  string   `json:"log_file" ini:"log_file"`
	DB                       string   `json:"db" ini:"db"`
	Resume                   bool     `json:"resume" ini:"resume"`
	BufferSize               uint     `json:"buffer_size" ini:"buffer_size"`
	NumThreads               uint     `json:"num_threads,omitempty" ini:"num_threads"`                               // "Number of threads to use, default 4
	ProgramVersion           bool     `json:"program_version,omitempty" ini:"program_version"`                       // Show the program version and exit
	IdentifierQuoteCharacter string   `json:"identifier_quote_character,omitempty" ini:"identifier_quote_character"` // This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK
	Verbose                  int      `json:"verbose,omitempty" ini:"verbose"`                                       // Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2
	Debug                    bool     `json:"debug,omitempty" ini:"debug"`                                           // (automatically sets verbosity to 3),print more info
	DefaultsFile             string   `json:"defaults_file,omitempty" ini:"defaults_file"`                           // Use a specific defaults file. Default: /etc/cnf
	DefaultsExtraFile        string   `json:"defaults_extra_file,omitempty" ini:"defaults_extra_file"`               // Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
	FifoDirectory            string   // Directory where the FIFO files will be created when needed. Default: Same as backup
	Logger                   *os.File `json:"logger"`
}

type ThreadsEntries struct {
	MaxThreadsPerTable          uint   `json:"max_threads_per_table,omitempty" ini:"max_threads_per_table"`
	MaxThreadsPerTableHard      uint   `json:"max_threads_per_table_hard,omitempty" ini:"max_threads_per_table_hard"`
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
}

type ExecutionEntries struct {
	EnableBinlog       bool   `json:"enable_binlog,omitempty"`
	InnodbOptimizeKeys string `json:"innodb_optimize_keys,omitempty"`
	PurgeModeStr       string `json:"purge_mode_str,omitempty"`
	DisableRedoLog     bool   `json:"disable_redo_log,omitempty"`
	OverwriteTables    bool   `json:"overwrite_tables,omitempty"`
	SerialTblCreation  bool   `json:"serial_tbl_creation,omitempty"`
	Stream             bool   `json:"stream,omitempty"`
	StreamOpt          string `json:"stream_opt,omitempty"`
}

func (o *OptionEntries) arguments_callback() bool {
	if o.Execution.InnodbOptimizeKeys != "" {
		o.global.innodb_optimize_keys = true
		if strings.ToUpper(o.Execution.InnodbOptimizeKeys) == AFTER_IMPORT_PER_TABLE {
			o.global.innodb_optimize_keys_per_table = true
			o.global.innodb_optimize_keys_all_tables = false
			return true
		}
		if strings.ToUpper(o.Execution.InnodbOptimizeKeys) == AFTER_IMPORT_ALL_TABLES {
			o.global.innodb_optimize_keys_all_tables = true
			o.global.innodb_optimize_keys_per_table = false
			return true
		}
	} else {
		o.global.innodb_optimize_keys = true
		o.global.innodb_optimize_keys_all_tables = false
		o.global.innodb_optimize_keys_per_table = true
		return true

	}
	return false
}

func newOptionEntries() *OptionEntries {
	o := new(OptionEntries)
	o.global = new(loadGlobal)
	o.Common = new(CommonEntries)
	o.Filter = new(FilterEntries)
	o.Connection = new(ConnectionEntries)
	o.Threads = new(ThreadsEntries)
	o.Pmm = new(PmmEntries)
	o.Statement = new(StatementEntries)
	o.Execution = new(ExecutionEntries)
	o.Regex = new(RegexEntries)
	return o
}

func loadOptionContext(o *OptionEntries) {
	// Common
	pflag.BoolVar(&o.Common.Help, "help", false, "Show help options")
	pflag.StringVarP(&o.Common.InputDirectory, "directory", "d", "", "Directory of the dump to import")
	pflag.StringVarP(&o.Common.LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVarP(&o.Common.DB, "database", "B", "", "An alternative database to restore into")
	pflag.BoolVar(&o.Common.Resume, "resume", false, "Expect to find resume file in backup dir and will only process those files")
	pflag.StringVar(&o.Common.DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/cnf")
	pflag.StringVar(&o.Common.DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
	pflag.UintVarP(&o.Common.NumThreads, "threads", "t", 4, "Number of threads to use")
	pflag.BoolVarP(&o.Common.ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.StringVar(&o.Common.IdentifierQuoteCharacter, "identifier-quote-character", "", "This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK")
	pflag.IntVarP(&o.Common.Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info")
	pflag.BoolVar(&o.Common.Debug, "debug", false, "(automatically sets verbosity to 4),Print more info")
	// Threads
	pflag.UintVar(&o.Threads.MaxThreadsPerTable, "max-threads-per-table", 4, "Maximum number of threads per table to use")
	pflag.UintVar(&o.Threads.MaxThreadsPerTableHard, "max-threads-per-table-hard", 4, "Maximum hard number of threads per table to use, we are not going to use more than this amount of threads per table")
	pflag.UintVar(&o.Threads.MaxThreadsForIndexCreation, "max-threads-for-index-creation", 4, "Maximum number of threads for index creation")
	pflag.UintVar(&o.Threads.MaxThreadsForPostCreation, "max-threads-for-post-actions", 4, "Maximum number of threads for post action like: constraints, procedure, views and triggers")
	pflag.UintVar(&o.Threads.MaxThreadsForSchemaCreation, "max-threads-for-schema-creation", 4, "Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation")
	pflag.StringVar(&o.Threads.ExecPerThread, "exec-per-thread", "", "Set the command that will receive by STDIN from the input file and write in the STDOUT")
	pflag.StringVar(&o.Threads.ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored")
	// Execution
	pflag.BoolVarP(&o.Execution.EnableBinlog, "enable-binlog", "e", false, "Enable binary logging of the restore data")
	pflag.StringVar(&o.Execution.InnodbOptimizeKeys, "innodb-optimize-keys", AFTER_IMPORT_PER_TABLE, "Creates the table without the indexes and it adds them at the end. Options: AFTER_IMPORT_PER_TABLE and AFTER_IMPORT_ALL_TABLES")
	pflag.StringVar(&o.Execution.PurgeModeStr, "purge-mode", "FAIL", "This specify the truncate mode which can be: FAIL, NONE, DROP, TRUNCATE and DELETE")
	pflag.BoolVar(&o.Execution.DisableRedoLog, "disable-redo-log", false, "Disables the REDO_LOG and enables it after, doesn't check initial status")
	pflag.BoolVarP(&o.Execution.OverwriteTables, "overwrite-tables", "o", false, "Drop tables if they already exist")
	pflag.BoolVar(&o.Execution.SerialTblCreation, "serialized-table-creation", false, "Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases")
	pflag.BoolVar(&o.Execution.Stream, "stream", false, "It will receive the stream from STDIN and creates the file in the disk before start processing")
	pflag.StringVar(&o.Execution.StreamOpt, "stream-opt", "", "accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given")
	// Filter
	pflag.StringVarP(&o.Filter.SourceDb, "source-db", "s", "", "Database to restore")
	pflag.BoolVar(&o.Filter.SkipTriggers, "skip-triggers", false, "Do not import triggers. By default, it imports triggers")
	pflag.BoolVar(&o.Filter.SkipPost, "skip-post", false, "Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions")
	pflag.BoolVar(&o.Filter.NoData, "no-data", false, "Do not dump or import table data")
	pflag.StringVarP(&o.Filter.TablesSkiplistFile, "omit-from-file", "O", "", "File containing a list of database.table entries to skip, one per line (skips before applying regex option)")
	pflag.StringVarP(&o.Filter.TablesList, "tables-list", "T", "", "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2")

	// Statement
	pflag.IntVarP(&o.Statement.Rows, "Rows", "r", 0, "Split the INSERT statement into this many Rows.")
	pflag.UintVarP(&o.Statement.CommitCount, "queries-per-transaction", "q", 1000, "Number of queries per transaction.")
	pflag.BoolVar(&o.Statement.AppendIfNotExist, "append-if-not-exist", false, "Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented")
	pflag.StringVar(&o.Statement.SetNamesStr, "set-names", BINARY, "Sets the names, use it at your own risk")
	pflag.BoolVar(&o.Statement.SkipDefiner, "skip-define", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")

	// Connection
	pflag.StringVarP(&o.Connection.Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&o.Connection.Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&o.Connection.Password, "password", "p", "", "User password")
	pflag.BoolVarP(&o.Connection.AskPassword, "ask-password", "a", false, "Prompt For User password")
	pflag.IntVarP(&o.Connection.Port, "port", "P", 3306, "TCP/IP port to connect to")
	pflag.StringVarP(&o.Connection.Socket, "socket", "S", "", "UNIX domain socket file to use for connection")
	pflag.StringVar(&o.Connection.Protocol, "protocol", "tcp", "The protocol to use for connection (tcp, socket)")
	pflag.StringVar(&o.Common.FifoDirectory, "fifodir", "", "Directory where the FIFO files will be created when needed. Default: Same as backup")
	// pmm
	pflag.StringVar(&o.Pmm.PmmPath, "pmm-path", "", "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution")
	pflag.StringVar(&o.Pmm.PmmResolution, "pmm-resolution", "", "which default will be high")
	// regex_entries
	pflag.StringVarP(&o.Regex.Regex, "regex", "x", "", "Regular expression for 'db.table' matching")
	pflag.Parse()

}

func NewDefaultOption() *OptionEntries {
	o := new(OptionEntries)
	o.Connection.Port = 3306
	o.Statement.CommitCount = 1000
	o.Execution.InnodbOptimizeKeys = AFTER_IMPORT_PER_TABLE
	o.Common.Verbose = 2
	o.Common.NumThreads = 4
	o.Threads.MaxThreadsPerTable = 4
	o.Threads.MaxThreadsForIndexCreation = 4
	o.Threads.MaxThreadsPerTableHard = 4
	o.Threads.MaxThreadsForPostCreation = 4
	o.Threads.MaxThreadsForSchemaCreation = 4
	return o
}
