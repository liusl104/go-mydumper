package mydumper

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	GZIP           = "GZIP"
	ZSTD           = "ZSTD"
	INSERT_ARG     = "INSERT"
	LOAD_DATA_ARG  = "LOAD_DATA"
	CSV_ARG        = "CSV"
	CLICKHOUSE_ARG = "CLICKHOUSE"
	SQL_INSERT     = 0
	LOAD_DATA      = 1
	CSV            = 2
	CLICKHOUSE     = 3
	SQL            = "sql"
	DAT            = "dat"
	TRADITIONAL    = 0
	AWS            = 1
)

func NewDefaultEntries() *OptionEntries {
	o := newEntries()
	before_arguments_callback(o)
	o.CommonOptionEntries.BufferSize = 200000
	o.Extra.CompressMethod = GZIP
	o.QueryRunning.LongqueryRetryInterval = 60
	o.QueryRunning.Longquery = 60
	// o.Daemon.SnapshotCount = 2
	// o.Daemon.SnapshotInterval = 60
	o.Chunks.MaxThreadsPerTable = math.MaxUint
	o.Chunks.MaxRows = 100000
	o.Statement.StatementSize = 1000000
	o.Connection.Port = 3306
	o.Common.NumThreads = g_get_num_processors()
	o.Common.Verbose = 2
	o.Statement.SetNamesStr = BINARY
	o.global.insert_statement = INSERT
	arguments_callback(o)
	common_arguments_callback(o)
	identifier_quote_character_arguments_callback(o)
	stream_arguments_callback(o)
	format_arguments_callback(o)
	row_arguments_callback(o)
	connection_arguments_callback(o)
	return o
}

type OptionEntries struct {
	global              *globalEntries           // run internal variables
	CommonOptionEntries *CommonOptionEntries     `json:"commonOptionEntries" ini:"mydumper"`
	Extra               *ExtraEntries            `json:"extra" ini:"mydumper"`             // Extra module
	Lock                *LockEntries             `json:"lock" ini:"mydumper"`              // Lock module
	QueryRunning        *QueryRunningEntries     `json:"query_running" ini:"mydumper"`     // QueryRunning module
	Exec                *ExecEntries             `json:"exec" ini:"mydumper"`              // Exec module
	Pmm                 *PmmEntries              `json:"pmm" ini:"mydumper"`               // Pmm module
	Daemon              *DaemonEntries           `json:"daemon" ini:"mydumper"`            // Daemon module
	Chunks              *ChunksEntries           `json:"chunks" ini:"mydumper"`            // Chunks module
	Checksum            *ChecksumEntries         `json:"checksum" ini:"mydumper"`          // Checksum module
	Filter              *FilterEntries           `json:"filter" ini:"mydumper"`            // Filter module
	Objects             *ObjectsEntries          `json:"objects" ini:"mydumper"`           // Objects module
	Statement           *StatementEntries        `json:"statement" ini:"mydumper"`         // Statement module
	Common              *CommonEntries           `json:"common" ini:"mydumper"`            // Common module
	CommonFilter        *CommonFilterEntries     `json:"common_filter" ini:"mydumper"`     // CommonFilter module
	CommonConnection    *CommonConnectionEntries `json:"common_connection" ini:"mydumper"` // CommonConnection module
	Connection          *ConnectionEntries       `json:"connection" ini:"mydumper"`        // Connection module
	Regex               *RegexEntries            `json:"regex" ini:"mydumper"`             // Regex module
	Stream              *StreamEntries           `json:"stream" ini:"mydumper"`            // Stream module
}

type CommonOptionEntries struct {
	BufferSize           int    `json:"buffer_size" ini:"buffer_size"`                 // queue buffer size
	OutputDirectoryParam string `json:"output_Directory_Param" ini:"output_directory"` // Directory to output files to
	Help                 bool   `json:"help" ini:"help"`                               // Show help options
	LogFile              string `json:"log_file" ini:"log_file"`                       // Log file name to use, by default stdout is used
	DiskLimits           string `json:"disk_limits" ini:"disk_limits"`                 // Set the limit to pause and resume if determines there is no enough disk space. Accepts values like: '<resume>:<pause>' in MB. For instance: 100:500 will pause when there is only 100MB free and will resume if 500MB are available
}

type globalEntries struct {
	major                                       int
	secondary                                   int
	revision                                    int
	product                                     server_type
	re_list                                     []*regexp.Regexp
	filename_re                                 *regexp.Regexp
	partition_re                                *regexp.Regexp
	tables_skiplist                             []string
	set_names_statement                         string
	detected_server                             server_type
	no_delete                                   bool
	no_stream                                   bool
	key_file                                    *ini.File
	connection_defaults_file                    string
	connection_default_file_group               string
	program_name                                string
	print_connection_details                    int64
	available_pids                              *asyncQueue
	close_file_queue                            *asyncQueue
	stream_queue                                *asyncQueue
	metadata_partial_queue                      *asyncQueue
	initial_metadata_queue                      *asyncQueue
	initial_metadata_lock_queue                 *asyncQueue
	metadata_partial_writer_alive               bool
	pipe_creation                               *sync.Mutex
	ref_table_mutex                             *sync.Mutex
	ref_table                                   map[string]string
	table_number                                int
	tablecol                                    uint
	exec_per_thread_extension                   string
	database_hash                               map[string]*database
	database_hash_mutex                         *sync.Mutex
	main_connection                             *client.Conn
	second_conn                                 *client.Conn
	exec_command_thread                         []*sync.WaitGroup
	pid_file_table                              map[*command]string
	num_exec_threads                            uint
	exec_command                                string
	file_hash                                   map[string]map[string][]string
	fifo_table_mutex                            *sync.Mutex
	exec_per_thread_cmd                         []string
	schema_sequence_fix                         bool
	dump_directory                              string
	database_counter                            int64
	min_rows_per_file                           uint64
	max_rows_per_file                           uint64
	rows_per_file                               uint64
	character_set_hash                          map[string]string
	character_set_hash_mutex                    *sync.Mutex
	non_innodb_table_mutex                      *sync.Mutex
	innodb_table_mutex                          *sync.Mutex
	view_schemas_mutex                          *sync.Mutex
	table_schemas_mutex                         *sync.Mutex
	trigger_schemas_mutex                       *sync.Mutex
	all_dbts_mutex                              *sync.Mutex
	init_mutex                                  *sync.Mutex
	ready_table_dump_mutex                      *sync.Mutex
	binlog_snapshot_gtid_executed               string
	less_locking_threads                        uint
	ignore                                      []string
	sync_wait                                   int
	it_is_a_consistent_backup                   bool
	need_dummy_read                             bool
	need_dummy_toku_read                        bool
	shutdown_triggered                          bool
	pmm                                         bool
	non_innodb_table                            *MList
	innodb_table                                *MList
	conf_per_table                              *configuration_per_table
	all_dbts                                    map[string]*db_table
	table_schemas                               []*db_table
	no_dump_sequences                           bool
	no_updated_tables                           []string
	tables                                      []string
	db_items                                    []string
	set_session                                 string
	pp                                          *function_pointer
	pause_at                                    uint
	resume_at                                   uint
	pause_mutex_per_thread                      []*sync.Mutex
	disk_check_thread                           *sync.WaitGroup
	sthread                                     *sync.WaitGroup
	pmmthread                                   *sync.WaitGroup
	stream_thread                               *sync.WaitGroup
	metadata_partial_writer_thread              *sync.WaitGroup
	chunk_builder                               *sync.WaitGroup
	threads                                     *sync.WaitGroup
	set_session_hash                            map[string]string
	set_global_hash                             map[string]string
	set_global                                  string
	set_global_back                             string
	output_directory                            string
	give_me_another_innodb_chunk_step_queue     *asyncQueue
	give_me_another_non_innodb_chunk_step_queue *asyncQueue
	log_output                                  *os.File
	fields_escaped_by                           string
	fields_enclosed_by                          string
	fields_terminated_by                        string
	lines_starting_by                           string
	lines_terminated_by                         string
	statement_terminated_by                     string
	insert_statement                            string
	starting_chunk_step_size                    uint64
	min_chunk_step_size                         uint64
	max_chunk_step_size                         uint64
	identifier_quote_character_str              string
	thread_pool                                 map[string]*Thread
	open_pipe                                   int64
	routine_type                                []string // {"FUNCTION", "PROCEDURE", "PACKAGE", "PACKAGE BODY"}
	nroutines                                   uint     // TODO 4
	rows_file_extension                         string   // SQL
	output_format                               int
	regex_list                                  []string
	show_binary_log_status                      string
	show_replica_status                         string
	identifier_quote_character_protect          func(r string) string
	message_dumping_data                        func(o *OptionEntries, tj *table_job)
	split_integer_tables                        bool
	headers                                     strings.Builder
	sql_mode                                    string
	start_replica                               string
	stop_replica                                string
	stop_replica_sql_thread                     string
	start_replica_sql_thread                    string
	reset_replica                               string
	show_all_replicas_status                    string
	change_replication_source                   string
	identifier_quote_character                  string
	replica_stopped                             bool
	source_control_command                      int
	server_version                              uint
	clickhouse                                  bool
	ignore_errors_list                          []uint16
	filename_regex                              string
}

type StreamEntries struct {
	Stream    bool   `json:"stream" ini:"stream"` // It will stream over STDOUT once the files has been written
	StreamOpt string `json:"stream_opt" ini:"stream_opt"`
}

type RegexEntries struct {
	Regex string `json:"regex" ini:"regex"` // Regular expression for 'db.table' matching
}

type ExtraEntries struct {
	ChunkFilesize          uint   `json:"chunk_filesize" ini:"chunk_filesize"`                         // Split tables into chunks of this output file size. This value is in MB
	ExitIfBrokenTableFound bool   `json:"exit_if_broken_table_found" ini:"exit_if_broken_table_found"` // Exits if a broken table has been found
	SuccessOn1146          bool   `json:"success_on_1146" ini:"success_on_1146"`                       // Not increment error count and Warning instead of Critical in case of table doesn't exist
	BuildEmptyFiles        bool   `json:"build_empty_files" ini:"build_empty_files"`                   // Build dump files even if no data available from table
	IgnoreGeneratedFields  bool   `json:"ignore_generated_fields" ini:"ignore_generated_fields"`       // Queries related to generated fields are not going to be executed.It will lead to restoration issues if you have generated columns
	OrderByPrimaryKey      bool   `json:"order_by_primary_key" ini:"order_by_primary_key"`             // Sort the data by Primary Key or Unique key if no primary key exists
	Compress               bool   `json:"compress" ini:"compress"`                                     // Compress output files
	CompressMethod         string `json:"compress_method" ini:"compress_method"`                       // CompressMethod Options: GZIP and ZSTD. Default: GZIP
	Compact                bool   `json:"compact" ini:"compact"`                                       // Give less verbose output. Disables header/footer constructs.
	UseDefer               bool   `json:"use_defer" ini:"use_defer"`                                   // Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables)
	CheckRowCount          bool   `json:"check_row_count" ini:"check_row_count"`                       // Run SELECT COUNT(*) and fail mydumper if dumped row count is different
	SourceData             int    `json:"source_data" ini:"source_data"`                               // It will include the options in the metadata file, to allow myloader to establish replication
}

type LockEntries struct {
	TidbSnapshot       string `json:"tidb_snapshot" ini:"tidb_snapshot"`               // Snapshot to use for TiDB
	NoLocks            bool   `json:"no_locks" ini:"no_locks"`                         // Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups
	UseSavepoints      bool   `json:"use_savepoints" ini:"use_savepoints"`             // Use savepoints to reduce metadata locking issues, needs SUPER privilege
	NoBackupLocks      bool   `json:"no_backup_locks" ini:"no_backup_locks"`           // Do not use Percona backup locks
	LockAllTables      bool   `json:"lock_all_tables" ini:"lock_all_tables"`           // Use LOCK TABLE for all, instead of FTWRL
	LessLocking        bool   `json:"less_locking" ini:"less_locking"`                 // Minimize locking time on InnoDB tables.
	TrxConsistencyOnly bool   `json:"trx_consistency_only" ini:"trx_consistency_only"` // Transactional consistency only
	SkipDdlLocks       bool   `json:"skip_ddl_locks" ini:"skip_ddl_locks"`             // Do not send DDL locks when possible
}

type QueryRunningEntries struct {
	LongqueryRetries       int    `json:"longquery_retries" ini:"longquery_retries"`               // Retry checking for long queries, default 0 (do not retry)
	LongqueryRetryInterval int    `json:"longquery_retry_interval" ini:"longquery_retry_interval"` // Time to wait before retrying the long query check in seconds, default 60
	Longquery              uint64 `json:"longquery" ini:"longquery"`                               // Set long query timer in seconds, default 60
	Killqueries            bool   `json:"killqueries" ini:"killqueries"`                           // Kill long running queries (instead of aborting)
}

type ExecEntries struct {
	// Num_exec_threads int    // Amount of threads to use with --exec
	// Exec_command     string // Command to execute using the file as parameter
	// Exec_per_thread           string // Set the command that will receive by STDIN and write in the STDOUT into the output file
	ExecPerThreadExtension string `json:"exec_per_thread_extension" ini:"exec_per_thread_extension" ` // Set the extension for the STDOUT file when --exec-per-thread is used
}

type PmmEntries struct {
	PmmPath       string `json:"pmm_path"  ini:"pmm_path"`            // which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
	PmmResolution string `json:"pmm_Resolution" ini:"pmm_resolution"` // which default will be high
}

type DaemonEntries struct {
	DaemonMode bool   `json:"daemon_mode" ini:"daemon_mode"` // Enable daemon mode
	PidFile    string `json:"pid_file" ini:"pid_file"`
	// SnapshotInterval int  `json:"snapshot_interval" ini:"snapshot_interval"` // Interval between each dump snapshot (in minutes), requires --daemon,default 60
	// 	SnapshotCount    int  `json:"snapshot_count" ini:"snapshot_count"`       // number of snapshots, default 2
}

type ChunksEntries struct {
	MaxThreadsPerTable uint   `json:"max_threads_per_table" ini:"max_threads_per_table"` // Maximum number of threads per table to use
	MaxRows            int    `json:"max_rows" ini:"max_rows"`                           // Limit the number of rows per block after the table is estimated, default 1000000. It has been deprecated, use --rows instead. Removed in future releases
	CharDeep           uint   `json:"char_deep" ini:"char_deep"`                         // Defines the amount of characters to use when the primary key is a string
	CharChunk          uint   `json:"char_chunk" ini:"char_chunk"`                       // Defines in how many pieces should split the table. By default we use the amount of threads
	RowsPerChunk       string `json:"rows_per_chunk" ini:"rows_per_chunk"`               // Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds
	SplitPartitions    bool   `json:"split_partitions" ini:"split_partitions"`           // Dump partitions into separate files. This options overrides the --rows option for partitioned tables.
}

type ChecksumEntries struct {
	DumpChecksums    bool `json:"dump_checksums" ini:"dump_checksums"`       // Dump checksums for all elements
	DataChecksums    bool `json:"data_checksums" ini:"data_checksums"`       // Dump table checksums with the data
	SchemaChecksums  bool `json:"schema_checksums" ini:"schema_checksums"`   // Dump schema table and view creation checksums
	RoutineChecksums bool `json:"routine_checksums" ini:"routine_checksums"` // Dump triggers, functions and routines checksums
}

type FilterEntries struct {
	DB             string `json:"database" ini:"database"`               // Log file name to use, by default stdout is used
	IgnoreEngines  string `json:"ignore_engines" ini:"ignore_engines"`   // Comma delimited list of storage engines to ignore
	WhereOption    string `json:"where_option" ini:"where_option"`       // Dump only selected records.
	UpdatedSince   int    `json:"updated_since" ini:"updated_since"`     // Use Update_time to dump only tables updated in the last U days
	PartitionRegex string `json:"partition_regex" ini:"partition_regex"` // Regex to filter by partition name.
}

type ObjectsEntries struct {
	NoSchemas       bool `json:"no_schemas" ini:"no_schemas"`             // Do not dump table schemas with the data and triggers
	DumpTablespaces bool `json:"dump_tablespaces" ini:"dump_tablespaces"` // Dump all the tablespaces.
	NoData          bool `json:"no_data" ini:"no_data"`                   // Do not dump table data
	DumpTriggers    bool `json:"dump_triggers" ini:"dump_triggers"`       // Dump triggers. By default, it do not dump triggers
	DumpEvents      bool `json:"dump_events" ini:"dump_events"`           // Dump events. By default, it do not dump events
	DumpRoutines    bool `json:"dump_routines" ini:"dump_routines"`       // Dump stored procedures and functions. By default, it do not dump stored procedures nor functions
	ViewsAsTables   bool `json:"views_as_tables" ini:"views_as_tables"`   // Export VIEWs as they were tables
	NoDumpViews     bool `json:"no_dump_views" ini:"no_dump_views"`       // Do not dump VIEWs
	SkipConstraints bool `json:"skip_constraints" ini:"skip_constraints"` // Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified
	SkipIndexes     bool `json:"skip_indexes" ini:"skip_indexes"`         // Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified
}

type StatementEntries struct {
	LoadData             bool   `json:"load_data" ini:"load_data"`                             // Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files
	Csv                  bool   `json:"csv" ini:"csv"`                                         // Automatically enables --load-data and set variables to export in CSV format.
	FieldsTerminatedByLd string `json:"fields_terminated_by_ld" ini:"fields_terminated_by_ld"` // Defines the character that is written between fields
	FieldsEnclosedByLd   string `json:"fields_enclosed_by_ld" ini:"fields_enclosed_by_ld"`     // Defines the character to enclose fields. Default: \"
	// FieldsEscapedBy         string `json:"fields_escaped_by" ini:"fields_escaped_by"`                   // Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\\'
	LinesStartingByLd       string `json:"lines_starting_by_ld" json:"lines_starting_by_ld"`            // Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
	LinesTerminatedByLd     string `json:"lines_terminated_by_ld" ini:"lines_terminated_by_ld"`         // Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
	StatementTerminatedByLd string `json:"statement_terminated_by_ld" ini:"statement_terminated_by_ld"` // This might never be used, unless you know what are you doing
	InsertIgnore            bool   `json:"insert_ignore" ini:"insert_ignore"`                           // Dump rows with INSERT IGNORE
	Replace                 bool   `json:"replace" ini:"replace"`                                       // Dump rows with REPLACE
	CompleteInsert          bool   `json:"complete_insert" ini:"complete_insert"`                       // Use complete INSERT statements that include column names
	HexBlob                 bool   `json:"hex_blob" ini:"hex_blob"`                                     // Dump binary columns using hexadecimal notation
	SkipDefiner             bool   `json:"skip_definer" ini:"skip_definer"`                             // Removes DEFINER from the CREATE statement. By default, statements are not modified
	StatementSize           int    `json:"statement_size" ini:"statement_size"`                         // Attempted size of INSERT statement in bytes, default 1000000
	SkipTz                  bool   `json:"skip_tz" ini:"skip_tz"`                                       // SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.
	SetNamesStr             string `json:"set_names_str" ini:"set_names_str"`                           // Sets the names, use it at your own risk, default binary
	OutputFormat            string `json:"output_format" int:"output_format"`                           // Set the output format which can be INSERT, LOAD_DATA, CSV or CLICKHOUSE. Default: INSERT
	IncludeHeader           bool   `json:"include_header" ini:"include_header"`                         // When --load-data or --csv is used, it will include the header with the column name
}

type CommonEntries struct {
	NumThreads               uint   `json:"num_threads" ini:"num_threads"`                               // "Number of threads to use, default 4
	ProgramVersion           bool   `json:"program_version" ini:"program_version"`                       // Show the program version and exit
	IdentifierQuoteCharacter string `json:"identifier_quote_character" ini:"identifier_quote_character"` // This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK
	Verbose                  int    `json:"verbose" ini:"verbose"`                                       // Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2
	Debug                    bool   `json:"debug" ini:"debug"`                                           // Turn on debugging output (automatically sets verbosity to 3)
	DefaultsFile             string `json:"defaults_file" ini:"defaults_file"`                           // Use a specific defaults file. Default: /etc/cnf
	DefaultsExtraFile        string `json:"defaults_extra_file" ini:"defaults_extra_file"`               // Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
	SourceControlCommand     string `json:"source_control_command" ini:"source_control_command"`         // Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS
	ClearDumpDir             bool   `json:"clear_dump_dir" ini:"clear_dump_dir"`                         // Clear output directory before dumping                         //
	DirtyDumpDir             bool   `json:"dirty_dump_dir" ini:"dirty_dump_dir"`                         // Overwrite output directory without clearing (beware of leftower chunks)
	// Fifo_directory             string // Directory where the FIFO files will be created when needed. Default: Same as backup
	Logger *os.File `json:"logger"`
}

type CommonFilterEntries struct {
	TablesSkiplistFile string `json:"tables_skiplist_file" ini:"tables_skiplist_file"` // File containing a list of database.table entries to skip, one per line (skips before applying regex option)
	TablesList         string `json:"tables_list" ini:"tables_list"`                   // Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
}

type CommonConnectionEntries struct {
	// Compress_protocol bool   // Use compression on the MySQL connection
	Ssl bool `json:"ssl" ini:"ssl"` // Connect using SSL
	//	Ssl_mode    string // Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
	Key  string `json:"key" ini:"key"`   // The path name to the key file
	Cert string `json:"cert" ini:"cert"` // The path name to the certificate file
	Ca   string `json:"ca" ini:"ca"`     // The path name to the certificate authority file
	//	Capath      string // The path name to a directory that contains trusted SSL CA certificates in PEM format
	//	Cipher      string // A list of permissible ciphers to use for SSL encryption
	//	Tls_version string // Which protocols the server permits for encrypted connections
}

type ConnectionEntries struct {
	Hostname     string `json:"hostname" ini:"host"`               // The host to connect to
	Username     string `json:"username" ini:"user"`               // Username with the necessary privileges
	Password     string `json:"password" ini:"password"`           // User password
	HidePassword string `json:"hide_password" ini:"hide_password"` // hide password
	AskPassword  bool   `json:"ask_password" ini:"ask_password"`   // Prompt For User password
	Port         int    `json:"port" ini:"port"`                   // TCP/IP port to connect to
	SocketPath   string `json:"socket_path" ini:"socket_path"`     // UNIX domain socket file to use for connection
	Protocol     string `json:"protocol" ini:"protocol"`           // The protocol to use for connection (tcp, socket)
}

func newEntries() *OptionEntries {
	o := new(OptionEntries)
	o.global = new(globalEntries)
	o.Lock = new(LockEntries)
	o.Extra = new(ExtraEntries)
	o.Statement = new(StatementEntries)
	o.Exec = new(ExecEntries)
	o.Objects = new(ObjectsEntries)
	o.Filter = new(FilterEntries)
	o.Pmm = new(PmmEntries)
	o.Checksum = new(ChecksumEntries)
	o.Chunks = new(ChunksEntries)
	o.Daemon = new(DaemonEntries)
	o.QueryRunning = new(QueryRunningEntries)
	o.CommonConnection = new(CommonConnectionEntries)
	o.Common = new(CommonEntries)
	o.CommonFilter = new(CommonFilterEntries)
	o.Connection = new(ConnectionEntries)
	o.Regex = new(RegexEntries)
	o.Stream = new(StreamEntries)
	o.CommonOptionEntries = new(CommonOptionEntries)
	return o
}

func commandEntries(o *OptionEntries) {
	// option

	pflag.BoolVarP(&o.CommonOptionEntries.Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&o.CommonOptionEntries.OutputDirectoryParam, "outputdir", "o", "", "Directory to output files to")
	pflag.IntVarP(&o.CommonOptionEntries.BufferSize, "buffer-size", "b", 200000, "Queue buffer size")
	// pflag.BoolVarP(&o.CommonOptionEntries.Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&o.CommonOptionEntries.LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVar(&o.CommonOptionEntries.DiskLimits, "disk-limits", "", "Set the limit to pause and resume if determines there is no enough disk space.\nAccepts values like: '<resume>:<pause>' in MB.\nFor instance: 100:500 will pause when there is only 100MB free and will\nresume if 500MB are available")
	// Stream
	pflag.BoolVar(&o.Stream.Stream, "stream", false, "It will stream over STDOUT once the files has been written")
	pflag.StringVar(&o.Stream.StreamOpt, "stream-opt", "", "NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given")
	// extra
	pflag.UintVarP(&o.Extra.ChunkFilesize, "chunk-filesize", "F", 0, "Split data files into pieces of this size in MB. Useful for myloader multi-threading.")
	pflag.BoolVar(&o.Extra.ExitIfBrokenTableFound, "exit-if-broken-table-found", false, "Exits if a broken table has been found")
	pflag.BoolVar(&o.Extra.SuccessOn1146, "success_on_1146", false, "Not increment error count and Warning instead of Critical in case of table doesn't exist")
	pflag.BoolVarP(&o.Extra.BuildEmptyFiles, "build_empty_files", "e", false, "Build dump files even if no data available from table")
	pflag.BoolVar(&o.Extra.IgnoreGeneratedFields, "no-check-generated-fields", false, "Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	pflag.BoolVar(&o.Extra.OrderByPrimaryKey, "order-by-primary", false, "Sort the data by Primary Key or Unique key if no primary key exists")
	pflag.BoolVarP(&o.Extra.Compress, "compress", "c", false, "Compress output files")
	pflag.StringVar(&o.Extra.CompressMethod, "compress-method", "", "using: Options: GZIP and ZSTD. Default: GZIP")
	pflag.BoolVar(&o.Extra.Compact, "compact", false, "Give less verbose output. Disables header/footer constructs.")
	pflag.BoolVar(&o.Extra.UseDefer, "use-defer", false, "Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables).")
	pflag.BoolVar(&o.Extra.CheckRowCount, "check-row-count", false, "Run SELECT COUNT(*) and fail mydumper if dumped row count is different")
	pflag.IntVar(&o.Extra.SourceData, "source-data", 0, "It will include the options in the metadata file, to allow myloader to establish replication.")

	// lock
	pflag.StringVarP(&o.Lock.TidbSnapshot, "tidb-snapshot", "z", "", "Snapshot to use for TiDB")
	pflag.BoolVarP(&o.Lock.NoLocks, "no-locks", "k", false, "Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups")
	pflag.BoolVar(&o.Lock.UseSavepoints, "use-savepoints", false, "Use savepoints to reduce metadata locking issues, needs SUPER privilege")
	pflag.BoolVar(&o.Lock.NoBackupLocks, "no-backup-locks", false, "Do not use Percona backup locks")
	pflag.BoolVar(&o.Lock.LockAllTables, "lock-all-tables", false, "Use LOCK TABLE for all, instead of FTWRL")
	pflag.BoolVar(&o.Lock.LessLocking, "less-locking", false, "Minimize locking time on InnoDB tables.")
	pflag.BoolVar(&o.Lock.TrxConsistencyOnly, "trx-consistency-only", false, "Transactional consistency only")
	pflag.BoolVar(&o.Lock.SkipDdlLocks, "skip-ddl-locks", false, "Do not send DDL locks when possible")

	// query running

	pflag.IntVar(&o.QueryRunning.LongqueryRetries, "long-query-retries", 0, "Retry checking for long queries, default 0 (do not retry)")
	pflag.IntVar(&o.QueryRunning.LongqueryRetryInterval, "long-query-retry-interval", 60, "Time to wait before retrying the long query check in seconds")
	pflag.Uint64VarP(&o.QueryRunning.Longquery, "long-query-guard", "l", 60, "Set long query timer in seconds")
	pflag.BoolVarP(&o.QueryRunning.Killqueries, "kill-long-queries", "K", false, "Kill long running queries (instead of aborting)")

	// exec
	// pflag.IntVar(&o.Exec.Num_exec_threads, "exec-threads", 0, "Amount of threads to use with --exec")
	// pflag.StringVar(&o.Exec.Exec_command, "exec", "", "Command to execute using the file as parameter")
	// pflag.StringVar(&o.Exec.Exec_per_thread, "exec-per-thread", "", "Set the command that will receive by STDIN and write in the STDOUT into the output file")
	pflag.StringVar(&o.Exec.ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the extension for the STDOUT file when --exec-per-thread is used")
	// pmm
	pflag.StringVar(&o.Pmm.PmmPath, "pmm-path", "", "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution")
	pflag.StringVar(&o.Pmm.PmmResolution, "pmm-resolution", "", "which default will be high")
	// daemon
	pflag.BoolVarP(&o.Daemon.DaemonMode, "daemon", "D", false, "Enable daemon mode")
	pflag.StringVar(&o.Daemon.PidFile, "pid-file", fmt.Sprintf("/tmp/%s.pid", MYDUMPER), "Pid file used by Daemon mode.")
	// pflag.IntVarP(&o.Daemon.SnapshotInterval, "snapshot-interval", "I", 60, "Interval between each dump snapshot (in minutes), requires --daemon,default 60")
	// 	pflag.IntVarP(&o.Daemon.SnapshotCount, "snapshot-count", "X", 2, "number of snapshots, default 2")
	// chunks
	pflag.UintVar(&o.Chunks.MaxThreadsPerTable, "max-threads-per-table", 0, "Maximum number of threads per table to use")
	pflag.IntVar(&o.Chunks.MaxRows, "max-rows", 100000, "obsolete, Limit the number of rows per block after the table is estimated, It has been deprecated, use --rows instead. Removed in future releases")
	pflag.UintVar(&o.Chunks.CharDeep, "char-deep", 0, "Defines the amount of characters to use when the primary key is a string")
	pflag.UintVar(&o.Chunks.CharChunk, "char-chunk", 0, "Defines in how many pieces should split the table. By default we use the amount of threads")
	pflag.StringVarP(&o.Chunks.RowsPerChunk, "rows", "r", "", "Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds")
	pflag.BoolVar(&o.Chunks.SplitPartitions, "split-partitions", false, "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.")

	// Checksum
	pflag.BoolVarP(&o.Checksum.DumpChecksums, "checksum-all", "M", false, "Dump checksums for all elements")
	pflag.BoolVar(&o.Checksum.DataChecksums, "data-checksums", false, "Dump table checksums with the data")
	pflag.BoolVar(&o.Checksum.SchemaChecksums, "schema-checksums", false, "Dump schema table and view creation checksums")
	pflag.BoolVar(&o.Checksum.RoutineChecksums, "routine-checksums", false, "Dump triggers, functions and routines checksums")
	// filter
	pflag.StringVarP(&o.Filter.DB, "database", "B", "", "Comma delimited list of databases to dump")
	pflag.StringVarP(&o.Filter.IgnoreEngines, "ignore-engines", "i", "", "Comma delimited list of storage engines to ignore")
	pflag.StringVar(&o.Filter.WhereOption, "where", "", "Dump only selected records.")
	pflag.IntVarP(&o.Filter.UpdatedSince, "updated-since", "U", 0, "Use Update_time to dump only tables updated in the last U days")
	pflag.StringVar(&o.Filter.PartitionRegex, "partition-regex", "", "Regex to filter by partition name.")
	// Objects
	pflag.BoolVarP(&o.Objects.NoSchemas, "no-schemas", "m", false, "Do not dump table schemas with the data and triggers")
	pflag.BoolVarP(&o.Objects.DumpTablespaces, "all-tablespaces", "Y", false, "Dump all the tablespaces.")
	pflag.BoolVarP(&o.Objects.NoData, "no-data", "d", false, "Do not dump table data")
	pflag.BoolVarP(&o.Objects.DumpTriggers, "triggers", "G", false, "Dump triggers. By default, it do not dump triggers")
	pflag.BoolVarP(&o.Objects.DumpEvents, "events", "E", false, "Dump events. By default, it do not dump events")
	pflag.BoolVarP(&o.Objects.DumpRoutines, "routines", "R", false, "Dump stored procedures and functions. By default, it do not dump stored procedures nor functions")
	pflag.BoolVar(&o.Objects.ViewsAsTables, "views-as-tables", false, "Export VIEWs as they were tables")
	pflag.BoolVar(&o.Objects.SkipConstraints, "skip-constraints", false, "Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified")
	pflag.BoolVar(&o.Objects.SkipIndexes, "skip-indexes", false, "Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified")
	pflag.BoolVarP(&o.Objects.NoDumpViews, "no-views", "W", false, "Do not dump VIEWs")

	// statement
	pflag.BoolVar(&o.Statement.LoadData, "load-data", false, "Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files")
	pflag.BoolVar(&o.Statement.Csv, "csv", false, "Automatically enables --load-data and set variables to export in CSV format.")
	pflag.StringVar(&o.Statement.FieldsTerminatedByLd, "fields-terminated-by", "", "Defines the character that is written between fields")
	pflag.StringVar(&o.Statement.FieldsEnclosedByLd, "fields-enclosed-by", "", "Defines the character to enclose fields. Default: \"")
	// pflag.StringVar(&o.Statement.FieldsEscapedBy, "fields-escaped-by", "", "Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\\' ")
	pflag.StringVar(&o.Statement.LinesStartingByLd, "lines-starting-by", "", "Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&o.Statement.LinesTerminatedByLd, "lines-terminated-by", "", "Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&o.Statement.StatementTerminatedByLd, "statement-terminated-by", "", "This might never be used, unless you know what are you doing")
	pflag.BoolVar(&o.Statement.InsertIgnore, "insert-ignore", false, "Dump rows with INSERT IGNORE")
	pflag.BoolVar(&o.Statement.Replace, "replace", false, "Dump rows with REPLACE")
	pflag.BoolVar(&o.Statement.CompleteInsert, "complete-insert", false, "Use complete INSERT statements that include column names")
	pflag.BoolVar(&o.Statement.HexBlob, "hex-blob", false, "Dump binary columns using hexadecimal notation")
	pflag.BoolVar(&o.Statement.SkipDefiner, "skip-definer", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")
	pflag.IntVarP(&o.Statement.StatementSize, "statement-size", "s", 1000000, "Attempted size of INSERT statement in bytes")
	pflag.BoolVar(&o.Statement.SkipTz, "tz-utc", false, "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable. ")
	pflag.BoolVar(&o.Statement.SkipTz, "skip-tz-utc", false, "Doesn't add SET TIMEZONE on the backup files")
	pflag.StringVar(&o.Statement.SetNamesStr, "set-names", "", "Sets the names, use it at your own risk")
	pflag.StringVar(&o.Statement.OutputFormat, "format", INSERT, "Sets the names, use it at your own risk")
	pflag.BoolVar(&o.Statement.IncludeHeader, "include-header", false, "When --load-data or --csv is used, it will include the header with the column name")

	// connection
	pflag.StringVarP(&o.Connection.Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&o.Connection.Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&o.Connection.HidePassword, "password", "p", "", "User password")
	pflag.BoolVarP(&o.Connection.AskPassword, "ask-password", "a", false, "Prompt For User password")
	pflag.IntVarP(&o.Connection.Port, "port", "P", 0, "TCP/IP port to connect to")
	pflag.StringVarP(&o.Connection.SocketPath, "socket", "S", "", "UNIX domain socket file to use for connection")
	pflag.StringVar(&o.Connection.Protocol, "protocol", "", "The protocol to use for connection (tcp, socket)")
	// Common module
	pflag.UintVarP(&o.Common.NumThreads, "threads", "t", 4, "Number of threads to use, 0 means to use number of CPUs")
	pflag.BoolVarP(&o.Common.ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.StringVar(&o.Common.IdentifierQuoteCharacter, "identifier-quote-character", "", "This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK")
	pflag.IntVarP(&o.Common.Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info")
	pflag.BoolVar(&o.Common.Debug, "debug", false, "(automatically sets verbosity to 3),print more info")
	pflag.BoolVar(&o.Common.ClearDumpDir, "clear", false, "Clear output directory before dumping")
	pflag.BoolVar(&o.Common.DirtyDumpDir, "dirty", false, "Overwrite output directory without clearing (beware of leftower chunks)")
	pflag.StringVar(&o.Common.DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/mydumper.cnf")
	pflag.StringVar(&o.Common.DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
	pflag.StringVar(&o.Common.SourceControlCommand, "source-control-command", "", "Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS")

	// pflag.StringVar(&o.Common.Fifo_directory, "fifodir", "", "Directory where the FIFO files will be created when needed. Default: Same as backup")

	// CommonFilter module
	pflag.StringVarP(&o.CommonFilter.TablesSkiplistFile, "omit-from-file", "O", "", "File containing a list of database.table entries to skip, one per line (skips before applying regex option)")
	pflag.StringVarP(&o.CommonFilter.TablesList, "tables-list", "T", "", "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2")

	// CommonConnection module
	// pflag.BoolVarP(&o.CommonConnection.Compress_protocol, "compress-protocol", "C", false, "Use compression on the MySQL connection")
	pflag.BoolVar(&o.CommonConnection.Ssl, "ssl", false, "Connect using SSL")
	// pflag.StringVar(&o.CommonConnection.Ssl_mode, "ssl-mode", "", "Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY")
	pflag.StringVar(&o.CommonConnection.Key, "key", "", "The path name to the key file")
	pflag.StringVar(&o.CommonConnection.Cert, "cert", "", "The path name to the certificate file")
	pflag.StringVar(&o.CommonConnection.Ca, "ca", "", "The path name to the certificate authority file")
	// pflag.StringVar(&o.CommonConnection.Capath, "capath", "", "The path name to a directory that contains trusted SSL CA certificates in PEM format")
	// pflag.StringVar(&o.CommonConnection.Cipher, "cipher", "", "A list of permissible ciphers to use for SSL encryption")
	// pflag.StringVar(&o.CommonConnection.Tls_version, "tls-version", "", "Which protocols the server permits for encrypted connections")
	// regex_entries
	pflag.StringVarP(&o.Regex.Regex, "regex", "x", "", "Regular expression for 'db.table' matching")
	pflag.Parse()
	before_arguments_callback(o)
	identifier_quote_character_arguments_callback(o)
	arguments_callback(o)
	common_arguments_callback(o)
	format_arguments_callback(o)
	row_arguments_callback(o)
	stream_arguments_callback(o)
	connection_arguments_callback(o)
	after_arguments_callback(o)
	main_callback(o)
}

func pring_help() {
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [OPTION…] multi-threaded MySQL dumping\n", MYDUMPER)
	pflag.PrintDefaults()

	// os.Exit(EXIT_SUCCESS)
}

func main_callback(o *OptionEntries) {
	if o.Statement.LoadData {
		o.global.output_format = LOAD_DATA
		o.global.rows_file_extension = DAT
	}
	if o.Statement.Csv {
		o.global.output_format = CSV
		o.global.rows_file_extension = DAT
	}
	if o.CommonOptionEntries.Help {
		pring_help()
	}
	if o.Common.ProgramVersion {
		print_version(MYDUMPER)
		if !o.CommonOptionEntries.Help {
			os.Exit(EXIT_SUCCESS)
		}
	}
	if o.Common.Debug {
		set_debug(o)
		o.set_verbose()
	} else {
		o.set_verbose()
	}
	log.Infof("MyDumper backup version: %s", VERSION)
	initialize_common_options(o, MYDUMPER)
	hide_password(o)
	ask_password(o)
	if o.CommonOptionEntries.OutputDirectoryParam == "" {
		var datetimestr = time.Now().Format("20060102-150405")
		o.global.output_directory = fmt.Sprintf("%s-%s", DIRECTORY, datetimestr)
	} else {
		o.global.output_directory = o.CommonOptionEntries.OutputDirectoryParam
	}
	if o.CommonOptionEntries.Help {
		print_string("host", o.Connection.Hostname)
		print_string("user", o.Connection.Username)
		print_string("password", o.Connection.Password)
		print_bool("ask-password", o.Connection.AskPassword)
		print_int("port", o.Connection.Port)
		print_string("socket", o.Connection.SocketPath)
		print_string("protocol", o.Connection.Protocol)
		// print_bool("compress-protocol",o.Connection.compress_protocol)
		print_bool("ssl", o.CommonConnection.Ssl)
		// print_string("ssl-mode",ssl_mode)
		print_string("key", o.CommonConnection.Key)
		print_string("cert", o.CommonConnection.Cert)
		print_string("ca", o.CommonConnection.Ca)
		// print_string("capath",capath)
		// print_string("cipher",cipher)
		// print_string("tls-version",tls_version)
		print_list("regex", o.global.regex_list)
		print_string("database", o.Filter.DB)
		print_string("ignore-engines", o.Filter.IgnoreEngines)
		print_string("where", o.Filter.WhereOption)
		print_int("updated-since", o.Filter.UpdatedSince)
		print_string("partition-regex", o.Filter.PartitionRegex)
		print_string("omit-from-file", o.CommonFilter.TablesSkiplistFile)
		print_string("tables-list", o.CommonFilter.TablesList)
		print_string("tidb-snapshot", o.Lock.TidbSnapshot)
		print_bool("no-locks", o.Lock.NoLocks)
		print_bool("use-savepoints", o.Lock.UseSavepoints)
		print_bool("no-backup-locks", o.Lock.NoBackupLocks)
		print_bool("lock-all-tables", o.Lock.LockAllTables)
		print_bool("less-locking", o.Lock.LessLocking)
		print_bool("trx-consistency-only", o.Lock.TrxConsistencyOnly)
		print_bool("skip-ddl-locks", o.Lock.SkipDdlLocks)
		print_string("pmm-path", o.Pmm.PmmPath)
		print_string("pmm-resolution", o.Pmm.PmmResolution)
		print_uint("exec-threads", o.global.num_exec_threads)
		// print_string("exec", exec_command)
		// print_string("exec-per-thread", exec_per_thread)
		print_string("exec-per-thread-extension", o.Exec.ExecPerThreadExtension)
		print_int("long-query-retries", o.QueryRunning.LongqueryRetries)
		print_int("long-query-retry-interval", o.QueryRunning.LongqueryRetryInterval)
		print_int("long-query-guard", int(o.QueryRunning.Longquery))
		print_bool("kill-long-queries", o.QueryRunning.Killqueries)
		print_uint("max-threads-per-table", o.Chunks.MaxThreadsPerTable)
		//    print_string("char-deep",);
		//    print_string("char-chunk",);
		print_string("rows", fmt.Sprintf("%d:%d:%d", o.global.min_chunk_step_size, o.global.starting_chunk_step_size, o.global.max_chunk_step_size))
		print_bool("split-partitions", o.Chunks.SplitPartitions)
		print_bool("checksum-all", o.Checksum.DumpChecksums)
		print_bool("data-checksums", o.Checksum.DataChecksums)
		print_bool("schema-checksums", o.Checksum.SchemaChecksums)
		print_bool("routine-checksums", o.Checksum.RoutineChecksums)
		print_bool("no-schemas", o.Objects.NoSchemas)
		print_bool("all-tablespaces", o.Objects.DumpTablespaces)
		print_bool("no-data", o.Objects.NoData)
		print_bool("triggers", o.Objects.DumpTriggers)
		print_bool("events", o.Objects.DumpEvents)
		print_bool("routines", o.Objects.DumpRoutines)
		print_bool("views-as-tables", o.Objects.ViewsAsTables)
		print_bool("no-views", o.Objects.NoDumpViews)
		print_bool("load-data", o.Statement.LoadData)
		print_bool("csv", o.Statement.Csv)
		print_bool("clickhouse", o.global.clickhouse)
		print_bool("include-header", o.Statement.IncludeHeader)
		print_string("fields-terminated-by", o.Statement.FieldsTerminatedByLd)
		print_string("fields-enclosed-by", o.Statement.FieldsEnclosedByLd)
		print_string("fields-escaped-by", o.global.fields_escaped_by)
		print_string("lines-starting-by", o.Statement.LinesStartingByLd)
		print_string("lines-terminated-by", o.Statement.LinesTerminatedByLd)
		print_string("statement-terminated-by", o.Statement.StatementTerminatedByLd)
		print_bool("insert-ignore", o.Statement.InsertIgnore)
		print_bool("replace", o.Statement.Replace)
		print_bool("complete-insert", o.Statement.CompleteInsert)
		print_bool("hex-blob", o.Statement.HexBlob)
		print_bool("skip-definer", o.Statement.SkipDefiner)
		print_int("statement-size", o.Statement.StatementSize)
		print_bool("tz-utc", o.Statement.SkipTz)
		print_bool("skip-tz-utc", o.Statement.SkipTz)
		print_string("set-names", o.Statement.SetNamesStr)
		print_uint("chunk-filesize", o.Extra.ChunkFilesize)
		print_bool("exit-if-broken-table-found", o.Extra.ExitIfBrokenTableFound)
		print_bool("success-on-1146", o.Extra.SuccessOn1146)
		print_bool("build-empty-files", o.Extra.BuildEmptyFiles)
		print_bool("no-check-generated-fields", o.Extra.IgnoreGeneratedFields)
		print_bool("order-by-primary", o.Extra.OrderByPrimaryKey)
		print_bool("compact", o.Extra.Compact)
		print_bool("compress", o.Extra.CompressMethod != "")
		print_bool("use-defer", o.Extra.UseDefer)
		print_bool("check-row-count", o.Extra.CheckRowCount)
		print_bool("daemon", o.Daemon.DaemonMode)
		// print_int("snapshot-interval",snapshot_interval);
		// print_int("snapshot-count",snapshot_count);
		print_bool("help", o.CommonOptionEntries.Help)
		print_string("outputdir", o.global.output_directory)
		print_bool("clear", o.Common.ClearDumpDir)
		print_bool("dirty", o.Common.DirtyDumpDir)
		print_bool("stream", o.Stream.Stream)
		print_string("logfile", o.CommonOptionEntries.LogFile)
		print_string("disk-limits", o.CommonOptionEntries.DiskLimits)
		print_uint("threads", o.Common.NumThreads)
		print_bool("version", o.Common.ProgramVersion)
		print_int("verbose", o.Common.Verbose)
		print_bool("debug", o.Common.Debug)
		print_string("defaults-file", o.Common.DefaultsFile)
		print_string("defaults-extra-file", o.Common.DefaultsExtraFile)
		// print_string("fifodir",fifo_directory);
		os.Exit(EXIT_SUCCESS)
	}
	create_backup_dir(o.global.output_directory)
	if o.CommonOptionEntries.DiskLimits != "" {
		parse_disk_limits(o)
	}
	if o.Common.NumThreads < 2 {
		o.Extra.UseDefer = false
	}
	if o.Daemon.DaemonMode {
		o.Common.ClearDumpDir = true
		// initialize_daemon_thread()
		// run_daemon()
	} else {
		o.global.dump_directory = o.global.output_directory
	}
}
