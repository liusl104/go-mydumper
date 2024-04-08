package mydumper

import (
	"github.com/go-ini/ini"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/spf13/pflag"
	"os"
	"regexp"
	"sync"
)

const (
	GZIP = "GZIP"
	ZSTD = "ZSTD"
)

func arguments_callback(o *OptionEntries) bool {
	if o.Extra.Compress {
		if o.Extra.CompressMethod == "" {
			o.Extra.CompressMethod = GZIP
			return true
		}
		if o.Extra.CompressMethod == ZSTD {
			o.Extra.CompressMethod = ZSTD
			return true
		}
	}
	o.global.sync_wait = -1
	o.global.insert_statement = INSERT

	return false
}
func NewDefaultEntries() *OptionEntries {
	o := newEntries()
	o.CommonOptionEntries.BufferSize = 200000
	o.Extra.CompressMethod = GZIP
	o.QueryRunning.LongqueryRetryInterval = 60
	o.QueryRunning.Longquery = 60
	o.Daemon.SnapshotCount = 2
	o.Daemon.SnapshotInterval = 60
	o.Chunks.MaxRows = 100000
	o.Statement.StatementSize = 1000000
	o.Connection.Port = 3306
	o.Common.NumThreads = 4
	o.Common.Verbose = 2
	o.global.insert_statement = INSERT
	arguments_callback(o)
	identifier_quote_character_arguments_callback(o)
	stream_arguments_callback(o)
	connection_arguments_callback(o)
	return o
}

type OptionEntries struct {
	global              *globalEntries           // run internal variables
	CommonOptionEntries *CommonOptionEntries     `json:"commonOptionEntries" ini:"mydumper"`
	Extra               *ExtraEntries            `json:"extra,omitempty" ini:"mydumper"`             // Extra module
	Lock                *LockEntries             `json:"lock,omitempty" ini:"mydumper"`              // Lock module
	QueryRunning        *QueryRunningEntries     `json:"query_running,omitempty" ini:"mydumper"`     // QueryRunning module
	Exec                *ExecEntries             `json:"exec,omitempty" ini:"mydumper"`              // Exec module
	Pmm                 *PmmEntries              `json:"pmm,omitempty" ini:"mydumper"`               // Pmm module
	Daemon              *DaemonEntries           `json:"daemon,omitempty" ini:"mydumper"`            // Daemon module
	Chunks              *ChunksEntries           `json:"chunks,omitempty" ini:"mydumper"`            // Chunks module
	Checksum            *ChecksumEntries         `json:"checksum,omitempty" ini:"mydumper"`          // Checksum module
	Filter              *FilterEntries           `json:"filter,omitempty" ini:"mydumper"`            // Filter module
	Objects             *ObjectsEntries          `json:"objects,omitempty" ini:"mydumper"`           // Objects module
	Statement           *StatementEntries        `json:"statement,omitempty" ini:"mydumper"`         // Statement module
	Common              *CommonEntries           `json:"common,omitempty" ini:"mydumper"`            // Common module
	CommonFilter        *CommonFilterEntries     `json:"common_filter,omitempty" ini:"mydumper"`     // CommonFilter module
	CommonConnection    *CommonConnectionEntries `json:"common_connection,omitempty" ini:"mydumper"` // CommonConnection module
	Connection          *ConnectionEntries       `json:"connection,omitempty" ini:"mydumper"`        // Connection module
	Regex               *RegexEntries            `json:"regex,omitempty" ini:"mydumper"`             // Regex module
	Stream              *StreamEntries           `json:"stream,omitempty" ini:"mydumper"`            // Stream module
}
type CommonOptionEntries struct {
	BufferSize             int    `json:"buffer_size,omitempty" ini:"buffer_size"`       // queue buffer size
	Output_directory_param string `json:"output_Directory_Param" ini:"output_directory"` // Directory to output files to
	Help                   bool   `json:"help,omitempty" ini:"help"`                     // Show help options
	LogFile                string `json:"log_file,omitempty" ini:"log_file"`             // Log file name to use, by default stdout is used
	DiskLimits             string `json:"disk_limits,omitempty" ini:"disk_limits"`       // Set the limit to pause and resume if determines there is no enough disk space. Accepts values like: '<resume>:<pause>' in MB. For instance: 100:500 will pause when there is only 100MB free and will resume if 500MB are available
}
type globalEntries struct {
	major                                       int
	secondary                                   int
	revision                                    int
	product                                     server_type
	re                                          *regexp.Regexp
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
	stream_queue                                *asyncQueue
	ref_table_mutex                             *sync.Mutex
	ref_table                                   map[string]string
	table_number                                int
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
	non_innodb_table                            []*db_table
	innodb_table                                []*db_table
	conf_per_table                              *configuration_per_table
	all_dbts                                    []*db_table
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
	fields_enclosed_by                          string
	fields_terminated_by                        string
	lines_starting_by                           string
	lines_terminated_by                         string
	statement_terminated_by                     string
	insert_statement                            string
}

type StreamEntries struct {
	Stream    bool   `json:"stream,omitempty" ini:"stream"` // It will stream over STDOUT once the files has been written
	StreamOpt string `json:"stream_opt,omitempty" ini:"stream_opt"`
}

type RegexEntries struct {
	Regex string `json:"regex,omitempty" ini:"regex"` // Regular expression for 'db.table' matching
}

type ExtraEntries struct {
	ChunkFilesize          int    `json:"chunk_filesize,omitempty" ini:"chunk_filesize"`                         // Split tables into chunks of this output file size. This value is in MB
	ExitIfBrokenTableFound bool   `json:"exit_if_broken_table_found,omitempty" ini:"exit_if_broken_table_found"` // Exits if a broken table has been found
	SuccessOn1146          bool   `json:"success_on_1146,omitempty" ini:"success_on_1146"`                       // Not increment error count and Warning instead of Critical in case of table doesn't exist
	BuildEmptyFiles        bool   `json:"build_empty_files,omitempty" ini:"build_empty_files"`                   // Build dump files even if no data available from table
	IgnoreGeneratedFields  bool   `json:"ignore_generated_fields,omitempty" ini:"ignore_generated_fields"`       // Queries related to generated fields are not going to be executed.It will lead to restoration issues if you have generated columns
	OrderByPrimaryKey      bool   `json:"order_by_primary_key,omitempty" ini:"order_by_primary_key"`             // Sort the data by Primary Key or Unique key if no primary key exists
	Compress               bool   `json:"compress,omitempty" ini:"compress"`                                     // Compress output files
	CompressMethod         string `json:"compress_method,omitempty" ini:"compress_method"`                       // CompressMethod Options: GZIP and ZSTD. Default: GZIP
}

type LockEntries struct {
	TidbSnapshot       string `json:"tidb_snapshot,omitempty" ini:"tidb_snapshot"`               // Snapshot to use for TiDB
	NoLocks            bool   `json:"no_locks,omitempty" ini:"no_locks"`                         // Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups
	UseSavepoints      bool   `json:"use_savepoints,omitempty" ini:"use_savepoints"`             // Use savepoints to reduce metadata locking issues, needs SUPER privilege
	NoBackupLocks      bool   `json:"no_backup_locks,omitempty" ini:"no_backup_locks"`           // Do not use Percona backup locks
	LockAllTables      bool   `json:"lock_all_tables,omitempty" ini:"lock_all_tables"`           // Use LOCK TABLE for all, instead of FTWRL
	LessLocking        bool   `json:"less_locking,omitempty" ini:"less_locking"`                 // Minimize locking time on InnoDB tables.
	TrxConsistencyOnly bool   `json:"trx_consistency_only,omitempty" ini:"trx_consistency_only"` // Transactional consistency only
}

type QueryRunningEntries struct {
	LongqueryRetries       int    `json:"longquery_retries,omitempty" ini:"longquery_retries"`               // Retry checking for long queries, default 0 (do not retry)
	LongqueryRetryInterval int    `json:"longquery_retry_interval,omitempty" ini:"longquery_retry_interval"` // Time to wait before retrying the long query check in seconds, default 60
	Longquery              uint64 `json:"longquery,omitempty" ini:"longquery"`                               // Set long query timer in seconds, default 60
	Killqueries            bool   `json:"killqueries,omitempty" ini:"killqueries"`                           // Kill long running queries (instead of aborting)
}

type ExecEntries struct {
	// Num_exec_threads int    // Amount of threads to use with --exec
	// Exec_command     string // Command to execute using the file as parameter
	// Exec_per_thread           string // Set the command that will receive by STDIN and write in the STDOUT into the output file
	ExecPerThreadExtension string `json:"exec_per_thread_extension,omitempty" ini:"exec_per_thread_extension" ` // Set the extension for the STDOUT file when --exec-per-thread is used
}

type PmmEntries struct {
	PmmPath       string `json:"pmm_path,omitempty"  ini:"pmm_path"`            // which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
	PmmResolution string `json:"pmm_Resolution,omitempty" ini:"pmm_resolution"` // which default will be high
}

type DaemonEntries struct {
	DaemonMode       bool `json:"daemon_mode,omitempty" ini:"daemon_mode"`             // Enable daemon mode
	SnapshotInterval int  `json:"snapshot_interval,omitempty" ini:"snapshot_interval"` // Interval between each dump snapshot (in minutes), requires --daemon,default 60
	SnapshotCount    int  `json:"snapshot_count,omitempty" ini:"snapshot_count"`       // number of snapshots, default 2
}

type ChunksEntries struct {
	MaxRows         int    `json:"max_rows,omitempty" ini:"max_rows"`                 // Limit the number of rows per block after the table is estimated, default 1000000. It has been deprecated, use --rows instead. Removed in future releases
	CharDeep        uint   `json:"char_deep,omitempty" ini:"char_deep"`               // Defines the amount of characters to use when the primary key is a string
	CharChunk       uint   `json:"char_chunk,omitempty" ini:"char_chunk"`             // Defines in how many pieces should split the table. By default we use the amount of threads
	RowsPerChunk    string `json:"rows_per_chunk,omitempty" ini:"rows_per_chunk"`     // Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds
	SplitPartitions bool   `json:"split_partitions,omitempty" ini:"split_partitions"` // Dump partitions into separate files. This options overrides the --rows option for partitioned tables.
}

type ChecksumEntries struct {
	DumpChecksums    bool `json:"dump_checksums,omitempty" ini:"dump_checksums"`       // Dump checksums for all elements
	DataChecksums    bool `json:"data_checksums,omitempty" ini:"data_checksums"`       // Dump table checksums with the data
	SchemaChecksums  bool `json:"schema_checksums,omitempty" ini:"schema_checksums"`   // Dump schema table and view creation checksums
	RoutineChecksums bool `json:"routine_checksums,omitempty" ini:"routine_checksums"` // Dump triggers, functions and routines checksums
}

type FilterEntries struct {
	DB             string `json:"database,omitempty" ini:"database"`               // Log file name to use, by default stdout is used
	IgnoreEngines  string `json:"ignore_engines,omitempty" ini:"ignore_engines"`   // Comma delimited list of storage engines to ignore
	WhereOption    string `json:"where_option,omitempty" ini:"where_option"`       // Dump only selected records.
	UpdatedSince   int    `json:"updated_since,omitempty" ini:"updated_since"`     // Use Update_time to dump only tables updated in the last U days
	PartitionRegex string `json:"partition_regex,omitempty" ini:"partition_regex"` // Regex to filter by partition name.
}

type ObjectsEntries struct {
	NoSchemas       bool `json:"no_schemas,omitempty" ini:"no_schemas"`             // Do not dump table schemas with the data and triggers
	DumpTablespaces bool `json:"dump_tablespaces,omitempty" ini:"dump_tablespaces"` // Dump all the tablespaces.
	NoData          bool `json:"no_data,omitempty" ini:"no_data"`                   // Do not dump table data
	DumpTriggers    bool `json:"dump_triggers,omitempty" ini:"dump_triggers"`       // Dump triggers. By default, it do not dump triggers
	DumpEvents      bool `json:"dump_events,omitempty" ini:"dump_events"`           // Dump events. By default, it do not dump events
	DumpRoutines    bool `json:"dump_routines,omitempty" ini:"dump_routines"`       // Dump stored procedures and functions. By default, it do not dump stored procedures nor functions
	ViewsAsTables   bool `json:"views_as_tables,omitempty" ini:"views_as_tables"`   // Export VIEWs as they were tables
	NoDumpViews     bool `json:"no_dump_views,omitempty" ini:"no_dump_views"`       // Do not dump VIEWs
}

type StatementEntries struct {
	LoadData                bool   `json:"load_data,omitempty" ini:"load_data"`                                   // Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files
	Csv                     bool   `json:"csv,omitempty" ini:"csv"`                                               // Automatically enables --load-data and set variables to export in CSV format.
	FieldsTerminatedByLd    string `json:"fields_terminated_by_ld,omitempty" ini:"fields_terminated_by_ld"`       // Defines the character that is written between fields
	FieldsEnclosedByLd      string `json:"fields_enclosed_by_ld,omitempty" ini:"fields_enclosed_by_ld"`           // Defines the character to enclose fields. Default: \"
	FieldsEscapedBy         string `json:"fields_escaped_by,omitempty" ini:"fields_escaped_by"`                   // Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\\'
	LinesStartingByLd       string `json:"lines_starting_by_ld,omitempty" json:"lines_starting_by_ld"`            // Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
	LinesTerminatedByLd     string `json:"lines_terminated_by_ld,omitempty" ini:"lines_terminated_by_ld"`         // Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
	StatementTerminatedByLd string `json:"statement_terminated_by_ld,omitempty" ini:"statement_terminated_by_ld"` // This might never be used, unless you know what are you doing
	InsertIgnore            bool   `json:"insert_ignore,omitempty" ini:"insert_ignore"`                           // Dump rows with INSERT IGNORE
	Replace                 bool   `json:"replace,omitempty" ini:"replace"`                                       // Dump rows with REPLACE
	CompleteInsert          bool   `json:"complete_insert,omitempty" ini:"complete_insert"`                       // Use complete INSERT statements that include column names
	HexBlob                 bool   `json:"hex_blob,omitempty" ini:"hex_blob"`                                     // Dump binary columns using hexadecimal notation
	SkipDefiner             bool   `json:"skip_definer,omitempty" ini:"skip_definer"`                             // Removes DEFINER from the CREATE statement. By default, statements are not modified
	StatementSize           int    `json:"statement_size,omitempty" ini:"statement_size"`                         // Attempted size of INSERT statement in bytes, default 1000000
	SkipTz                  bool   `json:"skip_tz,omitempty" ini:"skip_tz"`                                       // SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.
	SetNamesStr             string `json:"set_names_str,omitempty" ini:"set_names_str"`                           // Sets the names, use it at your own risk, default binary
}

type CommonEntries struct {
	NumThreads               uint   `json:"num_threads,omitempty" ini:"num_threads"`                               // "Number of threads to use, default 4
	ProgramVersion           bool   `json:"program_version,omitempty" ini:"program_version"`                       // Show the program version and exit
	IdentifierQuoteCharacter string `json:"identifier_quote_character,omitempty" ini:"identifier_quote_character"` // This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK
	Verbose                  int    `json:"verbose,omitempty" ini:"verbose"`                                       // Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2
	Debug                    bool   `json:"debug,omitempty" ini:"debug"`                                           // (automatically sets verbosity to 3),print more info
	DefaultsFile             string `json:"defaults_file,omitempty" ini:"defaults_file"`                           // Use a specific defaults file. Default: /etc/mydumper.cnf
	DefaultsExtraFile        string `json:"defaults_extra_file,omitempty" ini:"defaults_extra_file"`               // Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
	// Fifo_directory             string // Directory where the FIFO files will be created when needed. Default: Same as backup
}

type CommonFilterEntries struct {
	TablesSkiplistFile string `json:"tables_skiplist_file,omitempty" ini:"tables_skiplist_file"` // File containing a list of database.table entries to skip, one per line (skips before applying regex option)
	TablesList         string `json:"tables_list,omitempty" ini:"tables_list"`                   // Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
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

func CommandEntries(o *OptionEntries) {
	// option

	pflag.BoolVarP(&o.CommonOptionEntries.Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&o.CommonOptionEntries.Output_directory_param, "outputdir", "o", "", "Directory to output files to")
	pflag.IntVarP(&o.CommonOptionEntries.BufferSize, "buffer-size", "b", 200000, "Queue buffer size")

	pflag.StringVarP(&o.CommonOptionEntries.LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVar(&o.CommonOptionEntries.DiskLimits, "disk-limits", "", "Set the limit to pause and resume if determines there is no enough disk space.\nAccepts values like: '<resume>:<pause>' in MB.\nFor instance: 100:500 will pause when there is only 100MB free and will\nresume if 500MB are available")
	// Stream
	pflag.BoolVar(&o.Stream.Stream, "stream", false, "It will stream over STDOUT once the files has been written")
	pflag.StringVar(&o.Stream.StreamOpt, "stream-opt", "", "It will stream over STDOUT once the files has been written")
	// extra
	pflag.IntVarP(&o.Extra.ChunkFilesize, "chunk-filesize", "F", 0, "Split tables into chunks of this output file size. This value is in MB")
	pflag.BoolVar(&o.Extra.ExitIfBrokenTableFound, "exit-if-broken-table-found", false, "Exits if a broken table has been found")
	pflag.BoolVar(&o.Extra.SuccessOn1146, "success_on_1146", false, "Not increment error count and Warning instead of Critical in case of table doesn't exist")
	pflag.BoolVarP(&o.Extra.BuildEmptyFiles, "build_empty_files", "e", false, "Build dump files even if no data available from table")
	pflag.BoolVar(&o.Extra.IgnoreGeneratedFields, "no-check-generated-fields", false, "Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	pflag.BoolVar(&o.Extra.OrderByPrimaryKey, "order-by-primary", false, "Sort the data by Primary Key or Unique key if no primary key exists")
	pflag.BoolVarP(&o.Extra.Compress, "compress", "c", false, "Compress output files")
	pflag.StringVar(&o.Extra.CompressMethod, "compress-method", "GZIP", "using: Options: GZIP and ZSTD. Default: GZIP")
	// lock
	pflag.StringVarP(&o.Lock.TidbSnapshot, "tidb-snapshot", "z", "", "Snapshot to use for TiDB")
	pflag.BoolVarP(&o.Lock.NoLocks, "no-locks", "k", false, "Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups")
	pflag.BoolVar(&o.Lock.UseSavepoints, "use-savepoints", false, "Use savepoints to reduce metadata locking issues, needs SUPER privilege")
	pflag.BoolVar(&o.Lock.NoBackupLocks, "no-backup-locks", false, "Do not use Percona backup locks")
	pflag.BoolVar(&o.Lock.LockAllTables, "lock-all-tables", false, "Use LOCK TABLE for all, instead of FTWRL")
	pflag.BoolVar(&o.Lock.LessLocking, "less-locking", false, "Minimize locking time on InnoDB tables.")
	pflag.BoolVar(&o.Lock.TrxConsistencyOnly, "trx-consistency-only", false, "Transactional consistency only")

	// query running

	pflag.IntVar(&o.QueryRunning.LongqueryRetries, "long-query-retries", 0, "Retry checking for long queries, default 0 (do not retry)")
	pflag.IntVar(&o.QueryRunning.LongqueryRetryInterval, "long-query-retry-interval", 60, "Time to wait before retrying the long query check in seconds, default 60")
	pflag.Uint64VarP(&o.QueryRunning.Longquery, "long-query-guard", "l", 60, "Set long query timer in seconds, default 60")
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
	pflag.IntVarP(&o.Daemon.SnapshotInterval, "snapshot-interval", "I", 60, "Interval between each dump snapshot (in minutes), requires --daemon,default 60")
	pflag.IntVarP(&o.Daemon.SnapshotCount, "snapshot-count", "X", 2, "number of snapshots, default 2")
	// chunks
	pflag.IntVar(&o.Chunks.MaxRows, "max-rows", 100000, "Limit the number of rows per block after the table is estimated, default 1000000. It has been deprecated, use --rows instead. Removed in future releases")
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
	pflag.StringVarP(&o.Filter.DB, "database", "B", "", "Log file name to use, by default stdout is used")
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
	pflag.BoolVarP(&o.Objects.NoDumpViews, "no-views", "W", false, "Do not dump VIEWs")
	// statement
	pflag.BoolVar(&o.Statement.LoadData, "load-data", false, "Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files")
	pflag.BoolVar(&o.Statement.Csv, "csv", false, "Automatically enables --load-data and set variables to export in CSV format.")
	pflag.StringVar(&o.Statement.FieldsTerminatedByLd, "fields-terminated-by", "", "Defines the character that is written between fields")
	pflag.StringVar(&o.Statement.FieldsEnclosedByLd, "fields-enclosed-by", "", "Defines the character to enclose fields. Default: \"")
	pflag.StringVar(&o.Statement.FieldsEscapedBy, "fields-escaped-by", "", "Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\\' ")
	pflag.StringVar(&o.Statement.LinesStartingByLd, "lines-starting-by", "", "Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&o.Statement.LinesTerminatedByLd, "lines-terminated-by", "", "Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&o.Statement.StatementTerminatedByLd, "statement-terminated-by", "", "This might never be used, unless you know what are you doing")
	pflag.BoolVar(&o.Statement.InsertIgnore, "insert-ignore", false, "Dump rows with INSERT IGNORE")
	pflag.BoolVar(&o.Statement.Replace, "replace", false, "Dump rows with REPLACE")
	pflag.BoolVar(&o.Statement.CompleteInsert, "complete-insert", false, "Use complete INSERT statements that include column names")
	pflag.BoolVar(&o.Statement.HexBlob, "hex-blob", false, "Dump binary columns using hexadecimal notation")
	pflag.BoolVar(&o.Statement.SkipDefiner, "skip-definer", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")
	pflag.IntVarP(&o.Statement.StatementSize, "statement-size", "s", 1000000, "Attempted size of INSERT statement in bytes, default 1000000")
	pflag.BoolVar(&o.Statement.SkipTz, "tz-utc", false, "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable. ")
	pflag.BoolVar(&o.Statement.SkipTz, "skip-tz-utc", false, "Doesn't add SET TIMEZONE on the backup files")
	pflag.StringVar(&o.Statement.SetNamesStr, "set-names", "binary", "Sets the names, use it at your own risk, default binary")
	// connection
	pflag.StringVarP(&o.Connection.Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&o.Connection.Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&o.Connection.Password, "password", "p", "", "User password")
	pflag.BoolVarP(&o.Connection.AskPassword, "ask-password", "a", false, "Prompt For User password")
	pflag.IntVarP(&o.Connection.Port, "port", "P", 3306, "TCP/IP port to connect to")
	pflag.StringVarP(&o.Connection.Socket, "socket", "S", "", "UNIX domain socket file to use for connection")
	pflag.StringVar(&o.Connection.Protocol, "protocol", "tcp", "The protocol to use for connection (tcp, socket)")
	// Common module
	pflag.UintVarP(&o.Common.NumThreads, "threads", "t", 4, "Number of threads to use, default 4")
	pflag.BoolVarP(&o.Common.ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.StringVar(&o.Common.IdentifierQuoteCharacter, "identifier-quote-character", "", "This set the identifier quote character that is used to INSERT statements only on mydumper and to split statement on myloader. Use SQL_MODE to change the CREATE TABLE statements Posible values are: BACKTICK and DOUBLE_QUOTE. Default: BACKTICK")
	pflag.IntVarP(&o.Common.Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2")
	pflag.BoolVar(&o.Common.Debug, "debug", false, "(automatically sets verbosity to 3),print more info")
	pflag.StringVar(&o.Common.DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/mydumper.cnf")
	pflag.StringVar(&o.Common.DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
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
	identifier_quote_character_arguments_callback(o)
	arguments_callback(o)
	stream_arguments_callback(o)
	connection_arguments_callback(o)
}

func identifier_quote_character_arguments_callback(o *OptionEntries) bool {
	if o.Common.IdentifierQuoteCharacter != "" {
		if o.Common.IdentifierQuoteCharacter == BACKTICK {
			return true
		}
		if o.Common.IdentifierQuoteCharacter == DOUBLE_QUOTE {
			return true
		}
	} else {
		o.Common.IdentifierQuoteCharacter = BACKTICK
		return true
	}
	return false
}
