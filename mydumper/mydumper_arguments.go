package mydumper

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	. "go-mydumper/src"
	"strings"
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
)

var (
	BufferSize           uint
	Help                 bool
	compress_method      string
	split_integer_tables bool
	output_format        int
	rows_file_extension  string = SQL
)

func entries() {
	pflag.BoolVarP(&Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&OutputDirectoryParam, "outputdir", "o", "", "Directory to output files to")
	pflag.BoolVar(&ClearDumpDir, "clear", false, "Clear output directory before dumping")
	pflag.BoolVar(&DirtyDumpDir, "dirty", false, "Overwrite output directory without clearing (beware of leftower chunks)")
	pflag.UintVarP(&BufferSize, "buffer-size", "b", 200000, "Queue buffer size")
	// pflag.BoolVarP(&Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVar(&DiskLimits, "disk-limits", "", "Set the limit to pause and resume if determines there is no enough disk space.\nAccepts values like: '<resume>:<pause>' in MB.\nFor instance: 100:500 will pause when there is only 100MB free and will\nresume if 500MB are available")
}
func extra_entries() {
	pflag.UintVarP(&ChunkFilesize, "chunk-filesize", "F", 0, "Split data files into pieces of this size in MB. Useful for myloader multi-threading.")
	pflag.BoolVar(&ExitIfBrokenTableFound, "exit-if-broken-table-found", false, "Exits if a broken table has been found")
	pflag.BoolVar(&SuccessOn1146, "success_on_1146", false, "Not increment error count and Warning instead of Critical in case of table doesn't exist")
	pflag.BoolVarP(&BuildEmptyFiles, "build_empty_files", "e", false, "Build dump files even if no data available from table")
	pflag.BoolVar(&IgnoreGeneratedFields, "no-check-generated-fields", false, "Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	pflag.BoolVar(&OrderByPrimaryKey, "order-by-primary", false, "Sort the data by Primary Key or Unique key if no primary key exists")
	pflag.StringVarP(&Compress, "compress", "c", "", "Compress output files")
	pflag.BoolVar(&Compact, "compact", false, "Give less verbose output. Disables header/footer constructs.")
	pflag.BoolVar(&UseDefer, "use-defer", false, "Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables).")
	pflag.BoolVar(&CheckRowCount, "check-row-count", false, "Run SELECT COUNT(*) and fail mydumper if dumped row count is different")
	pflag.IntVar(&SourceData, "source-data", 0, "It will include the options in the metadata file, to allow myloader to establish replication.")

}

func lock_entries() {
	pflag.StringVarP(&TidbSnapshot, "tidb-snapshot", "z", "", "Snapshot to use for TiDB")
	pflag.BoolVarP(&NoLocks, "no-locks", "k", false, "Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups")
	pflag.BoolVar(&UseSavepoints, "use-savepoints", false, "Use savepoints to reduce metadata locking issues, needs SUPER privilege")
	pflag.BoolVar(&NoBackupLocks, "no-backup-locks", false, "Do not use Percona backup locks")
	pflag.BoolVar(&LockAllTables, "lock-all-tables", false, "Use LOCK TABLE for all, instead of FTWRL")
	pflag.BoolVar(&LessLocking, "less-locking", false, "Minimize locking time on InnoDB tables.")
	pflag.BoolVar(&TrxConsistencyOnly, "trx-consistency-only", false, "Transactional consistency only")
	pflag.BoolVar(&SkipDdlLocks, "skip-ddl-locks", false, "Do not send DDL locks when possible")

}

func query_running_entries() {
	pflag.IntVar(&LongqueryRetries, "long-query-retries", 0, "Retry checking for long queries, default 0 (do not retry)")
	pflag.IntVar(&LongqueryRetryInterval, "long-query-retry-interval", 60, "Time to wait before retrying the long query check in seconds")
	pflag.Uint64VarP(&Longquery, "long-query-guard", "l", 60, "Set long query timer in seconds")
	pflag.BoolVarP(&Killqueries, "kill-long-queries", "K", false, "Kill long running queries (instead of aborting)")

}

func exec_entries() {
	pflag.UintVar(&Num_exec_threads, "exec-threads", 4, "Amount of threads to use with --exec")
	pflag.StringVar(&Exec_command, "exec", "", "Command to execute using the file as parameter")
	pflag.StringVar(&Exec_per_thread, "exec-per-thread", "", "Set the command that will receive by STDIN and write in the STDOUT into the output file")
	pflag.StringVar(&ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the extension for the STDOUT file when --exec-per-thread is used")
}

func pmm_entries() {
	// pmm
	pflag.StringVar(&PmmPath, "pmm-path", "", "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution")
	pflag.StringVar(&PmmResolution, "pmm-resolution", "", "which default will be high")

}
func daemon_entries() {
	pflag.BoolVarP(&DaemonMode, "daemon", "D", false, "Enable daemon mode")
	pflag.StringVar(&PidFile, "pid-file", fmt.Sprintf("/tmp/%s.pid", MYDUMPER), "Pid file used by Daemon mode.")
	pflag.IntVarP(&SnapshotInterval, "snapshot-interval", "I", 60, "Interval between each dump snapshot (in minutes), requires --daemon,default 60")
	pflag.IntVarP(&SnapshotCount, "snapshot-count", "X", 2, "number of snapshots, default 2")
}

func chunks_entries() {
	// chunks
	pflag.UintVar(&MaxThreadsPerTable, "max-threads-per-table", 0, "Maximum number of threads per table to use")
	pflag.UintVar(&CharDeep, "char-deep", 0, "Defines the amount of characters to use when the primary key is a string")
	pflag.UintVar(&CharChunk, "char-chunk", 0, "Defines in how many pieces should split the table. By default we use the amount of threads")
	pflag.StringVarP(&RowsPerChunk, "rows", "r", "", "Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds")
	pflag.BoolVar(&SplitPartitions, "split-partitions", false, "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.")

}

func checksum_entries() {
	// Checksum
	pflag.BoolVarP(&DumpChecksums, "checksum-all", "M", false, "Dump checksums for all elements")
	pflag.BoolVar(&DataChecksums, "data-checksums", false, "Dump table checksums with the data")
	pflag.BoolVar(&SchemaChecksums, "schema-checksums", false, "Dump schema table and view creation checksums")
	pflag.BoolVar(&RoutineChecksums, "routine-checksums", false, "Dump triggers, functions and routines checksums")

}
func filter_entries() {
	// filter
	pflag.StringVarP(&DB, "database", "B", "", "Comma delimited list of databases to dump")
	pflag.StringVarP(&IgnoreEngines, "ignore-engines", "i", "", "Comma delimited list of storage engines to ignore")
	pflag.StringVar(&WhereOption, "where", "", "Dump only selected records.")
	pflag.IntVarP(&UpdatedSince, "updated-since", "U", 0, "Use Update_time to dump only tables updated in the last U days")
	pflag.StringVar(&PartitionRegex, "partition-regex", "", "Regex to filter by partition name.")

}

func objects_entries() {
	// Objects
	pflag.BoolVarP(&NoSchemas, "no-schemas", "m", false, "Do not dump table schemas with the data and triggers")
	pflag.BoolVarP(&DumpTablespaces, "all-tablespaces", "Y", false, "Dump all the tablespaces.")
	pflag.BoolVarP(&NoData, "no-data", "d", false, "Do not dump table data")
	pflag.BoolVarP(&DumpTriggers, "triggers", "G", false, "Dump triggers. By default, it do not dump triggers")
	pflag.BoolVarP(&DumpEvents, "events", "E", false, "Dump events. By default, it do not dump events")
	pflag.BoolVarP(&DumpRoutines, "routines", "R", false, "Dump stored procedures and functions. By default, it do not dump stored procedures nor functions")
	pflag.BoolVar(&ViewsAsTables, "views-as-tables", false, "Export VIEWs as they were tables")
	pflag.BoolVar(&SkipConstraints, "skip-constraints", false, "Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified")
	pflag.BoolVar(&SkipIndexes, "skip-indexes", false, "Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified")
	pflag.BoolVarP(&NoDumpViews, "no-views", "W", false, "Do not dump VIEWs")

}
func statement_entries() {
	// statement
	pflag.BoolVar(&LoadData, "load-data", false, "Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files")
	pflag.BoolVar(&Csv, "csv", false, "Automatically enables --load-data and set variables to export in CSV format.")
	pflag.StringVar(&OutputFormat, "format", INSERT_ARG, "Sets the names, use it at your own risk")
	pflag.BoolVar(&IncludeHeader, "include-header", false, "When --load-data or --csv is used, it will include the header with the column name")

	pflag.StringVar(&FieldsTerminatedByLd, "fields-terminated-by", "", "Defines the character that is written between fields")
	pflag.StringVar(&FieldsEnclosedByLd, "fields-enclosed-by", "", "Defines the character to enclose fields. Default: \"")
	pflag.StringVar(&FieldsEscapedBy, "fields-escaped-by", "", "Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\\' ")
	pflag.StringVar(&LinesStartingByLd, "lines-starting-by", "", "Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&LinesTerminatedByLd, "lines-terminated-by", "", "Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.")
	pflag.StringVar(&StatementTerminatedByLd, "statement-terminated-by", "", "This might never be used, unless you know what are you doing")
	pflag.BoolVar(&InsertIgnore, "insert-ignore", false, "Dump rows with INSERT IGNORE")
	pflag.BoolVar(&Replace, "replace", false, "Dump rows with REPLACE")
	pflag.BoolVar(&CompleteInsert, "complete-insert", false, "Use complete INSERT statements that include column names")
	pflag.BoolVar(&HexBlob, "hex-blob", false, "Dump binary columns using hexadecimal notation")
	pflag.BoolVar(&SkipDefiner, "skip-definer", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")
	pflag.IntVarP(&StatementSize, "statement-size", "s", 1000000, "Attempted size of INSERT statement in bytes")
	pflag.BoolVar(&SkipTz, "tz-utc", false, "SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable. ")
	pflag.BoolVar(&SkipTz, "skip-tz-utc", false, "Doesn't add SET TIMEZONE on the backup files")
	pflag.StringVar(&SetNamesStr, "set-names", "", "Sets the names, use it at your own risk")

}

func load_contex_entries() {
	entries()
	Common_entries()
	filter_entries()
	Common_filter_entries()
	lock_entries()
	pmm_entries()
	exec_entries()
	query_running_entries()
	chunks_entries()
	checksum_entries()
	objects_entries()
	statement_entries()
	extra_entries()
	daemon_entries()
	pflag.Parse()
	Stream_arguments_callback()
	arguments_callback()
}

func arguments_callback() bool {
	if Compress != "" {
		if strings.EqualFold(Compress, GZIP) {
			compress_method = GZIP
		} else if strings.EqualFold(Compress, ZSTD) {
			compress_method = ZSTD
		} else {
			log.Fatalf("Unknown compression method %s", Compress)
		}

	}
	if RowsPerChunk != "" {
		split_integer_tables = parse_rows_per_chunk(RowsPerChunk, &min_chunk_step_size, &starting_chunk_step_size, &max_chunk_step_size)
	}
	if OutputFormat != "" {
		if strings.EqualFold(OutputFormat, INSERT_ARG) {
			output_format = SQL_INSERT
		} else if strings.EqualFold(OutputFormat, LOAD_DATA_ARG) {
			LoadData = true
			rows_file_extension = DAT
			output_format = LOAD_DATA
		} else if strings.EqualFold(OutputFormat, CSV_ARG) {
			Csv = true
			rows_file_extension = DAT
			output_format = CSV
		} else if strings.EqualFold(OutputFormat, CLICKHOUSE_ARG) {
			clickhouse = true
			rows_file_extension = DAT
			output_format = CLICKHOUSE
		} else {
			log.Fatalf("Unknown output format %s", OutputFormat)
		}
	}
	return Common_arguments_callback()
}

func connection_arguments_callback() {
	if HidePassword != "" {
		var tempPasswd []byte = make([]byte, len(HidePassword))
		copy(tempPasswd, []byte(HidePassword))
		Password = string(tempPasswd)
	}
	if Port != 0 || Hostname != "" {
		Protocol = "tcp"
	}
	if SocketPath != "" || (Port == 0 && Hostname == "" && SocketPath == "") {
		Protocol = "socket"
	}

	if Protocol != "" {
		if strings.ToLower(Protocol) == "tcp" {
			Protocol = strings.ToLower(Protocol)

		}
		if strings.ToLower(Protocol) == "socket" {
			Protocol = strings.ToLower(Protocol)
		}
	}

}
