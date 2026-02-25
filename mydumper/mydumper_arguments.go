package mydumper

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"github.com/spf13/pflag"
	"strings"
)

const (
	INSERT_ARG     = "INSERT"
	LOAD_DATA_ARG  = "LOAD_DATA"
	CSV_ARG        = "CSV"
	MEMORY         = "MEMORY"
	CLICKHOUSE_ARG = "CLICKHOUSE"
	SQL_INSERT     = 0
	LOAD_DATA      = 1
	CSV            = 2
	CLICKHOUSE     = 3
	SQL            = "sql"
	DAT            = "dat"
)

const (
	AUTO         string = "AUTO"
	FTWRL        string = "FTWRL"
	LOCK_ALL     string = "LOCK_ALL"
	GTID         string = "GTID"
	NO_LOCK      string = "NO_LOCK"
	SAFE_NO_LOCK string = "SAFE_NO_LOCK"
)

var (
	SyncThreadLockMode           string = "AUTO"
	compress_method              string
	split_integer_tables         bool   = true
	output_format                int    = SQL_INSERT
	rows_file_extension          string = SQL
	max_time_per_select          int
	OutputDirectoryStr           string
	masquerade_filename          bool
	TrxTables                    int = 1
	UseSingleColumn              bool
	RowsHard                     string
	RowsPerChunk                 string
	TableEngineForViewDependency string = MEMORY
	ftwrl_max_wait_time          int    = 60
	ftwrl_timeout_retries        int    = 0
	DefaultCharacterSet          string
)

// entries registers main mydumper flags: help, outputdir, clear, logfile, disk-limits, etc.
func entries() {
	pflag.BoolVarP(&Help, "help", "?", false, "Show help options")
	pflag.StringVarP(&OutputDirectoryStr, "outputdir", "o", "", "Directory to output files to")
	pflag.BoolVar(&ClearDumpDir, "clear", false, "Clear output directory before dumping")
	pflag.BoolVar(&DirtyDumpDir, "dirty", false, "Overwrite output directory without clearing (beware of leftower chunks)")
	pflag.BoolVar(&MergeDumpDir, "merge", false, "Merge the metadata with previous backup and overwrite output directory without clearing (beware of leftower chunks)")
	pflag.UintVarP(&BufferSize, "buffer-size", "b", 1000, "Queue buffer size")
	pflag.StringVarP(&LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVar(&DiskLimits, "disk-limits", "", "Set the limit to pause and resume if determines there is no enough disk space.\nAccepts values like: '<resume>:<pause>' in MB.\nFor instance: 100:500 will pause when there is only 100MB free and will\nresume if 500MB are available")
	pflag.BoolVar(&masquerade_filename, "masquerade-filename", false, "Masquerades the filenames")
	pflag.IntVar(&ftwrl_max_wait_time, "ftwrl-max-wait-time", 60, "Sets the max time that we are going to wait before kill the FLUSH TABLES related commands. Default: 60")
	pflag.IntVar(&ftwrl_timeout_retries, "ftwrl-timeout-retries", 0, "Sets the amount of retries before give up acquiring FLUSH TABLES. Default: 0, never gives up.")
	pflag.StringVar(&ReplicaDataStr, "replica-data", "", "Includes the replica information")
}

// extra_entries registers chunk-filesize, compress, compact, use-defer, and related options.
func extra_entries() {
	pflag.UintVarP(&ChunkFilesize, "chunk-filesize", "F", 0, "Split data files into pieces of this size in MB. Useful for myloader multi-threading.")
	pflag.BoolVar(&ExitIfBrokenTableFound, "exit-if-broken-table-found", false, "Exits if a broken table has been found")
	pflag.BoolVar(&SuccessOn1146, "success_on_1146", false, "This option is deprecated use --ignore_engines-Errors instead")
	pflag.BoolVarP(&BuildEmptyFiles, "build_empty_files", "e", false, "Build dump files even if no data available from table")
	pflag.BoolVar(&IgnoreGeneratedFields, "no-check-generated-fields", false, "Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns")
	pflag.BoolVar(&OrderByPrimaryKey, "order-by-primary", false, "Sort the data by Primary Key or Unique key if no primary key exists")
	pflag.BoolVarP(&Compress, "compress", "c", false, "Compress output files")
	pflag.BoolVar(&Compact, "compact", false, "Give less verbose output. Disables header/footer constructs.")
	pflag.BoolVar(&UseDefer, "use-defer", false, "Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables).")
	pflag.BoolVar(&CheckRowCount, "check-row-count", false, "Run SELECT COUNT(*) and fail mydumper if dumped row count is different")

}

// lock_entries registers lock-related flags: tidb-snapshot, no-locks, sync-thread-lock-mode, trx-tables, etc.
func lock_entries() {
	pflag.StringVarP(&TidbSnapshot, "tidb-snapshot", "z", "", "Snapshot to use for TiDB")
	pflag.BoolVarP(&NoLocks, "no-locks", "k", false, "This option is deprecated use --sync-thread-lock-mode instead")
	pflag.BoolVar(&LockAllTables, "lock-all-tables", false, "This option is deprecated use --sync-thread-lock-mode instead")
	pflag.StringVar(&SyncThreadLockMode, "sync-thread-lock-mode", "AUTO", "There are 4 modes that can be use to sync: SAFE_NO_LOCK, FTWRL, LOCK_ALL and GTID. \nIf you don't need a consistent backup, use: NO_LOCK. More info https://mydumper.github.io/mydumper/docs/html/locks.html. \nDefault: AUTO which uses the best option depending on the database vendor")
	pflag.BoolVar(&UseSavepoints, "use-savepoints", false, "Use savepoints to reduce metadata locking issues, needs SUPER privilege")
	pflag.BoolVar(&NoBackupLocks, "no-backup-locks", false, "Do not use Percona backup locks")
	pflag.BoolVar(&LessLocking, "less-locking", false, "This option is deprecated and its behaviour is the default which is useful if you don't have transaction tables. Use --trx-tables otherwise")
	pflag.BoolVar(&TrxConsistencyOnly, "trx-consistency-only", false, "This option is deprecated use --trx-tables instead")
	pflag.IntVar(&TrxTables, "trx-tables", 1, "The backup process changes, if we know that we are exporting transactional tables only")
	pflag.BoolVar(&SkipDdlLocks, "skip-ddl-locks", false, "Do not send DDL locks when possible")
}

// query_running_entries registers long-query-guard, kill-long-queries, and retry options.
func query_running_entries() {
	pflag.IntVar(&LongqueryRetries, "long-query-retries", 0, "Retry checking for long queries, default 0 (do not retry)")
	pflag.IntVar(&LongqueryRetryInterval, "long-query-retry-interval", 60, "Time to wait before retrying the long query check in seconds")
	pflag.Uint64VarP(&Longquery, "long-query-guard", "l", 60, "Set long query timer in seconds")
	pflag.BoolVarP(&Killqueries, "kill-long-queries", "K", false, "Kill long running queries (instead of aborting)")

}

// exec_entries registers exec and exec-per-thread related flags.
func exec_entries() {
	pflag.UintVar(&Num_exec_threads, "exec-threads", 4, "Amount of threads to use with --exec")
	pflag.StringVar(&Exec_command, "exec", "", "Command to execute using the file as parameter")
	pflag.StringVar(&Exec_per_thread, "exec-per-thread", "", "Set the command that will receive by STDIN and write in the STDOUT into the output file")
	pflag.StringVar(&ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the extension for the STDOUT file when --exec-per-thread is used")
}

// pmm_entries registers PMM collector path and resolution flags.
func pmm_entries() {
	// pmm
	pflag.StringVar(&PmmPath, "pmm-path", "", "which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution")
	pflag.StringVar(&PmmResolution, "pmm-resolution", "", "which default will be high")

}

// daemon_entries registers daemon mode, pid-file, snapshot-interval, and snapshot-count.
func daemon_entries() {
	pflag.BoolVarP(&DaemonMode, "daemon", "D", false, "Enable daemon mode")
	pflag.StringVar(&PidFile, "pid-file", fmt.Sprintf("/tmp/%s.pid", MYDUMPER), "Pid file used by Daemon mode.")
	pflag.IntVarP(&SnapshotInterval, "snapshot-interval", "I", 60, "Interval between each dump snapshot (in minutes), requires --daemon,default 60")
	pflag.IntVarP(&SnapshotCount, "snapshot-count", "X", 2, "number of snapshots, default 2")
}

// chunks_entries registers chunk-related flags: rows, max-threads-per-table, split-partitions, etc.
func chunks_entries() {
	// chunks
	pflag.IntVar(&MaxTimePerSelect, "max-time-per-select", 2, "Maximum amount of seconds that a select should take. Default: 2")
	pflag.UintVar(&MaxThreadsPerTable, "max-threads-per-table", 4, "Maximum number of threads per table to use")
	pflag.BoolVar(&UseSingleColumn, "use-single-column", false, "It will ignore_engines if the table has multiple columns and use only the first column to split the table")
	pflag.StringVarP(&RowsPerChunk, "rows", "r", "", "Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds")
	pflag.StringVar(&RowsHard, "rows-hard", "", "This set the MIN and MAX limit when even if --rows is 0")
	pflag.BoolVar(&SplitPartitions, "split-partitions", false, "Dump partitions into separate files. This options overrides the --rows option for partitioned tables.")

}

// checksum_entries registers checksum-all, data-checksums, schema-checksums, routine-checksums.
func checksum_entries() {
	// Checksum
	pflag.BoolVarP(&DumpChecksums, "checksum-all", "M", false, "Dump checksums for all elements")
	pflag.BoolVar(&DataChecksums, "data-checksums", false, "Dump table checksums with the data")
	pflag.BoolVar(&SchemaChecksums, "schema-checksums", false, "Dump schema table and view creation checksums")
	pflag.BoolVar(&RoutineChecksums, "routine-checksums", false, "Dump triggers, functions and routines checksums")

}

// filter_entries registers database, ignore-engines, where, updated-since, partition-regex.
func filter_entries() {
	// filter
	pflag.StringVarP(&DB, "database", "B", "", "Comma delimited list of databases to dump")
	pflag.StringVarP(&IgnoreEnginesStr, "ignore-engines-engines", "i", "", "Comma delimited list of storage engines to ignore_engines")
	pflag.StringVar(&WhereOption, "where", "", "Dump only selected records.")
	pflag.IntVarP(&UpdatedSince, "updated-since", "U", 0, "Use Update_time to dump only tables updated in the last U days")
	pflag.StringVar(&PartitionRegex, "partition-regex", "", "Regex to filter by partition name.")

}

// objects_entries registers no-schemas, no-data, triggers, events, routines, views, etc.
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

// statement_entries registers load-data, csv, format, statement-size, complete-insert, hex-blob, etc.
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
	pflag.StringVar(&Set_names_in_conn_by_default, "set-names", "", "Sets the names, use it at your own risk, default binary")
	pflag.StringVar(&DefaultCharacterSet, "default-character-set", "", "Accepts a list of up to 2 charsets, and executes 'SET NAMES' with the proper charset from the list, where the first item is used when executes SHOW CREATE TABLE and the second item is used for the rest. Use it at your own risk as it might cause inconsistencies #1974. Default: auto,binary. auto means that it is going to use the table character set.")
	pflag.StringVar(&TableEngineForViewDependency, "table-engine-for-view-dependency", MEMORY, "Table engine to be used for the CREATE TABLE statement for temporary tables when using views")
}

// load_contex_entries registers all mydumper flag groups, parses flags, and runs connection/stream/arguments callbacks.
func load_contex_entries() {
	entries()
	Common_entries()
	Connection_entries()
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
	Connection_arguments_callback()
	Stream_arguments_callback()
	arguments_callback()
	_ = Set_verbose()
}

// arguments_callback applies post-parse logic: compress method, output format, lock mode, rows per chunk, etc.; returns true to continue.
func arguments_callback() bool {
	if Compress {
		if Exec_command == "" {
			compress_method = GZIP
		} else {
			if strings.EqualFold(Exec_command, GZIP) {
				compress_method = GZIP
			} else if strings.EqualFold(Exec_command, ZSTD) {
				compress_method = ZSTD
			} else {
				log.Fatalf("Unknown compression method %s", Exec_command)
			}
		}

	}
	if RowsHard != "" {
		parse_rows_per_chunk(RowsHard, &min_integer_chunk_step_size, &max_integer_chunk_step_size, &max_integer_chunk_step_size, "Invalid option on --rows-hard")
	}
	if TrxTables > 0 {
		log.Debugf("Setting --less-locking and --trx-consistency-only to true because --trx-tables is set to %d", TrxTables)
	}
	if RowsPerChunk != "" {
		split_integer_tables = parse_rows_per_chunk(RowsPerChunk, &min_chunk_step_size, &starting_chunk_step_size, &max_chunk_step_size, "Invalid option on --rows")
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
			log.Criticalf("Unknown output format %s", OutputFormat)
			return false
		}
	}
	if TrxConsistencyOnly {
		M_critical("--trx-consistency-only is deprecated use --trx-tables instead")
	}
	if LessLocking {
		M_critical("--less-locking is deprecated and its behaviour is the default which is useful if you don't have transaction tables. Use --trx-tables otherwise")
	}
	if LockAllTables {
		M_critical("--lock-all-tables is deprecated use --sync-thread-lock-mode instead")
	}
	if NoLocks {
		M_critical("--no-locks is deprecated use --sync-thread-lock-mode instead")
	}
	if SyncThreadLockMode != "" {
		if strings.EqualFold(SyncThreadLockMode, AUTO) {
			SyncThreadLockMode = AUTO
		} else if strings.EqualFold(SyncThreadLockMode, FTWRL) {
			SyncThreadLockMode = FTWRL
		} else if strings.EqualFold(SyncThreadLockMode, LOCK_ALL) {
			SyncThreadLockMode = LOCK_ALL
		} else if strings.EqualFold(SyncThreadLockMode, GTID) {
			SyncThreadLockMode = GTID
		} else if strings.EqualFold(SyncThreadLockMode, NO_LOCK) {
			SyncThreadLockMode = NO_LOCK
		} else if strings.EqualFold(SyncThreadLockMode, SAFE_NO_LOCK) {
			SyncThreadLockMode = SAFE_NO_LOCK
		} else {
			log.Criticalf("Unknown sync thread lock mode %s", SyncThreadLockMode)
		}
	}
	if SuccessOn1146 {
		M_critical("--success-on-1146 is deprecated use --ignore_engines-Errors instead")
	}
	if DefaultCharacterSet != "" {
		var value_split = strings.SplitN(DefaultCharacterSet, ",", 2)
		if len(value_split) == 0 {
			log.Criticalf("Invalid value for --default-character-set")
		}
		SetNamesInConnForSct = value_split[0]
		if len(value_split) > 1 {
			Set_names_in_conn_by_default = value_split[1]
		} else {
			Set_names_in_conn_by_default = SetNamesInConnForSct
		}
	}
	if SetNamesStr != "" {
		var value_split = strings.SplitN(SetNamesStr, ",", 2)
		if len(value_split) == 0 {
			log.Criticalf("Invalid value for --set-names")
		}
		SetNamesInFileForSct = value_split[0]
		if len(value_split) > 1 {
			SetNamesInFileByDefault = value_split[1]
		} else {
			SetNamesInFileByDefault = SetNamesInFileForSct
		}
	}

	return Common_arguments_callback()
}

// connection_arguments_callback applies connection-related post-parse options (e.g. defaults file).
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
