package mydumper

import (
	"fmt"
	"github.com/spf13/pflag"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	OutputDirectoryParam string
	ClearDumpDir         bool
	DirtyDumpDir         bool
	DaemonMode           bool
	PidFile              string
	SkipConstraints      bool
	SkipIndexes          bool
	shutdown_triggered   bool
	dump_directory       string
	output_directory     string
	errors               int
	DIRECTORY            string = "export"
)

func parse_disk_limits() {
	strsplit := strings.SplitN(DiskLimits, ":", 3)
	if len(strsplit) != 2 {
		log.Fatalf("Parse limit failed")
	}
	p_at, err := strconv.Atoi(strsplit[0])
	if err != nil {
		log.Fatalf("Parse limit failed")
	}
	r_at, err := strconv.Atoi(strsplit[1])
	if err != nil {
		log.Fatalf("Parse limit failed")
	}
	set_disk_limits(uint(p_at), uint(r_at))
}

func CommandDump() error {
	load_contex_entries()
	Initialize_share_common()
	if LoadData {
		output_format = LOAD_DATA
		rows_file_extension = DAT
	}
	if Csv {
		output_format = CSV
		rows_file_extension = DAT
	}
	if OutputDirectoryParam == "" {
		dt := time.Now()
		var datetimestr string
		datetimestr = dt.Format("20060102-150405")
		output_directory = fmt.Sprintf("%s-%s", DIRECTORY, datetimestr)
	} else {
		output_directory = OutputDirectoryParam
	}
	if Debug {
		Set_debug()
		_ = Set_verbose()
	} else {
		_ = Set_verbose()
	}
	Initialize_common_options(MYDUMPER)
	Hide_password()
	if Help {
		print_help()
	}
	if ProgramVersion {
		Print_version(MYDUMPER)
		if !Help {
			os.Exit(EXIT_SUCCESS)
		}
	}

	log.Infof("MyDumper backup version: %s", VERSION)

	Ask_password()
	if !DaemonMode {
		err := StartDump()
		if err != nil {
			return err
		}
	}
	Create_backup_dir(output_directory, "")
	if DiskLimits != "" {
		parse_disk_limits()
	}
	if NumThreads > 2 {
		UseDefer = false
	}
	if DaemonMode {
		ClearDumpDir = true
		initialize_daemon_thread()
		runDaemon()
	} else {
		dump_directory = output_directory
		return StartDump()
	}
	defer func() {
		if LogFile != "" {
			_ = Log_output.Close()
		}
	}()
	return nil
}

func print_help() {
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [OPTION…] multi-threaded MySQL dumping\n", MYDUMPER)
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
	Print_string("database", DB)
	Print_string("ignore-engines", IgnoreEngines)
	Print_string("where", WhereOption)
	Print_int("updated-since", UpdatedSince)
	Print_string("partition-regex", PartitionRegex)
	Print_string("omit-from-file", TablesSkiplistFile)
	Print_string("tables-list", TablesList)
	Print_string("tidb-snapshot", TidbSnapshot)
	Print_bool("no-locks", NoLocks)
	Print_bool("use-savepoints", UseSavepoints)
	Print_bool("no-backup-locks", NoBackupLocks)
	Print_bool("lock-all-tables", LockAllTables)
	Print_bool("less-locking", LessLocking)
	Print_bool("trx-consistency-only", TrxConsistencyOnly)
	Print_bool("skip-ddl-locks", SkipDdlLocks)
	Print_string("pmm-path", PmmPath)
	Print_string("pmm-resolution", PmmResolution)
	Print_uint("exec-threads", Num_exec_threads)
	Print_string("exec", exec_command)
	Print_string("exec-per-thread", Exec_per_thread)
	Print_string("exec-per-thread-extension", ExecPerThreadExtension)
	Print_int("long-query-retries", LongqueryRetries)
	Print_int("long-query-retry-interval", LongqueryRetryInterval)
	Print_uint64("long-query-guard", Longquery)
	Print_bool("kill-long-queries", Killqueries)
	Print_uint("max-threads-per-table", MaxThreadsPerTable)
	Print_string("rows", fmt.Sprintf("%d:%d:%d", min_chunk_step_size, starting_chunk_step_size, max_chunk_step_size))
	Print_bool("split-partitions", SplitPartitions)
	Print_bool("checksum-all", DumpChecksums)
	Print_bool("data-checksums", DataChecksums)
	Print_bool("schema-checksums", SchemaChecksums)
	Print_bool("routine-checksums", RoutineChecksums)
	Print_bool("no-schemas", NoSchemas)
	Print_bool("all-tablespaces", DumpTablespaces)
	Print_bool("no-data", NoData)
	Print_bool("triggers", DumpTriggers)
	Print_bool("events", DumpEvents)
	Print_bool("routines", DumpRoutines)
	Print_bool("views-as-tables", ViewsAsTables)
	Print_bool("no-views", NoDumpViews)
	Print_bool("load-data", LoadData)
	Print_bool("csv", Csv)
	Print_bool("clickhouse", clickhouse)
	Print_bool("include-header", IncludeHeader)
	Print_string("fields-terminated-by", FieldsTerminatedByLd)
	Print_string("fields-enclosed-by", FieldsEnclosedByLd)
	Print_string("fields-escaped-by", FieldsEscapedBy)
	Print_string("lines-starting-by", LinesStartingByLd)
	Print_string("lines-terminated-by", LinesTerminatedByLd)
	Print_string("statement-terminated-by", StatementTerminatedByLd)
	Print_bool("insert-ignore", InsertIgnore)
	Print_bool("replace", Replace)
	Print_bool("complete-insert", CompleteInsert)
	Print_bool("hex-blob", HexBlob)
	Print_bool("skip-definer", SkipDefiner)
	Print_int("statement-size", StatementSize)
	Print_bool("tz-utc", SkipTz)
	Print_bool("skip-tz-utc", SkipTz)
	Print_string("set-names", SetNamesStr)
	Print_uint("chunk-filesize", ChunkFilesize)
	Print_bool("exit-if-broken-table-found", ExitIfBrokenTableFound)
	Print_bool("success-on-1146", SuccessOn1146)
	Print_bool("build-empty-files", BuildEmptyFiles)
	Print_bool("no-check-generated-fields", IgnoreGeneratedFields)
	Print_bool("order-by-primary", OrderByPrimaryKey)
	Print_bool("compact", Compact)
	Print_bool("compress", compress_method != "")
	Print_bool("use-defer", UseDefer)
	Print_bool("check-row-count", CheckRowCount)
	Print_bool("daemon", DaemonMode)
	Print_int("snapshot-interval", SnapshotInterval)
	Print_int("snapshot-count", SnapshotCount)
	Print_bool("help", Help)
	Print_string("outputdir", output_directory)
	Print_bool("clear", ClearDumpDir)
	Print_bool("dirty", DirtyDumpDir)
	Print_bool("stream", Stream != "")
	Print_string("logfile", LogFile)
	Print_string("disk-limits", DiskLimits)
	Print_uint("threads", NumThreads)
	Print_bool("version", ProgramVersion)
	Print_bool("verbose", Verbose != 0)
	Print_bool("debug", Debug)
	Print_string("defaults-file", DefaultsFile)
	Print_string("defaults-extra-file", DefaultsExtraFile)
	Print_string("fifodir", FifoDirectory)
	os.Exit(EXIT_SUCCESS)
}
