package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"github.com/spf13/pflag"
	"strings"
)

type checksum_modes int

const (
	CHECKSUM_SKIP checksum_modes = iota
	CHECKSUM_WARN
	CHECKSUM_FAIL
)
const (
	AFTER_IMPORT_PER_TABLE  = "AFTER_IMPORT_PER_TABLE"
	AFTER_IMPORT_ALL_TABLES = "AFTER_IMPORT_ALL_TABLES"
	SKIP                    = "SKIP"
)

var (
	checksum_mode checksum_modes = CHECKSUM_FAIL
	checksum_str  string
)

var (
	EnableBinlog                bool
	InnodbOptimizeKeys          string
	OptimizeKeys                string
	PurgeModeStr                string
	DisableRedoLog              bool
	CheckSum                    string
	OverwriteTables             bool
	overwrite_tables            bool
	OverwriteUnsafe             bool
	RetryCount                  uint = 10
	SerialTblCreation           bool
	RefreshTableListInterval    uint = 100
	SetGtidPurge                bool
	Rows                        int
	CommitCount                 uint = 1000
	AppendIfNotExist            bool
	SkipDefiner                 bool
	IgnoreSet                   string
	InputDirectory              string
	QuoteCharacter              string
	ShowWarnings                bool
	Resume                      bool
	KillAtOnce                  bool
	MaxThreadsForIndexCreation  uint = 4
	MaxThreadsForPostCreation   uint = 1
	MaxThreadsForSchemaCreation uint = 4
	ExecPerThread               string
	ExecPerThreadExtension      string
	SourceDb                    string
	SkipTriggers                bool
	SkipPost                    bool
	SkipConstraints             bool
	SkipIndexes                 bool
	MySQLDump                   bool
	DropDatabase                bool
	DropTableStr                string
	SkipTableSorting            bool
	MaxStatementSize            uint64
	MaxTransactionSize          uint64 = DEFAULT_MAX_TRANSACTION_SIZE
	optimize_keys_str           string
	ignore_set_items            []string
	ignore_set_list             []string
)

func arguments_callback() bool {
	if InnodbOptimizeKeys != "" {
		log.Criticalf("Option --innodb-optimize-keys is deprecated use --optimize-keys instead")
	}
	if OptimizeKeys != "" {
		optimize_keys_str = OptimizeKeys
		if OptimizeKeys == "" || OptimizeKeys == "1" {
			optimize_keys_per_table = true
			optimize_keys_all_tables = false
		} else if strings.EqualFold(OptimizeKeys, SKIP) {
			optimize_keys = false
			optimize_keys_per_table = false
			optimize_keys_all_tables = false
		} else if strings.EqualFold(OptimizeKeys, AFTER_IMPORT_PER_TABLE) {
			optimize_keys_per_table = true
			optimize_keys_all_tables = false
		} else if strings.EqualFold(OptimizeKeys, AFTER_IMPORT_ALL_TABLES) {
			optimize_keys_all_tables = true
			optimize_keys_per_table = false
		} else {
			log.Criticalf("--optimize-keys accepts: after_import_per_table (default value), after_import_all_tables")
		}
	}

	if QuoteCharacter != "" {
		if strings.EqualFold(QuoteCharacter, "BACKTICK") || strings.EqualFold(QuoteCharacter, "BT") || strings.EqualFold(QuoteCharacter, "`") {
			Identifier_quote_character = BACKTICK
		} else if strings.EqualFold(QuoteCharacter, "DOUBLE_QUOTE") || strings.EqualFold(QuoteCharacter, "DT") || strings.EqualFold(QuoteCharacter, "\"") {
			Identifier_quote_character = DOUBLE_QUOTE
		} else {
			log.Criticalf("--quote-character accepts: backtick, bt, `, double_quote, dt, \"")
		}
	}
	if CheckSum != "" {
		checksum_str = CheckSum
		if checksum_str == "" || strings.EqualFold(CheckSum, "FAIL") {
			checksum_mode = CHECKSUM_FAIL
		} else if strings.EqualFold(CheckSum, "WARN") {
			checksum_mode = CHECKSUM_WARN
		} else if strings.EqualFold(CheckSum, "SKIP") {
			checksum_mode = CHECKSUM_SKIP
		} else {
			log.Criticalf("--checksum accepts: fail (default), warn, skip")
		}
	}
	if IgnoreSet != "" {
		ignore_set_items = strings.Split(IgnoreSet, ",")
		var i uint
		for i = 0; i < uint(len(ignore_set_items)); i++ {
			ignore_set_list = append(ignore_set_list, ignore_set_items[i])
		}
	}
	if OverwriteTables {
		log.Criticalf("Option --overwrite-tables has been deprecated. User -o/--drop-table instead")
	}
	if PurgeModeStr != "" {
		log.Criticalf("Option --purge-mode has been deprecated. User -o/--drop-table instead")
	}
	if DropTableStr != "" {
		overwrite_tables = true
		if strings.EqualFold(DropTableStr, "TRUNCATE") {
			purge_mode = TRUNCATE
		} else if strings.EqualFold(DropTableStr, "DROP") || strings.EqualFold(DropTableStr, "1") || strings.EqualFold(DropTableStr, "") {
			purge_mode = DROP
		} else if strings.EqualFold(DropTableStr, "NONE") {
			purge_mode = NONE
		} else if strings.EqualFold(DropTableStr, "FAIL") {
			purge_mode = FAIL
		} else if strings.EqualFold(DropTableStr, "DELETE") {
			purge_mode = DELETE
		} else {
			log.Criticalf("Purge mode unknown: %s", DropTableStr)
		}
	} else {
		purge_mode = DROP
	}
	if EnableBinlog {
		log.Criticalf("Option --enable-binlog / -e is discouraged. Use [myloader_session_variables] in the --defaults-file or --defaults-extra-file instead")
	}
	return Common_arguments_callback()
}

func entries() {
	pflag.BoolVarP(&Help, "help", "?", false, "Show help options")
	pflag.UintVarP(&BufferSize, "buffer-size", "b", 200000, "Queue buffer size")
	pflag.StringVarP(&InputDirectory, "directory", "d", "", "Directory of the dump to import")
	pflag.StringVar(&FifoDirectory, "fifodir", "", "Directory where the FIFO files will be created when needed. Default: Same as backup")
	pflag.StringVarP(&LogFile, "logfile", "L", "", "Log file name to use, by default stdout is used")
	pflag.StringVarP(&DB, "database", "B", "", "An alternative database to restore into")
	pflag.BoolVar(&ShowWarnings, "show-warnings", false, "If enabled, during INSERT IGNORE the warnings will be printed")
	pflag.BoolVar(&Resume, "resume", false, "Expect to find resume file in backup dir and will only process those files")
	pflag.BoolVarP(&KillAtOnce, "kill-at-once", "k", false, "When Ctrl+c is pressed it immediately terminates the process")
	pflag.BoolVar(&MySQLDump, "mysqldump", false, "It expect a mysqldump format when stream is used")

}

func load_from_metadata_entries() {
	pflag.StringVarP(&QuoteCharacter, "quote-character", "Q", "", "Identifier quote character used in INSERT statements. "+
		"Posible values are: BACKTICK, bt, ` for backtick and DOUBLE_QUOTE, dt, \" for double quote. "+
		"Default: detect from dump if possible, otherwise BACKTICK")
	pflag.BoolVar(&LocalInFile, "local-infile", false, "Enables the ability to use the 'LOAD DATA LOCAL INFILE' statement Default: detect from metadata file if possible, otherwise is disabled")
}

func threads_entries() {
	pflag.UintVar(&MaxThreadsPerTable, "max-threads-per-table", 0, "Maximum number of threads per table to use, defaults to --threads")
	pflag.UintVar(&MaxThreadsForIndexCreation, "max-threads-for-index-creation", 4, "Maximum number of threads for index creation, default 4")
	pflag.UintVar(&MaxThreadsForPostCreation, "max-threads-for-post-actions", 1, "Maximum number of threads for post action like: constraints, procedure, views and triggers, default 1")
	pflag.UintVar(&MaxThreadsForSchemaCreation, "max-threads-for-schema-creation", 4, "Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4")
	pflag.StringVar(&ExecPerThread, "exec-per-thread", "", "Set the command that will receive by STDIN from the input file and write in the STDOUT")
	pflag.StringVar(&ExecPerThreadExtension, "exec-per-thread-extension", "", "Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored")

}

func execution_entries() {
	// Execution
	pflag.BoolVarP(&EnableBinlog, "enable-binlog", "e", false, "Enable binary logging of the restore data")
	pflag.StringVar(&InnodbOptimizeKeys, "innodb-optimize-keys", "", "Option --innodb-optimize-keys is deprecated use --optimize-keys instead")
	pflag.StringVar(&OptimizeKeys, "optimize-keys", "", "Creates the table without the indexes unless SKIP is selected. It will add the indexes right after completing the table restoration by default or after importing all the tables. Options: AFTER_IMPORT_PER_TABLE, AFTER_IMPORT_ALL_TABLES and SKIP. Default: AFTER_IMPORT_PER_TABLE")
	pflag.BoolVar(&NoSchemas, "no-schema", false, "Do not import table schemas and triggers")
	pflag.StringVar(&PurgeModeStr, "purge-mode", "", "This specify the truncate mode which can be: FAIL, NONE, DROP, TRUNCATE and DELETE. Default if not set: FAIL")
	pflag.BoolVar(&DisableRedoLog, "disable-redo-log", false, "Disables the REDO_LOG and enables it after, doesn't check initial status")
	pflag.StringVar(&CheckSum, "checksum", "", "Treat checksums: skip, fail(default), warn.")
	pflag.BoolVar(&DropDatabase, "drop-database", false, "Executes a DROP DATABASE if the schema database file is found. ")
	pflag.StringVarP(&DropTableStr, "drop-table", "o", "", "Executes or simulates a DROP TABLE if the table already exists. The drop modes can be: FAIL, NONE, DROP, TRUNCATE and DELETE. If the option is not set, the default is set to: FAIL. If the option is used without a parameter, the default is: DROP.")
	pflag.BoolVar(&OverwriteTables, "overwrite-tables", false, "Option --overwrite-tables has been deprecated. User -o/--drop-table instead.")
	pflag.BoolVar(&OverwriteUnsafe, "overwrite-unsafe", false, "Same as --overwrite-tables but starts data load as soon as possible. May cause InnoDB deadlocks for foreign keys.")
	pflag.UintVar(&RetryCount, "retry-count", 10, "Lock wait timeout exceeded retry count, default 10 (currently only for DROP TABLE)")
	pflag.BoolVar(&SerialTblCreation, "serialized-table-creation", false, "Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases")
	pflag.StringVar(&Stream, "stream", "", "It will receive the stream from STDIN and creates the file in the disk before start processing.Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given")
	pflag.UintVar(&RefreshTableListInterval, "metadata-refresh-interval", 100, "Every this amount of tables the internal metadata will be refreshed. If the amount of tables you have in your metadata file is high, then you should increase this value. Default: 100")
	pflag.BoolVar(&SkipTableSorting, "skip-table-sorting", false, "Starting with largest table is better, but this can be ignored due performance impact when you have high amount of tables")
	pflag.BoolVar(&SetGtidPurge, "set-gtid-purged", false, "After import, it will execute the SET GLOBAL gtid_purged with the value found on source section of the metadata file")
}

func filter_entries() {
	// Filter
	pflag.StringVarP(&SourceDb, "source-db", "s", "", "Database to restore")
	pflag.BoolVar(&SkipTriggers, "skip-triggers", false, "Do not import triggers. By default, it imports triggers")
	pflag.BoolVar(&SkipPost, "skip-post", false, "Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions")
	pflag.BoolVar(&SkipConstraints, "skip-constraints", false, "Do not import constraints. By default, it imports contraints")
	pflag.BoolVar(&SkipIndexes, "skip-indexes", false, "Do not import secondary index on InnoDB tables. By default, it import the indexes")
	pflag.BoolVar(&NoData, "no-data", false, "Do not dump or import table data")

}

func statement_entries() {
	// Statement
	pflag.IntVarP(&Rows, "Rows", "r", 0, "Split the INSERT statement into this many Rows.")
	pflag.UintVarP(&CommitCount, "queries-per-transaction", "q", 1000, "Number of queries per transaction, default 1000")
	pflag.Uint64Var(&MaxStatementSize, "max-statement-size", 0, "Informs what is the max statement size. Currently not being used.")
	pflag.Uint64Var(&MaxTransactionSize, "max-transaction-size", DEFAULT_MAX_TRANSACTION_SIZE, "Set the max size of the transaction in megabytes, default 1000")
	pflag.BoolVar(&AppendIfNotExist, "append-if-not-exist", false, "Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented")
	pflag.StringVar(&Set_names_in_conn_by_default, "set-names", "", "Sets the names, use it at your own risk, default binary")
	pflag.BoolVar(&SkipDefiner, "skip-define", false, "Removes DEFINER from the CREATE statement. By default, statements are not modified")
	pflag.StringVar(&IgnoreSet, "ignore-set", "", "List of variables that will be ignored from the header of SET")
}

func load_contex_entries() {
	entries()
	Common_entries()
	Connection_entries()
	filter_entries()
	Common_filter_entries()
	Pmm_entries()
	execution_entries()
	threads_entries()
	statement_entries()
	load_from_metadata_entries()
	pflag.Parse()
	Connection_arguments_callback()
	arguments_callback()
	Stream_arguments_callback()
}
