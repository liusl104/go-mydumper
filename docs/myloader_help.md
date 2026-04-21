# myloader help
```shell
Usage:
  myloader [OPTION…] multi-threaded MySQL dumping
  -r, --Rows int                               Split the INSERT statement into this many Rows.
      --append-if-not-exist                    Appends IF NOT EXISTS to the create table statements. This will be removed when https://bugs.mysql.com/bug.php?id=103791 has been implemented
  -a, --ask-password                           Prompt For User password
  -b, --buffer-size uint                       Queue buffer size (default 200000)
      --ca string                              The path name to the certificate authority file
      --capath string                          The path name to a directory that contains trusted SSL CA certificates in PEM format
      --cert string                            The path name to the certificate file
      --checksum string                        Treat checksums: skip, fail(default), warn.
      --cipher string                          A list of permissible ciphers to use for SSL encryption
  -B, --database string                        An alternative database to restore into
      --debug                                  (automatically sets verbosity to 4),Turn on debugging output,(automatically sets verbosity to 4)
      --defaults-extra-file string             Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
      --defaults-file string                   Use a specific defaults file. Default: /etc/mydumper.cnf
  -d, --directory string                       Directory of the dump to import
      --disable-redo-log                       Disables the REDO_LOG and enables it after, doesn't check initial status
      --drop-database                          Executes a DROP DATABASE if the schema database file is found.
  -o, --drop-table string                      Executes or simulates a DROP TABLE if the table already exists. The drop modes can be: FAIL, NONE, DROP, TRUNCATE and DELETE. If the option is not set, the default is set to: FAIL. If the option is used without a parameter, the default is: DROP.
  -e, --enable-binlog                          Enable binary logging of the restore data
      --exec-per-thread string                 Set the command that will receive by STDIN from the input file and write in the STDOUT
      --exec-per-thread-extension string       Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored
      --fifodir string                         Directory where the FIFO files will be created when needed. Default: Same as backup
  -?, --help                                   Show help options
  -h, --host string                            The host to connect to
      --ignore-errors string                   Not increment error count and Warning instead of Critical in case of any of the comman separated error number list
      --ignore-set string                      List of variables that will be ignored from the header of SET
      --innodb-optimize-keys string            Option --innodb-optimize-keys is deprecated use --optimize-keys instead
      --json                                   log output format in JSON, need log level is debug mode
      --key string                             The path name to the key file
  -k, --kill-at-once                           When Ctrl+c is pressed it immediately terminates the process
      --local-infile                           Enables the ability to use the 'LOAD DATA LOCAL INFILE' statement Default: detect from metadata file if possible, otherwise is disabled
  -L, --logfile string                         Log file name to use, by default stdout is used
      --max-statement-size uint                Informs what is the max statement size. Currently not being used.
      --max-threads-for-index-creation uint    Maximum number of threads for index creation, default 4 (default 4)
      --max-threads-for-post-actions uint      Maximum number of threads for post action like: constraints, procedure, views and triggers, default 1 (default 1)
      --max-threads-for-schema-creation uint   Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4 (default 4)
      --max-threads-per-table uint             Maximum number of threads per table to use, defaults to --threads
      --max-transaction-size uint              Set the max size of the transaction in megabytes, default 1000 (default 1000)
      --metadata-refresh-interval uint         Every this amount of tables the internal metadata will be refreshed. If the amount of tables you have in your metadata file is high, then you should increase this value. Default: 100 (default 100)
      --mysqldump                              It expect a mysqldump format when stream is used
      --no-data                                Do not dump or import table data
      --no-schema                              Do not import table schemas and triggers
  -O, --omit-from-file string                  File containing a list of database.table entries to skip, one per line (skips before applying regex option)
      --optimize-keys string                   Creates the table without the indexes unless SKIP is selected. It will add the indexes right after completing the table restoration by default or after importing all the tables. Options: AFTER_IMPORT_PER_TABLE, AFTER_IMPORT_ALL_TABLES and SKIP. Default: AFTER_IMPORT_PER_TABLE
      --optimize-keys-engines string           List of engines that will be used to split the create table statement into multiple stages if possible. Default: InnoDB,ROCKSDB
      --overwrite-tables                       Option --overwrite-tables has been deprecated. User -o/--drop-table instead.
      --overwrite-unsafe                       Same as --overwrite-tables but starts data load as soon as possible. May cause InnoDB deadlocks for foreign keys.
  -p, --password string                        User password
      --pmm-path string                        which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
      --pmm-resolution string                  which default will be high
  -P, --port int                               TCP/IP port to connect to (default 3306)
      --protocol string                        The protocol to use for connection (tcp, socket) (default "tcp")
      --purge-mode string                      This specify the truncate mode which can be: FAIL, NONE, DROP, TRUNCATE and DELETE. Default if not set: FAIL
  -q, --queries-per-transaction uint           Number of queries per transaction, default 1000 (default 1000)
  -Q, --quote-character string                 Identifier quote character used in INSERT statements. Posible values are: BACKTICK, bt, ` for backtick and DOUBLE_QUOTE, dt, " for double quote. Default: detect from dump if possible, otherwise BACKTICK
      --resume                                 Expect to find resume file in backup dir and will only process those files
      --retry-count uint                       Lock wait timeout exceeded retry count, default 10 (currently only for DROP TABLE) (default 10)
      --serialized-table-creation              Table recreation will be executed in series, one thread at a time. This means --max-threads-for-schema-creation=1. This option will be removed in future releases
      --server-version string                  Set the server version avoid automatic detection
      --set-gtid-purged                        After import, it will execute the SET GLOBAL gtid_purged with the value found on source section of the metadata file
      --set-names string                       Sets the names, use it at your own risk, default binary
      --show-warnings                          If enabled, during INSERT IGNORE the warnings will be printed
      --skip-constraints                       Do not import constraints. By default, it imports contraints
      --skip-define                            Removes DEFINER from the CREATE statement. By default, statements are not modified
      --skip-indexes                           Do not import secondary index on InnoDB tables. By default, it import the indexes
      --skip-post                              Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions
      --skip-table-sorting                     Starting with largest table is better, but this can be ignored due performance impact when you have high amount of tables
      --skip-triggers                          Do not import triggers. By default, it imports triggers
  -S, --socket string                          UNIX domain socket file to use for connection
      --source-control-command string          Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS (default "TRADITIONAL")
      --source-data string                     It will include the options in the metadata file, to allow myloader to establish replication
  -s, --source-db string                       Database to restore
      --ssl                                    Connect using SSL
      --ssl-mode string                        Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
      --stream string                          It will receive the stream from STDIN and creates the file in the disk before start processing.Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given
  -T, --tables-list string                     Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
  -t, --threads uint                           Number of threads to use, 0 means to use number of CPUs (default 4)
      --throttle string                        xpects a string like Threads_running=10. It will check the SHOW GLOBAL STATUS and if it is higher, it will increase the sleep time between SELECT.
                                               If option is used without parameters it will use Threads_running and the amount of threads
      --tls-version string                     Which protocols the server permits for encrypted connections
  -u, --user string                            Username with the necessary privileges
  -v, --verbose uint                           Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, 4 = debug (default 2)
  -V, --version                                Show the program version and exit
# host                                  =
# user                                  =
# password                              =
# ask-password                          = FALSE
port                                    = 3306
# socket                                =
# protocol                              =
# compress-protocol                     = FALSE
# ssl                                   = FALSE
# ssl-mode                              =
# key                                   =
# cert                                  =
# ca                                    =
# capath                                =
# cipher                                =
# tls-version                           =
# regex                                 = ""
# source-db                             =
# skip-triggers                         = FALSE
# skip-constraints                      = FALSE
# skip-indexes                          = FALSE
# skip-post                             = FALSE
# no-data                               = FALSE
# omit-from-file                        =
# tables-list                           =
# pmm-path                              =
# pmm-resolution                        =
# enable-binlog                         = FALSE
innodb-optimize-keys                    = AFTER_IMPORT_PER_TABLE
# no-schemas                            = FALSE
# purge-mode                            =
# disable-redo-log                      = FALSE
# checksum                              =
# overwrite-tables                      = FALSE
# overwrite-unsafe                      = FALSE
retry-count                             = 10
# serialized-table-creation             = FALSE
# stream                                = FALSE
max-threads-per-table                   = 0
max-threads-for-index-creation          = 4
max-threads-for-post-actions            = 1
max-threads-for-schema-creation         = 4
# exec-per-thread                       =
# exec-per-thread-extension             =
rows                                    = 0
queries-per-transaction                 = 1000
# append-if-not-exist                   = FALSE
set-names                               = binary
# skip-definer                          = FALSE
help                                    = TRUE
# directory                             =
# logfile                               =
# database                              =
quote-character                         = `
# resume                                = FALSE
threads                                 = 4
# version                               = FALSE
verbose                                 = TRUE
# debug                                 = FALSE
# defaults-file                         =
# defaults-extra-file                   =
# fifodir                               =
```