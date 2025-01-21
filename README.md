# go-mydumper
Go language development is based on the mydumper-0.16.7-5 package and command line multi-threaded data tools
# mydumper help
```shell
Usage:
  mydumper [OPTION…] multi-threaded MySQL dumping
  -Y, --all-tablespaces                    Dump all the tablespaces.
  -a, --ask-password                       Prompt For User password
  -b, --buffer-size uint                   Queue buffer size (default 200000)
  -e, --build_empty_files                  Build dump files even if no data available from table
      --ca string                          The path name to the certificate authority file
      --capath string                      The path name to a directory that contains trusted SSL CA certificates in PEM format
      --cert string                        The path name to the certificate file
      --char-chunk uint                    Defines in how many pieces should split the table. By default we use the amount of threads
      --char-deep uint                     Defines the amount of characters to use when the primary key is a string
      --check-row-count                    Run SELECT COUNT(*) and fail mydumper if dumped row count is different
  -M, --checksum-all                       Dump checksums for all elements
  -F, --chunk-filesize uint                Split data files into pieces of this size in MB. Useful for myloader multi-threading.
      --cipher string                      A list of permissible ciphers to use for SSL encryption
      --clear                              Clear output directory before dumping
      --compact                            Give less verbose output. Disables header/footer constructs.
      --complete-insert                    Use complete INSERT statements that include column names
  -c, --compress string                    Compress output files
      --csv                                Automatically enables --load-data and set variables to export in CSV format.
  -D, --daemon                             Enable daemon mode
      --data-checksums                     Dump table checksums with the data
  -B, --database string                    Comma delimited list of databases to dump
      --debug                              (automatically sets verbosity to 4),Print more info
      --defaults-extra-file string         Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
      --defaults-file string               Use a specific defaults file. Default: /etc/mydumper.cnf
      --dirty                              Overwrite output directory without clearing (beware of leftower chunks)
      --disk-limits string                 Set the limit to pause and resume if determines there is no enough disk space.
                                           Accepts values like: '<resume>:<pause>' in MB.
                                           For instance: 100:500 will pause when there is only 100MB free and will
                                           resume if 500MB are available
  -E, --events                             Dump events. By default, it do not dump events
      --exec string                        Command to execute using the file as parameter
      --exec-per-thread string             Set the command that will receive by STDIN and write in the STDOUT into the output file
      --exec-per-thread-extension string   Set the extension for the STDOUT file when --exec-per-thread is used
      --exec-threads uint                  Amount of threads to use with --exec (default 4)
      --exit-if-broken-table-found         Exits if a broken table has been found
      --fields-enclosed-by string          Defines the character to enclose fields. Default: "
      --fields-escaped-by string           Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\'
      --fields-terminated-by string        Defines the character that is written between fields
      --fifodir string                     Directory where the FIFO files will be created when needed. Default: Same as backup
      --format string                      Sets the names, use it at your own risk (default "INSERT")
  -?, --help                               Show help options
      --hex-blob                           Dump binary columns using hexadecimal notation
  -h, --host string                        The host to connect to
  -i, --ignore-engines string              Comma delimited list of storage engines to ignore
      --include-header                     When --load-data or --csv is used, it will include the header with the column name
      --insert-ignore                      Dump rows with INSERT IGNORE
      --json                               log output format in JSON, need log level is debug mode
      --key string                         The path name to the key file
  -K, --kill-long-queries                  Kill long running queries (instead of aborting)
      --less-locking                       Minimize locking time on InnoDB tables.
      --lines-starting-by string           Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
      --lines-terminated-by string         Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
      --load-data                          Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files
      --lock-all-tables                    Use LOCK TABLE for all, instead of FTWRL
  -L, --logfile string                     Log file name to use, by default stdout is used
  -l, --long-query-guard uint              Set long query timer in seconds (default 60)
      --long-query-retries int             Retry checking for long queries, default 0 (do not retry)
      --long-query-retry-interval int      Time to wait before retrying the long query check in seconds (default 60)
      --max-threads-per-table uint         Maximum number of threads per table to use (default 4)
      --no-backup-locks                    Do not use Percona backup locks
      --no-check-generated-fields          Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns
  -d, --no-data                            Do not dump table data
  -k, --no-locks                           Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups
  -m, --no-schemas                         Do not dump table schemas with the data and triggers
  -W, --no-views                           Do not dump VIEWs
  -O, --omit-from-file string              File containing a list of database.table entries to skip, one per line (skips before applying regex option)
      --order-by-primary                   Sort the data by Primary Key or Unique key if no primary key exists
  -o, --outputdir string                   Directory to output files to
      --partition-regex string             Regex to filter by partition name.
  -p, --password string                    User password
      --pid-file string                    Pid file used by Daemon mode. (default "/tmp/mydumper.pid")
      --pmm-path string                    which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
      --pmm-resolution string              which default will be high
  -P, --port int                           TCP/IP port to connect to (default 3306)
      --protocol string                    The protocol to use for connection (tcp, socket) (default "tcp")
      --replace                            Dump rows with REPLACE
      --routine-checksums                  Dump triggers, functions and routines checksums
  -R, --routines                           Dump stored procedures and functions. By default, it do not dump stored procedures nor functions
  -r, --rows string                        Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds
      --schema-checksums                   Dump schema table and view creation checksums
      --set-names string                   Sets the names, use it at your own risk
      --skip-constraints                   Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified
      --skip-ddl-locks                     Do not send DDL locks when possible
      --skip-definer                       Removes DEFINER from the CREATE statement. By default, statements are not modified
      --skip-indexes                       Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified
      --skip-tz-utc                        Doesn't add SET TIMEZONE on the backup files
  -X, --snapshot-count int                 number of snapshots, default 2 (default 2)
  -I, --snapshot-interval int              Interval between each dump snapshot (in minutes), requires --daemon,default 60 (default 60)
  -S, --socket string                      UNIX domain socket file to use for connection
      --source-control-command string      Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS
      --source-data int                    It will include the options in the metadata file, to allow myloader to establish replication.
      --split-partitions                   Dump partitions into separate files. This options overrides the --rows option for partitioned tables.
      --ssl                                Connect using SSL
      --ssl-mode string                    Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
  -s, --statement-size int                 Attempted size of INSERT statement in bytes (default 1000000)
      --statement-terminated-by string     This might never be used, unless you know what are you doing
      --success_on_1146                    Not increment error count and Warning instead of Critical in case of table doesn't exist
  -T, --tables-list string                 Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
  -t, --threads uint                       Number of threads to use, 0 means to use number of CPUs. Default: 4
  -z, --tidb-snapshot string               Snapshot to use for TiDB
      --tls-version string                 Which protocols the server permits for encrypted connections
  -G, --triggers                           Dump triggers. By default, it do not dump triggers
      --trx-consistency-only               Transactional consistency only
      --tz-utc                             SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.
  -U, --updated-since int                  Use Update_time to dump only tables updated in the last U days
      --use-defer                          Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables).
      --use-savepoints                     Use savepoints to reduce metadata locking issues, needs SUPER privilege
  -u, --user string                        Username with the necessary privileges
  -v, --verbose uint                       Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info (default 2)
  -V, --version                            Show the program version and exit
      --views-as-tables                    Export VIEWs as they were tables
      --where string                       Dump only selected records.
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
# database                              =
# ignore-engines                        =
# where                                 =
updated-since                           = 0
# partition-regex                       =
# omit-from-file                        =
# tables-list                           =
# tidb-snapshot                         =
# no-locks                              = FALSE
# use-savepoints                        = FALSE
# no-backup-locks                       = FALSE
# lock-all-tables                       = FALSE
# less-locking                          = FALSE
# trx-consistency-only                  = FALSE
# skip-ddl-locks                        = FALSE
# pmm-path                              =
# pmm-resolution                        =
exec-threads                            = 4
# exec                                  =
# exec-per-thread                       =
# exec-per-thread-extension             =
long-query-retries                      = 0
long-query-retry-interval               = 60
long-query-guard                        = 60
# kill-long-queries                     = FALSE
max-threads-per-table                   = 4
rows                                    = 0:0:0
# split-partitions                      = FALSE
# checksum-all                          = FALSE
# data-checksums                        = FALSE
# schema-checksums                      = FALSE
# routine-checksums                     = FALSE
# no-schemas                            = FALSE
# all-tablespaces                       = FALSE
# no-data                               = FALSE
# triggers                              = FALSE
# events                                = FALSE
# routines                              = FALSE
# views-as-tables                       = FALSE
# no-views                              = FALSE
# load-data                             = FALSE
# csv                                   = FALSE
# clickhouse                            = FALSE
# include-header                        = FALSE
# fields-terminated-by                  =
# fields-enclosed-by                    =
# fields-escaped-by                     =
# lines-starting-by                     =
# lines-terminated-by                   =
# statement-terminated-by               =
# insert-ignore                         = FALSE
# replace                               = FALSE
# complete-insert                       = FALSE
# hex-blob                              = FALSE
# skip-definer                          = FALSE
statement-size                          = 1000000
# tz-utc                                = FALSE
# skip-tz-utc                           = FALSE
# set-names                             =
chunk-filesize                          = 0
# exit-if-broken-table-found            = FALSE
# success-on-1146                       = FALSE
# build-empty-files                     = FALSE
# no-check-generated-fields             = FALSE
# order-by-primary                      = FALSE
# compact                               = FALSE
# compress                              = FALSE
# use-defer                             = FALSE
# check-row-count                       = FALSE
# daemon                                = FALSE
snapshot-interval                       = 60
snapshot-count                          = 2
help                                    = TRUE
outputdir                               = export-20250121-155048
# clear                                 = FALSE
# dirty                                 = FALSE
# stream                                = FALSE
# logfile                               =
# disk-limits                           =
threads                                 = 0
# version                               = FALSE
verbose                                 = TRUE
# debug                                 = FALSE
# defaults-file                         =
# defaults-extra-file                   =
# fifodir                               =
```

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
      --debug                                  (automatically sets verbosity to 4),Print more info
      --defaults-extra-file string             Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
      --defaults-file string                   Use a specific defaults file. Default: /etc/mydumper.cnf
  -d, --directory string                       Directory of the dump to import
      --disable-redo-log                       Disables the REDO_LOG and enables it after, doesn't check initial status
  -e, --enable-binlog                          Enable binary logging of the restore data
      --exec-per-thread string                 Set the command that will receive by STDIN from the input file and write in the STDOUT
      --exec-per-thread-extension string       Set the input file extension when --exec-per-thread is used. Otherwise it will be ignored
      --fifodir string                         Directory where the FIFO files will be created when needed. Default: Same as backup
  -?, --help                                   Show help options
  -h, --host string                            The host to connect to
      --ignore-errors string                   Not increment error count and Warning instead of Critical in case of any of the comman separated error number list
      --ignore-set string                      List of variables that will be ignored from the header of SET
      --innodb-optimize-keys string            Creates the table without the indexes unless SKIP is selected.
                                               It will add the indexes right after complete the table restoration by default or after import all the tables.
                                               Options: AFTER_IMPORT_PER_TABLE, AFTER_IMPORT_ALL_TABLES and SKIP. Default: AFTER_IMPORT_PER_TABLE
      --json                                   log output format in JSON, need log level is debug mode
      --key string                             The path name to the key file
  -k, --kill-at-once                           When Ctrl+c is pressed it immediately terminates the process
  -L, --logfile string                         Log file name to use, by default stdout is used
      --max-threads-for-index-creation uint    Maximum number of threads for index creation, default 4 (default 4)
      --max-threads-for-post-actions uint      Maximum number of threads for post action like: constraints, procedure, views and triggers, default 1 (default 1)
      --max-threads-for-schema-creation uint   Maximum number of threads for schema creation. When this is set to 1, is the same than --serialized-table-creation, default 4 (default 4)
      --max-threads-per-table uint             Maximum number of threads per table to use, defaults to --threads
      --metadata-refresh-interval uint         Every this amount of tables the internal metadata will be refreshed. If the amount of tables you have in your metadata file is high, then you should increase this value. Default: 100 (default 100)
      --no-data                                Do not dump or import table data
      --no-schema                              Do not import table schemas and triggers
  -O, --omit-from-file string                  File containing a list of database.table entries to skip, one per line (skips before applying regex option)
  -o, --overwrite-tables                       Drop tables if they already exist
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
      --set-gtid-purged                        After import, it will execute the SET GLOBAL gtid_purged with the value found on source section of the metadata file
      --set-names string                       Sets the names, use it at your own risk, default binary
      --show-warnings                          If enabled, during INSERT IGNORE the warnings will be printed
      --skip-constraints                       Do not import constraints. By default, it imports contraints
      --skip-define                            Removes DEFINER from the CREATE statement. By default, statements are not modified
      --skip-indexes                           Do not import secondary index on InnoDB tables. By default, it import the indexes
      --skip-post                              Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions
      --skip-triggers                          Do not import triggers. By default, it imports triggers
  -S, --socket string                          UNIX domain socket file to use for connection
      --source-control-command string          Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS
  -s, --source-db string                       Database to restore
      --ssl                                    Connect using SSL
      --ssl-mode string                        Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
      --stream string                          It will receive the stream from STDIN and creates the file in the disk before start processing.Since v0.12.7-1, accepts NO_DELETE, NO_STREAM_AND_NO_DELETE and TRADITIONAL which is the default value and used if no parameter is given
  -T, --tables-list string                     Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
  -t, --threads uint                           Number of threads to use, 0 means to use number of CPUs. Default: 4
      --tls-version string                     Which protocols the server permits for encrypted connections
  -u, --user string                            Username with the necessary privileges
  -v, --verbose uint                           Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info (default 2)
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