# mydumper help
```shell
Usage:
  mydumper [OPTION…] multi-threaded MySQL dumping
  -Y, --all-tablespaces                           Dump all the tablespaces.
  -a, --ask-password                              Prompt For User password
  -b, --buffer-size uint                          Queue buffer size (default 200000)
  -e, --build_empty_files                         Build dump files even if no data available from table
      --ca string                                 The path name to the certificate authority file
      --capath string                             The path name to a directory that contains trusted SSL CA certificates in PEM format
      --cert string                               The path name to the certificate file
      --check-row-count                           Run SELECT COUNT(*) and fail mydumper if dumped row count is different
  -M, --checksum-all                              Dump checksums for all elements
  -F, --chunk-filesize uint                       Split data files into pieces of this size in MB. Useful for myloader multi-threading.
      --cipher string                             A list of permissible ciphers to use for SSL encryption
      --clear                                     Clear output directory before dumping
      --compact                                   Give less verbose output. Disables header/footer constructs.
      --complete-insert                           Use complete INSERT statements that include column names
  -c, --compress string                           Compress output files
      --csv                                       Automatically enables --load-data and set variables to export in CSV format.
  -D, --daemon                                    Enable daemon mode
      --data-checksums                            Dump table checksums with the data
  -B, --database string                           Comma delimited list of databases to dump
      --debug                                     (automatically sets verbosity to 4),Turn on debugging output,(automatically sets verbosity to 4)
      --default-character-set string              Accepts a list of up to 2 charsets, and executes 'SET NAMES' with the proper charset from the list, where the first item is used when executes SHOW CREATE TABLE and the second item is used for the rest. Use it at your own risk as it might cause inconsistencies #1974. Default: auto,binary. auto means that it is going to use the table character set.
      --defaults-extra-file string                Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
      --defaults-file string                      Use a specific defaults file. Default: /etc/mydumper.cnf
      --dirty                                     Overwrite output directory without clearing (beware of leftower chunks)
      --disk-limits string                        Set the limit to pause and resume if determines there is no enough disk space.
                                                  Accepts values like: '<resume>:<pause>' in MB.
                                                  For instance: 100:500 will pause when there is only 100MB free and will
                                                  resume if 500MB are available
  -E, --events                                    Dump events. By default, it do not dump events
      --exec string                               Command to execute using the file as parameter
      --exec-per-thread string                    Set the command that will receive by STDIN and write in the STDOUT into the output file
      --exec-per-thread-extension string          Set the extension for the STDOUT file when --exec-per-thread is used
      --exec-threads uint                         Amount of threads to use with --exec (default 4)
      --exit-if-broken-table-found                Exits if a broken table has been found
      --fields-enclosed-by string                 Defines the character to enclose fields. Default: "
      --fields-escaped-by string                  Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\'
      --fields-terminated-by string               Defines the character that is written between fields
      --format string                             Sets the names, use it at your own risk (default "INSERT")
      --ftwrl-max-wait-time int                   Sets the max time that we are going to wait before kill the FLUSH TABLES related commands. Default: 60 (default 60)
      --ftwrl-timeout-retries int                 Sets the amount of retries before give up acquiring FLUSH TABLES. Default: 0, never gives up.
  -?, --help                                      Show help options
      --hex-blob                                  Dump binary columns using hexadecimal notation
  -h, --host string                               The host to connect to
      --ignore-errors string                      Not increment error count and Warning instead of Critical in case of any of the comman separated error number list
  -i, --ignore_engines-engines string             Comma delimited list of storage engines to ignore_engines
      --include-header                            When --load-data or --csv is used, it will include the header with the column name
      --insert-ignore_engines                     Dump rows with INSERT IGNORE
      --json                                      log output format in JSON, need log level is debug mode
      --key string                                The path name to the key file
  -K, --kill-long-queries                         Kill long running queries (instead of aborting)
      --less-locking                              This option is deprecated and its behaviour is the default which is useful if you don't have transaction tables. Use --trx-tables otherwise
      --lines-starting-by string                  Adds the string at the begining of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
      --lines-terminated-by string                Adds the string at the end of each row. When --load-data is used it is added to the LOAD DATA statement. Its affects INSERT INTO statements also when it is used.
      --load-data                                 Instead of creating INSERT INTO statements, it creates LOAD DATA statements and .dat files
      --lock-all-tables                           This option is deprecated use --sync-thread-lock-mode instead
  -L, --logfile string                            Log file name to use, by default stdout is used
  -l, --long-query-guard uint                     Set long query timer in seconds (default 60)
      --long-query-retries int                    Retry checking for long queries, default 0 (do not retry)
      --long-query-retry-interval int             Time to wait before retrying the long query check in seconds (default 60)
      --masquerade-filename                       Masquerades the filenames
      --max-threads-per-table uint                Maximum number of threads per table to use (default 4)
      --max-time-per-select int                   Maximum amount of seconds that a select should take. Default: 2 (default 2)
      --merge                                     Merge the metadata with previous backup and overwrite output directory without clearing (beware of leftower chunks)
      --no-backup-locks                           Do not use Percona backup locks
      --no-check-generated-fields                 Queries related to generated fields are not going to be executed. It will lead to restoration issues if you have generated columns
  -d, --no-data                                   Do not dump table data
  -k, --no-locks                                  This option is deprecated use --sync-thread-lock-mode instead
  -m, --no-schemas                                Do not dump table schemas with the data and triggers
  -W, --no-views                                  Do not dump VIEWs
  -O, --omit-from-file string                     File containing a list of database.table entries to skip, one per line (skips before applying regex option)
      --optimize-keys-engines string              List of engines that will be used to split the create table statement into multiple stages if possible. Default: InnoDB,ROCKSDB
      --order-by-primary                          Sort the data by Primary Key or Unique key if no primary key exists
  -o, --outputdir string                          Directory to output files to
      --partition-regex string                    Regex to filter by partition name.
  -p, --password string                           User password
      --pid-file string                           Pid file used by Daemon mode. (default "/tmp/mydumper.pid")
      --pmm-path string                           which default value will be /usr/local/percona/pmm2/collectors/textfile-collector/high-resolution
      --pmm-resolution string                     which default will be high
  -P, --port int                                  TCP/IP port to connect to (default 3306)
      --protocol string                           The protocol to use for connection (tcp, socket) (default "tcp")
      --replace                                   Dump rows with REPLACE
      --replica-data string                       Includes the replica information
      --routine-checksums                         Dump triggers, functions and routines checksums
  -R, --routines                                  Dump stored procedures and functions. By default, it do not dump stored procedures nor functions
  -r, --rows string                               Spliting tables into chunks of this many rows. It can be MIN:START_AT:MAX. MAX can be 0 which means that there is no limit. It will double the chunk size if query takes less than 1 second and half of the size if it is more than 2 seconds
      --rows-hard string                          This set the MIN and MAX limit when even if --rows is 0
      --schema-checksums                          Dump schema table and view creation checksums
      --server-version string                     Set the server version avoid automatic detection
      --set-names string                          Sets the names, use it at your own risk
      --skip-constraints                          Remove the constraints from the CREATE TABLE statement. By default, the statement is not modified
      --skip-ddl-locks                            Do not send DDL locks when possible
      --skip-definer                              Removes DEFINER from the CREATE statement. By default, statements are not modified
      --skip-indexes                              Remove the indexes from the CREATE TABLE statement. By default, the statement is not modified
      --skip-tz-utc                               Doesn't add SET TIMEZONE on the backup files
  -X, --snapshot-count int                        number of snapshots, default 2 (default 2)
  -I, --snapshot-interval int                     Interval between each dump snapshot (in minutes), requires --daemon,default 60 (default 60)
  -S, --socket string                             UNIX domain socket file to use for connection
      --source-control-command string             Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS (default "TRADITIONAL")
      --source-data string                        It will include the options in the metadata file, to allow myloader to establish replication
      --split-partitions                          Dump partitions into separate files. This options overrides the --rows option for partitioned tables.
      --ssl                                       Connect using SSL
      --ssl-mode string                           Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
  -s, --statement-size int                        Attempted size of INSERT statement in bytes (default 1000000)
      --statement-terminated-by string            This might never be used, unless you know what are you doing
      --success_on_1146                           This option is deprecated use --ignore_engines-Errors instead
      --sync-thread-lock-mode string              There are 4 modes that can be use to sync: SAFE_NO_LOCK, FTWRL, LOCK_ALL and GTID.
                                                  If you don't need a consistent backup, use: NO_LOCK. More info https://mydumper.github.io/mydumper/docs/html/locks.html.
                                                  Default: AUTO which uses the best option depending on the database vendor (default "AUTO")
      --table-engine-for-view-dependency string   Table engine to be used for the CREATE TABLE statement for temporary tables when using views (default "MEMORY")
  -T, --tables-list string                        Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2
  -t, --threads uint                              Number of threads to use, 0 means to use number of CPUs (default 4)
      --throttle string                           xpects a string like Threads_running=10. It will check the SHOW GLOBAL STATUS and if it is higher, it will increase the sleep time between SELECT.
                                                  If option is used without parameters it will use Threads_running and the amount of threads
  -z, --tidb-snapshot string                      Snapshot to use for TiDB
      --tls-version string                        Which protocols the server permits for encrypted connections
  -G, --triggers                                  Dump triggers. By default, it do not dump triggers
      --trx-consistency-only                      This option is deprecated use --trx-tables instead
      --trx-tables int                            The backup process changes, if we know that we are exporting transactional tables only (default 1)
      --tz-utc                                    SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.
  -U, --updated-since int                         Use Update_time to dump only tables updated in the last U days
      --use-defer                                 Use defer integer sharding until all non-integer PK tables processed (saves RSS for huge quantities of tables).
      --use-savepoints                            Use savepoints to reduce metadata locking issues, needs SUPER privilege
      --use-single-column                         It will ignore_engines if the table has multiple columns and use only the first column to split the table
  -u, --user string                               Username with the necessary privileges
  -v, --verbose uint                              Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, 4 = debug (default 2)
  -V, --version                                   Show the program version and exit
      --views-as-tables                           Export VIEWs as they were tables
      --where string                              Dump only selected records.
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
# ignore_engines-engines                =
# where                                 =
updated-since                           = 0
# partition-regex                       =
# omit-from-file                        =
# tables-list                           =
# tidb-snapshot                         =
# use-savepoints                        = FALSE
# no-backup-locks                       = FALSE
trx-tables                              = 1
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
# insert-ignore_engines                 = FALSE
# replace                               = FALSE
# complete-insert                       = FALSE
# hex-blob                              = FALSE
# skip-definer                          = FALSE
statement-size                          = 1000000
# tz-utc                                = FALSE
# skip-tz-utc                           = FALSE
# set-names                             =
# set-names-file                        =
chunk-filesize                          = 0
# exit-if-broken-table-found            = FALSE
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
# outputdir                             =
# clear                                 = FALSE
# dirty                                 = FALSE
# merge                                 = FALSE
# stream                                = FALSE
# logfile                               =
# disk-limits                           =
threads                                 = 4
# version                               = FALSE
verbose                                 = TRUE
# debug                                 = FALSE
# defaults-file                         =
# defaults-extra-file                   =
```