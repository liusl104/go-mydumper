package mydumper

import (
	"github.com/go-ini/ini"
	"github.com/spf13/pflag"
	"slices"
	"strconv"
	"strings"
)

const (
	BACKTICK          = "`"
	DOUBLE_QUOTE      = "\""
	TRADITIONAL  uint = 0
	AWS               = 1
)

var (
	Source_control_command         uint = TRADITIONAL
	Set_session                    *GString
	Set_global                     *GString
	Set_global_back                *GString
	sql_mode                       string
	main_connection                *DBConnection
	Key_file                       *ini.File
	Tables                         []string
	Set_names_statement            string
	Set_names_in_conn_by_default   string
	Identifier_quote_character     = BACKTICK
	Identifier_quote_character_str = "`"
	schema_sequence_fix            bool
	Detected_server                ServerType
	No_stream                      bool
	No_sync                        bool
	MaxThreadsPerTable             uint
	DB                             string
	NoSchemas                      bool
	NoData                         bool
	SetNamesStr                    string
	HidePassword                   string
	Sql_mode                       string
	Main_connection                *DBConnection
	Schema_sequence_fix            bool
	Throttle_variable              string
	Throttle_value                 int
	ReplicaDataStr                 string
)

var (
	SourceControlCommand string     // Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS
	Verbose              uint   = 2 // Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info,default 2
	Debug                bool       // (automatically sets verbosity to 3),print more info
	DefaultsFile         string     // Use a specific defaults file. Default: /etc/cnf
	DefaultsExtraFile    string     // Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values
	NumThreads           uint   = 4 // "Number of threads to use, default 4
	ProgramVersion       bool       // Show the program version and exit
	Regex                string     // Regular expression for 'db.table' matching
	TablesSkiplistFile   string     // File containing a list of database.table entries to skip, one per line (skips before applying regex option)
	TablesList           string
	Stream               string
	UseDefer             bool
	OptimizeKeyEngines   []string
	OptimizeKeysEngines  string
	FifoDirectory        string
	SourceDataStr        string
	SourceData           *Replication_settings = new(Replication_settings)
	ReplicaData          *Replication_settings = new(Replication_settings)
	Throttle             int
	ThrottleStr          string
	ServerVersionArg     string
)

type Replication_settings struct {
	Enabled                  bool
	Exec_start_replica       bool
	Exec_reset_replica       bool
	Exec_change_source       bool
	Auto_position            bool
	Source_ssl               bool
	Exec_start_replica_until bool
}

func Common_entries() {
	pflag.StringVar(&SourceDataStr, "source-data", "", "It will include the options in the metadata file, to allow myloader to establish replication")
	pflag.UintVarP(&NumThreads, "threads", "t", 4, "Number of threads to use, 0 means to use number of CPUs")
	pflag.BoolVarP(&ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.UintVarP(&Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, 4 = debug")
	pflag.BoolVar(&Debug, "debug", false, "(automatically sets verbosity to 4),Turn on debugging output,(automatically sets verbosity to 4)")
	pflag.StringVar(&IgnoreErrors, "ignore-errors", "", "Not increment error count and Warning instead of Critical in case of any of the comman separated error number list")
	pflag.BoolVar(&Json, "json", false, "log output format in JSON, need log level is debug mode")
	pflag.StringVar(&DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/mydumper.cnf")
	pflag.StringVar(&DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
	pflag.StringVar(&SourceControlCommand, "source-control-command", "TRADITIONAL", "Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS")
	pflag.StringVar(&OptimizeKeysEngines, "optimize-keys-engines", "", "List of engines that will be used to split the create table statement into multiple stages if possible. Default: InnoDB,ROCKSDB")
	pflag.StringVar(&ServerVersionArg, "server-version", "", "Set the server version avoid automatic detection")
	pflag.StringVar(&ThrottleStr, "throttle", "", "xpects a string like Threads_running=10. It will check the SHOW GLOBAL STATUS and if it is higher, it will increase the sleep time between SELECT. \nIf option is used without parameters it will use Threads_running and the amount of threads")
}

func Common_filter_entries() {
	pflag.StringVarP(&TablesSkiplistFile, "omit-from-file", "O", "", "File containing a list of database.table entries to skip, one per line (skips before applying regex option)")
	pflag.StringVarP(&TablesList, "tables-list", "T", "", "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2")
}

func parse_source_replica_options(value string, rep_set *Replication_settings) {
	rep_set.Enabled = true
	if value != "" {
		var lp = strings.Split(value, ",")
		if len(lp) == 1 {
			var _source_data uint64
			var err error
			_source_data, err = strconv.ParseUint(lp[0], 10, 64)
			if err == nil {
				rep_set.Exec_reset_replica = _source_data&(1<<(0)) > 0
				rep_set.Exec_change_source = _source_data&(1<<(1)) > 0
				rep_set.Exec_start_replica = _source_data&(1<<(2)) > 0
				rep_set.Source_ssl = _source_data&(1<<(3)) > 0
				rep_set.Auto_position = _source_data&(1<<(4)) > 0
				rep_set.Exec_start_replica_until = _source_data&(1<<(5)) > 0
				return
			}
		}
		rep_set.Exec_reset_replica = slices.Contains(lp, "exec_reset_replica")
		rep_set.Exec_change_source = slices.Contains(lp, "exec_change_source")
		rep_set.Exec_start_replica = slices.Contains(lp, "exec_start_replica")
		rep_set.Source_ssl = slices.Contains(lp, "source_ssl")
		rep_set.Auto_position = slices.Contains(lp, "auto_position")
		rep_set.Exec_start_replica_until = slices.Contains(lp, "exec_start_replica_until")
	}
}
func Common_arguments_callback() bool {
	if ThrottleStr != "" {
		var tp []string
		var tq = strings.SplitN(ThrottleStr, ":", 2)
		if len(tq[1]) > 0 {
			throttle_max_usleep_limit, _ = strconv.Atoi(tq[0])
			tp = strings.SplitN(tq[1], "=", 2)
		} else {
			tp = strings.SplitN(ThrottleStr, "=", 2)
		}
		Throttle_variable = tp[0]
		Throttle_value, _ = strconv.Atoi(tp[1])
	} else {
		Throttle_variable = "Threads_running"
		Throttle_value = 0
	}
	if OptimizeKeysEngines != "" {
		OptimizeKeyEngines = strings.Split(OptimizeKeysEngines, ",")
	}
	if SourceControlCommand != "" {
		if strings.ToUpper(SourceControlCommand) == "TRADITIONAL" {
			Source_control_command = TRADITIONAL
		}
		if strings.ToUpper(SourceControlCommand) == "AWS" {
			Source_control_command = AWS
		}
	}
	if IgnoreErrors != "" {
		var tmp_ignore_errors_list = strings.Split(IgnoreErrors, ",")
		for _, errCode := range tmp_ignore_errors_list {
			code, _ := strconv.Atoi(strings.TrimSpace(errCode))
			IgnoreErrorsList = append(IgnoreErrorsList, uint16(code))
		}
	}
	if SourceDataStr != "" {
		parse_source_replica_options(SourceDataStr, SourceData)
	}
	if ReplicaDataStr != "" {
		parse_source_replica_options(ReplicaDataStr, ReplicaData)
	}
	return true
}
