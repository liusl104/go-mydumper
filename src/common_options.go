package mydumper

import (
	"github.com/go-ini/ini"
	"github.com/spf13/pflag"
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
	set_names_str                  string
	Set_names_statement            string
	Identifier_quote_character     = BACKTICK
	Identifier_quote_character_str = "`"
	schema_sequence_fix            bool
	Detected_server                ServerType
	No_stream                      bool
	MaxThreadsPerTable             uint
	DB                             string
	NoSchemas                      bool
	NoData                         bool
	SetNamesStr                    string
	HidePassword                   string
	Sql_mode                       string
	Main_connection                *DBConnection
	Schema_sequence_fix            bool
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
	FifoDirectory        string
)

func Common_entries() {
	pflag.UintVarP(&NumThreads, "threads", "t", 0, "Number of threads to use, 0 means to use number of CPUs. Default: 4")
	pflag.BoolVarP(&ProgramVersion, "version", "V", false, "Show the program version and exit")
	pflag.UintVarP(&Verbose, "verbose", "v", 2, "Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info")
	pflag.BoolVar(&Debug, "debug", false, "(automatically sets verbosity to 4),Print more info")
	pflag.StringVar(&DefaultsFile, "defaults-file", "", "Use a specific defaults file. Default: /etc/cnf")
	pflag.StringVar(&DefaultsExtraFile, "defaults-extra-file", "", "Use an additional defaults file. This is loaded after --defaults-file, replacing previous defined values")
	// pflag.StringVar(&FifoDirectory, "fifodir", "", "Directory where the FIFO files will be created when needed. Default: Same as backup")
	pflag.StringVar(&SourceControlCommand, "source-control-command", "", "Instruct the proper commands to execute depending where are configuring the replication. Options: TRADITIONAL, AWS")
}

func Common_filter_entries() {
	pflag.StringVarP(&TablesSkiplistFile, "omit-from-file", "O", "", "File containing a list of database.table entries to skip, one per line (skips before applying regex option)")
	pflag.StringVarP(&TablesList, "tables-list", "T", "", "Comma delimited table list to dump (does not exclude regex option). Table name must include database name. For instance: test.t1,test.t2")
}
func LoadOptionContext() {

	pflag.Parse()
}
