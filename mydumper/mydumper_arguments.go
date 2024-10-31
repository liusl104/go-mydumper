package mydumper

import (
	"strings"
)

func row_arguments_callback(o *OptionEntries) bool {
	if o.Chunks.RowsPerChunk != "" {
		o.global.split_integer_tables = parse_rows_per_chunk(o.Chunks.RowsPerChunk, &o.global.min_chunk_step_size, &o.global.starting_chunk_step_size, &o.global.max_chunk_step_size)
	}
	return true
}

func format_arguments_callback(o *OptionEntries) bool {
	if strings.ToUpper(o.Statement.OutputFormat) == INSERT_ARG {
		o.global.output_format = SQL_INSERT
		return true
	}
	if strings.ToUpper(o.Statement.OutputFormat) == LOAD_DATA_ARG {
		o.Statement.LoadData = true
		o.global.rows_file_extension = DAT
		o.global.output_format = LOAD_DATA
		return true
	}
	if strings.ToUpper(o.Statement.OutputFormat) == CSV_ARG {
		o.Statement.Csv = true
		o.global.rows_file_extension = DAT
		o.global.output_format = CSV
		return true
	}
	if strings.ToUpper(o.Statement.OutputFormat) == CLICKHOUSE_ARG {
		o.global.clickhouse = true
		o.global.rows_file_extension = DAT
		o.global.output_format = CLICKHOUSE
		return true
	}
	return false
}

func common_arguments_callback(o *OptionEntries) bool {
	if o.Common.SourceControlCommand != "" {
		if strings.ToUpper(o.Common.SourceControlCommand) == "TRADITIONAL" {
			o.global.source_control_command = TRADITIONAL
			return true
		}
		if strings.ToUpper(o.Common.SourceControlCommand) == "AWS" {
			o.global.source_control_command = AWS
			return true
		}
	}
	return false
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

func arguments_callback(o *OptionEntries) bool {
	o.global.sync_wait = -1
	o.global.insert_statement = INSERT
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
	return false
}

func stream_arguments_callback(o *OptionEntries) bool {
	if o.Stream.Stream {
		if strings.ToUpper(o.Stream.StreamOpt) == "TRADITIONAL" || o.Stream.StreamOpt == "" {
			return true
		}
		if strings.ToUpper(o.Stream.StreamOpt) == "NO_DELETE" {
			o.global.no_delete = true
			return true
		}
		if strings.ToUpper(o.Stream.StreamOpt) == "NO_STREAM_AND_NO_DELETE" {
			o.global.no_delete = true
			o.global.no_stream = true
			return true
		}
		if strings.ToUpper(o.Stream.StreamOpt) == "NO_STREAM" {
			o.global.no_stream = true
			return true
		}
	}
	return false
}

func connection_arguments_callback(o *OptionEntries) {
	if o.Connection.HidePassword != "" {
		var tempPasswd []byte = make([]byte, len(o.Connection.HidePassword))
		copy(tempPasswd, []byte(o.Connection.HidePassword))
		o.Connection.Password = string(tempPasswd)
	}
	if o.Connection.Port != 0 || o.Connection.Hostname != "" {
		o.Connection.Protocol = "tcp"
	}
	if o.Connection.SocketPath != "" || (o.Connection.Port == 0 && o.Connection.Hostname == "" && o.Connection.SocketPath == "") {
		o.Connection.Protocol = "socket"
	}

	if o.Connection.Protocol != "" {
		if strings.ToLower(o.Connection.Protocol) == "tcp" {
			o.Connection.Protocol = strings.ToLower(o.Connection.Protocol)

		}
		if strings.ToLower(o.Connection.Protocol) == "socket" {
			o.Connection.Protocol = strings.ToLower(o.Connection.Protocol)
		}
	}

}

func before_arguments_callback(o *OptionEntries) bool {
	o.global.rows_file_extension = SQL
	o.global.output_format = SQL_INSERT
	o.global.identifier_quote_character = BACKTICK
	o.global.identifier_quote_character_str = "`"
	o.global.source_control_command = TRADITIONAL
	o.global.print_connection_details = 1
	o.global.routine_type = []string{"FUNCTION", "PROCEDURE", "PACKAGE", "PACKAGE BODY"}
	o.global.split_integer_tables = true
	o.global.nroutines = 4
	o.global.num_exec_threads = 4
	o.global.sync_wait = -1
	o.global.insert_statement = INSERT
	o.global.filename_regex = "^[\\w\\-_ ]+$"
	o.global.product = SERVER_TYPE_UNKNOWN

	return true
}

func after_arguments_callback(o *OptionEntries) bool {
	if o.CommonOptionEntries.BufferSize == 0 {
		o.CommonOptionEntries.BufferSize = 200000
	}
	if o.Extra.CompressMethod == "" {
		o.Extra.CompressMethod = GZIP
	}
	if o.QueryRunning.LongqueryRetryInterval == 0 {
		o.QueryRunning.LongqueryRetryInterval = 60
	}
	if o.QueryRunning.Longquery == 0 {
		o.QueryRunning.Longquery = 60
	}
	// o.Daemon.SnapshotCount = 2
	// o.Daemon.SnapshotInterval = 60
	if o.Chunks.MaxThreadsPerTable == 0 {
		// o.Chunks.MaxThreadsPerTable = math.MaxUint
	}
	if o.Chunks.MaxRows == 0 {
		o.Chunks.MaxRows = 100000
	}
	if o.Statement.StatementSize == 0 {
		o.Statement.StatementSize = 1000000
	}

	if o.Connection.Port == 0 {
		o.Connection.Port = 3306
	}
	if o.Common.NumThreads == 0 {
		o.Common.NumThreads = g_get_num_processors()
	}
	if o.Common.Verbose == 0 {
		o.Common.Verbose = 2
	}
	if o.Statement.SetNamesStr == "" {
		o.Statement.SetNamesStr = BINARY
	}
	return true
}
