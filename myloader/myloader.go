package myloader

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	G_TIME_SPAN_SECOND = 1000000
	G_TIME_SPAN_MINUTE = 60000000
	G_TIME_SPAN_HOUR   = 3600000000
	G_TIME_SPAN_DAY    = 86400000000
	DIRECTORY          = "import"
)

func myloader_initialize_hash_of_session_variables(o *OptionEntries) map[string]string {
	var set_session_hash = o.initialize_hash_of_session_variables()
	if !o.Execution.EnableBinlog {
		set_session_hash["SQL_LOG_BIN"] = "0"
	}
	if o.Statement.CommitCount > 1 {
		set_session_hash["AUTOCOMMIT"] = "0"
	}
	return set_session_hash
}

func print_time(timespan time.Time) string {
	var now_time = time.Now().UnixMicro()
	var days = (now_time - timespan.UnixMicro()) / G_TIME_SPAN_DAY
	var hours = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY)) / G_TIME_SPAN_HOUR
	var minutes = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR)) / G_TIME_SPAN_MINUTE
	var seconds = ((now_time - timespan.UnixMicro()) - (days * G_TIME_SPAN_DAY) - (hours * G_TIME_SPAN_HOUR) - (minutes * G_TIME_SPAN_MINUTE)) / G_TIME_SPAN_SECOND
	return fmt.Sprintf("%02d:%02d:%02d:%02d", days, hours, minutes, seconds)
}

func compare_by_time(a *db_table, b *db_table) bool {
	return a.finish_time.Sub(a.start_data_time).Microseconds() > b.finish_time.Sub(b.start_data_time).Microseconds()
}

func show_dbt(key any, dbt any, total any) {
	_ = key
	_ = dbt
	_ = total
	log.Infof("Table %s", key.(string))
}

func create_database(o *OptionEntries, td *thread_data, database string) {
	var query string
	var filename = fmt.Sprintf("%s-schema-create.sql", database)
	var filenamegz = fmt.Sprintf("%s-schema-create.sql%s", database, o.Threads.ExecPerThreadExtension)
	var filepath = fmt.Sprintf("%s/%s-schema-create.sql", o.global.directory, database)
	var filepathgz = fmt.Sprintf("%s/%s-schema-create.sql%s", o.global.directory, database, o.Threads.ExecPerThreadExtension)
	if g_file_test(filepath) {
		atomic.AddUint64(&o.global.detailed_errors.schema_errors, uint64(restore_data_from_file(o, td, database, "", filename, true)))
	} else if g_file_test(filepathgz) {
		atomic.AddUint64(&o.global.detailed_errors.schema_errors, uint64(restore_data_from_file(o, td, database, "", filenamegz, true)))
	} else {
		query = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
		if !m_query(td.thrconn, query, m_warning, fmt.Sprintf("Fail to create database: %s", database)) {
			atomic.AddUint64(&o.global.detailed_errors.schema_errors, 1)
		}
	}
	return
}

func print_errors(o *OptionEntries) {
	log.Infof("Errors found:")
	log.Infof("- Tablespace:\t%d", o.global.detailed_errors.tablespace_errors)
	log.Infof("- Schema:    \t%d", o.global.detailed_errors.schema_errors)
	log.Infof("- Data:      \t%d", o.global.detailed_errors.data_errors)
	log.Infof("- View:      \t%d", o.global.detailed_errors.view_errors)
	log.Infof("- Sequence:  \t%d", o.global.detailed_errors.sequence_errors)
	log.Infof("- Index:     \t%d", o.global.detailed_errors.index_errors)
	log.Infof("- Trigger:   \t%d", o.global.detailed_errors.trigger_errors)
	log.Infof("- Post:      \t%d", o.global.detailed_errors.post_errors)
	log.Infof("Retries:\t%d", o.global.detailed_errors.retries)
}

func StartLoad() {
	var conf = new(configuration)
	var err error
	context := newOptionEntries()
	loadOptionContext(context)
	if context.Common.DB == "" && context.Filter.SourceDb != "" {
		context.Common.DB = context.Filter.SourceDb
	}
	if context.Common.Help {
		pring_help()
	}
	if context.Common.Debug {
		set_debug(context)
		err = context.set_verbose()
	} else {
		err = context.set_verbose()
	}
	initialize_common_options(context, MYLOADER)
	hide_password(context)
	ask_password(context)
	if context.Common.ProgramVersion {
		print_version(MYLOADER)
		os.Exit(EXIT_SUCCESS)
	}
	initialize_set_names(context)
	context.global.load_data_list_mutex = g_mutex_new()
	context.global.load_data_list = make(map[string]*sync.Mutex)
	if context.Pmm.PmmPath != "" {
		context.global.pmm = true
		if context.Pmm.PmmResolution == "" {
			context.Pmm.PmmResolution = "high"
		}
	} else if context.Pmm.PmmPath != "" {
		context.global.pmm = true
		context.Pmm.PmmPath = fmt.Sprintf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution", context.Pmm.PmmResolution)
	}
	if context.global.pmm {
		// TODO
	}
	initialize_restore_job(context, context.Execution.PurgeModeStr)
	var current_dir = g_get_current_dir()
	if context.Common.InputDirectory == "" {
		if context.Execution.Stream {
			var datetime = time.Now()
			var datetimestr string
			datetimestr = datetime.Format("20060102-150405")
			context.global.directory = fmt.Sprintf("%s/%s-%s", current_dir, DIRECTORY, datetimestr)
			create_backup_dir(context.global.directory, context.Common.FifoDirectory)
		} else {
			log.Fatalf("a directory needs to be specified, see --help")
		}
	} else {
		if strings.HasPrefix(context.Common.InputDirectory, "/") {
			context.global.directory = context.Common.InputDirectory
		} else {
			context.global.directory = fmt.Sprintf("%s/%s", current_dir, context.Common.InputDirectory)
		}
		if !g_file_test(context.Common.InputDirectory) {
			if context.Execution.Stream {
				create_backup_dir(context.global.directory, context.Common.FifoDirectory)
			} else {
				log.Fatalf("the specified directory doesn't exists")
			}
		}
		if !context.Execution.Stream {
			var p = fmt.Sprintf("%s/metadata", context.global.directory)
			if !g_file_test(p) {
				log.Fatalf("the specified directory %s is not a mydumper backup", context.global.directory)
			}
		}
	}
	if context.Common.FifoDirectory != "" {
		if !strings.HasPrefix(context.Common.FifoDirectory, "/") {
			var tmp_fifo_directory = context.Common.FifoDirectory
			context.Common.FifoDirectory = fmt.Sprintf("%s/%s", current_dir, tmp_fifo_directory)
		}
		create_fifo_dir(context.Common.FifoDirectory)
	}
	err = os.Chdir(context.global.directory)
	if context.Filter.TablesSkiplistFile != "" {
		err = read_tables_skiplist(context, context.Filter.TablesSkiplistFile)
	}
	initialize_process(context, conf)
	initialize_common(context)
	initialize_connection(context, MYLOADER)
	err = initialize_regex(context, "")
	go signal_thread(context, conf)
	var conn *client.Conn
	conn, err = m_connect(context)
	context.global.set_session = ""
	context.global.set_global = ""
	context.global.set_global_back = ""
	err = detect_server_version(context, conn)
	context.global.detected_server = context.get_product()
	context.global.set_session_hash = myloader_initialize_hash_of_session_variables(context)
	context.global.set_global_hash = make(map[string]string)
	if context.global.key_file != nil {
		context.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(context.global.key_file, context, context.global.set_global_hash, "myloader_global_variables")
		context.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(context.global.key_file, context, context.global.set_global_hash, "myloader_session_variables")
	}
	context.global.set_session = refresh_set_session_from_hash(context.global.set_session, context.global.set_session_hash)
	refresh_set_global_from_hash(&context.global.set_global, &context.global.set_global_back, context.global.set_global_hash)
	execute_gstring(conn, context.global.set_session)
	execute_gstring(conn, context.global.set_global)
	context.global.identifier_quote_character_str = context.Common.IdentifierQuoteCharacter
	if context.Execution.DisableRedoLog {
		if context.get_major() == 8 && context.get_secondary() == 0 && context.get_revision() > 21 {
			log.Infof("Disabling redologs")
			m_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", m_critical, "DISABLE INNODB REDO LOG failed")
		} else {
			log.Errorf("Disabling redologs is not supported for version %d.%d.%d", context.get_major(), context.get_secondary(), context.get_revision())
		}
	}
	conf.database_queue = g_async_queue_new(context.Common.BufferSize)
	conf.table_queue = g_async_queue_new(context.Common.BufferSize)
	conf.data_queue = g_async_queue_new(context.Common.BufferSize)
	conf.post_table_queue = g_async_queue_new(context.Common.BufferSize)
	conf.post_queue = g_async_queue_new(context.Common.BufferSize)
	conf.index_queue = g_async_queue_new(context.Common.BufferSize)
	conf.view_queue = g_async_queue_new(context.Common.BufferSize)
	conf.ready = g_async_queue_new(context.Common.BufferSize)
	conf.pause_resume = g_async_queue_new(context.Common.BufferSize)
	conf.table_list_mutex = g_mutex_new()
	conf.stream_queue = g_async_queue_new(context.Common.BufferSize)
	conf.table_hash = make(map[string]*db_table)
	conf.table_hash_mutex = g_mutex_new()
	context.global.db_hash = make(map[string]*database)
	if g_file_test("resume") {
		if !context.Common.Resume {
			log.Fatalf("Resume file found but --resume has not been provided")
		}
	} else {
		if context.Common.Resume {
			log.Fatalf("Resume file not found")
		}
	}
	var t *thread_data = new(thread_data)
	t.thread_id = 0
	t.conf = conf
	t.thrconn = conn
	t.current_database = ""
	t.status = WAITING
	if context.Filter.TablesList != "" {
		context.global.tables = get_table_list(context, context.Filter.TablesList)
	}
	if context.Common.DB != "" {
		var d = get_db_hash(context, context.Common.DB, context.Common.DB)
		create_database(context, t, context.Common.DB)
		d.schema_state = CREATED
	}

	if context.Execution.SerialTblCreation {
		context.Threads.MaxThreadsForSchemaCreation = 1
	}

	initialize_worker_schema(context, conf)
	initialize_worker_index(context, conf)
	initialize_intermediate_queue(context, conf)
	if context.Execution.Stream {
		wait_stream_to_finish(context)
	} else {
		process_directory(context, conf)
	}
	wait_schema_worker_to_finish(context)
	wait_loader_threads_to_finish(context)
	create_index_shutdown_job(context, conf)
	wait_index_worker_to_finish(context)
	initialize_post_loding_threads(context, conf)
	create_post_shutdown_job(context, conf)
	wait_post_worker_to_finish(context)
	conf.ready.unref()
	if context.Execution.DisableRedoLog {
		m_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", m_critical, "ENABLE INNODB REDO LOG failed")
	}
	conf.data_queue.unref()
	var tl = conf.table_list
	for _, dbt := range tl {
		checksum_dbt(dbt, conn)
	}
	var d *database
	for _, d = range context.global.db_hash {
		if d.schema_checksum != "" {
			checksum_database_template(d.name, d.schema_checksum, conn, "Schema create checksum", checksum_database_defaults)
		}
		if d.post_checksum != "" {
			checksum_database_template(d.name, d.post_checksum, conn, "Post checksum", checksum_process_structure)
		}
		if d.triggers_checksum != "" {
			checksum_database_template(d.name, d.triggers_checksum, conn, "Triggers checksum", checksum_trigger_structure_from_database)
		}
	}
	if context.Execution.Stream && context.global.no_delete == false && context.Common.InputDirectory == "" {
		err = os.Remove(path.Join(context.global.directory, "metadata"))
		err = os.RemoveAll(context.global.directory)
		if err != nil {
			log.Fatalf("Restore directory not removed: %s", context.global.directory)
		}
	}
	if context.global.change_master_statement != "" {
		var i int
		var line = strings.Split(context.global.change_master_statement, ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				var str = line[i]
				str += ";"
				m_query(conn, str, m_warning, fmt.Sprintf("Sending CHANGE MASTER: %s", str))
			}
		}
	}
	conf.database_queue.unref()
	conf.table_queue.unref()
	conf.pause_resume.unref()
	conf.post_table_queue.unref()
	conf.post_queue.unref()
	execute_gstring(conn, context.global.set_global_back)
	err = conn.Close()
	free_loader_threads(context)
	if context.global.pmm {
		kill_pmm_thread()
	}
	print_errors(context)
	stop_signal_thread(context)
	if context.Common.LogFile != "" {
		err = context.global.log_output.Close()
	}
}
