package myloader

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	log "github.com/sirupsen/logrus"
	"os"
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

type io_restore_result struct {
	restore *asyncQueue
	result  *asyncQueue
}
type db_connection struct {
	conn    *client.Conn
	err     error
	code    uint16
	warning uint16
}
type connection_data struct {
	thrconn          *db_connection
	current_database *database
	thread_id        uint64
	queue            *io_restore_result
	ready            *asyncQueue
	transaction      bool
	in_use           *sync.Mutex
}

func (o *OptionEntries) myloader_initialize_hash_of_session_variables() map[string]string {
	var set_session_hash = o.initialize_hash_of_session_variables()
	if !o.EnableBinlog {
		set_session_hash["SQL_LOG_BIN"] = "0"
	}
	if o.CommitCount > 1 {
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

func (o *OptionEntries) create_database(td *thread_data, database string) {
	var filename = fmt.Sprintf("%s-schema-create.sql%s", database, o.ExecPerThreadExtension)
	var filepath = fmt.Sprintf("%s/%s-schema-create.sql%s", o.global.directory, database, o.ExecPerThreadExtension)
	if g_file_test(filepath) {
		atomic.AddUint64(&o.global.detailed_errors.schema_errors, uint64(o.restore_data_from_file(td, filename, true, nil)))
	} else {
		var data *GString = g_string_new("CREATE DATABASE IF NOT EXISTS %s%s%s", o.global.identifier_quote_character, database, o.global.identifier_quote_character)
		if o.restore_data_in_gstring_extended(td, data, true, nil, m_critical, "Failed to create database: %s", database) != 0 {
			atomic.AddUint64(&o.global.detailed_errors.schema_errors, 1)
		}
		data = nil
	}
	return
}

func print_errors(o *OptionEntries) {
	log.Infof("Errors found:")
	log.Infof("- Tablespace: %d", o.global.detailed_errors.tablespace_errors)
	log.Infof("- Schema:     %d", o.global.detailed_errors.schema_errors)
	log.Infof("- Data:       %d", o.global.detailed_errors.data_errors)
	log.Infof("- View:       %d", o.global.detailed_errors.view_errors)
	log.Infof("- Sequence:   %d", o.global.detailed_errors.sequence_errors)
	log.Infof("- Index:      %d", o.global.detailed_errors.index_errors)
	log.Infof("- Trigger:    %d", o.global.detailed_errors.trigger_errors)
	log.Infof("- Constraint: %d", o.global.detailed_errors.constraints_errors)
	log.Infof("- Post:       %d", o.global.detailed_errors.post_errors)
	log.Infof("Retries: %d", o.global.detailed_errors.retries)
}

func StartLoad() {
	var err error
	context := newOptionEntries()
	context.global.conf = new(configuration)
	context.initialize_share_common()
	context.loadOptionContext()
	if context.DB == "" && context.SourceDb != "" {
		context.DB = context.SourceDb
	}

	if context.Debug {
		set_debug(context)
		err = context.set_verbose()
	} else {
		err = context.set_verbose()
	}
	if context.OverwriteUnsafe {
		context.OverwriteTables = true
	}
	context.check_num_threads()
	if context.NumThreads > context.MaxThreadsPerTable {
		log.Infof("Using %d loader threads (%d per table)", context.NumThreads, context.MaxThreadsPerTable)
	} else {
		log.Infof("Using %d loader threads", context.NumThreads)
	}

	context.initialize_common_options(MYLOADER)
	if context.ProgramVersion {
		print_version(MYLOADER)
		os.Exit(EXIT_SUCCESS)
	}
	hide_password(context)
	ask_password(context)
	context.initialize_set_names()
	context.global.load_data_list_mutex = g_mutex_new()
	context.global.load_data_list = make(map[string]*sync.Mutex)
	if context.PmmPath != "" {
		context.global.pmm = true
		if context.PmmResolution == "" {
			context.PmmResolution = "high"
		}
	} else if context.PmmPath != "" {
		context.global.pmm = true
		context.PmmPath = fmt.Sprintf("/usr/local/percona/pmm2/collectors/textfile-collector/%s-resolution", context.PmmResolution)
	}
	if context.global.pmm {
		// TODO
	}
	context.initialize_restore_job(context.PurgeModeStr)
	var current_dir = g_get_current_dir()
	if context.InputDirectory == "" {
		if context.Stream {
			var datetime = time.Now()
			var datetimestr string
			datetimestr = datetime.Format("20060102-150405")
			context.global.directory = fmt.Sprintf("%s/%s-%s", current_dir, DIRECTORY, datetimestr)
			create_backup_dir(context.global.directory)
		} else {
			if !context.Help {
				log.Fatalf("a directory needs to be specified, see --help")
			}
		}
	} else {
		if strings.HasPrefix(context.InputDirectory, "/") {
			context.global.directory = context.InputDirectory
		} else {
			context.global.directory = fmt.Sprintf("%s/%s", current_dir, context.InputDirectory)
		}
		if context.Stream {
			if !g_file_test(context.InputDirectory) {
				create_backup_dir(context.global.directory)

			} else {
				if !context.global.no_stream {
					log.Fatalf("Backup directory (-d) must not exist when --stream / --stream=TRADITIONAL")
				}
			}
		} else {
			if !g_file_test(context.InputDirectory) {
				log.Fatalf("the specified directory doesn't exists")
			}
			var p = fmt.Sprintf("%s/metadata", context.global.directory)
			if !g_file_test(p) {
				log.Fatalf("the specified directory %s is not a mydumper backup", context.global.directory)
			}
		}
	}
	/*if context.FifoDirectory != "" {
		if !strings.HasPrefix(context.FifoDirectory, "/") {
			var tmp_fifo_directory = context.FifoDirectory
			context.FifoDirectory = fmt.Sprintf("%s/%s", current_dir, tmp_fifo_directory)
		}
		create_fifo_dir(context.FifoDirectory)
	}*/
	if context.Help {
		pring_help(context)
	}
	err = os.Chdir(context.global.directory)
	if context.TablesSkiplistFile != "" {
		err = context.read_tables_skiplist(context.TablesSkiplistFile)
	}
	context.initialize_process(context.global.conf)
	context.initialize_common()
	context.initialize_connection(MYLOADER)
	err = context.initialize_regex("")
	go context.signal_thread(context.global.conf)
	var conn *db_connection
	conn = context.m_connect()
	context.global.set_session = g_string_new("")
	context.global.set_global = g_string_new("")
	context.global.set_global_back = g_string_new("")
	err = context.detect_server_version(conn)
	context.global.detected_server = context.get_product()
	context.global.set_session_hash = context.myloader_initialize_hash_of_session_variables()
	context.global.set_global_hash = make(map[string]string)
	if context.global.key_file != nil {
		context.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(context.global.key_file, context, context.global.set_global_hash, "myloader_global_variables")
		context.global.set_global_hash = load_hash_of_all_variables_perproduct_from_key_file(context.global.key_file, context, context.global.set_global_hash, "myloader_session_variables")
	}
	refresh_set_session_from_hash(context.global.set_session, context.global.set_session_hash)
	refresh_set_global_from_hash(context.global.set_global, context.global.set_global_back, context.global.set_global_hash)
	execute_gstring(conn, context.global.set_session)
	execute_gstring(conn, context.global.set_global)
	initialize_conf_per_table(context.global.conf_per_table)
	load_per_table_info_from_key_file(context.global.key_file, context.global.conf_per_table, nil)
	// context.global.identifier_quote_character_str = context.IdentifierQuoteCharacter
	if context.DisableRedoLog {
		if context.get_major() == 8 && context.get_secondary() == 0 && context.get_revision() > 21 {
			log.Infof("Disabling redologs")
			m_query(conn, "ALTER INSTANCE DISABLE INNODB REDO_LOG", m_critical, "DISABLE INNODB REDO LOG failed")
		} else {
			log.Errorf("Disabling redologs is not supported for version %d.%d.%d", context.get_major(), context.get_secondary(), context.get_revision())
		}
	}
	context.global.conf.database_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.table_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.retry_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.data_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.post_table_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.post_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.index_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.view_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.ready = g_async_queue_new(context.BufferSize)
	context.global.conf.pause_resume = g_async_queue_new(context.BufferSize)
	context.global.conf.table_list_mutex = g_mutex_new()
	context.global.conf.stream_queue = g_async_queue_new(context.BufferSize)
	context.global.conf.table_hash = make(map[string]*db_table)
	context.global.conf.table_hash_mutex = g_mutex_new()
	if g_file_test("resume") {
		if !context.Resume {
			log.Fatalf("Resume file found but --resume has not been provided")
		}
	} else {
		if context.Resume {
			log.Fatalf("Resume file not found")
		}
	}
	context.initialize_connection_pool(conn)
	var t *thread_data = new(thread_data)
	initialize_thread_data(t, context.global.conf, WAITING, 0, nil)
	if context.global.database_db != nil {
		if !context.NoSchemas {
			create_database(t, context.global.database_db.real_database)
		}
		context.global.database_db.schema_state = CREATED
	}
	if context.TablesList != "" {
		context.global.tables = get_table_list(context.TablesList)
	}
	if context.SerialTblCreation {
		context.MaxThreadsForSchemaCreation = 1
	}
	context.initialize_worker_schema(context.global.conf)
	context.initialize_worker_index(context.global.conf)
	context.initialize_intermediate_queue(context.global.conf)
	if context.Stream {
		if context.Resume {
			log.Fatalf("We don't expect to find resume files in a stream scenario")
		}
		initialize_stream(context, context.global.conf)
	}
	context.initialize_loader_threads(context.global.conf)
	if context.Stream {
		wait_stream_to_finish(context)
	} else {
		context.process_directory(context.global.conf)
	}
	var dbt *db_table
	for _, dbt = range context.global.conf.table_list {
		if dbt.max_connections_per_job == 1 {
			dbt.max_connections_per_job = 0
		}
	}

	context.wait_schema_worker_to_finish()
	context.wait_loader_threads_to_finish()
	context.wait_control_job()
	context.create_index_shutdown_job(context.global.conf)
	context.wait_index_worker_to_finish()
	context.initialize_post_loding_threads(context.global.conf)
	context.create_post_shutdown_job(context.global.conf)
	context.wait_post_worker_to_finish()
	g_async_queue_unref(context.global.conf.ready)
	context.global.conf.ready = nil
	g_async_queue_unref(context.global.conf.data_queue)
	context.global.conf.data_queue = nil
	var cd *connection_data = context.close_restore_thread(true)
	cd.in_use.Lock()
	if context.DisableRedoLog {
		m_query(conn, "ALTER INSTANCE ENABLE INNODB REDO_LOG", m_critical, "ENABLE INNODB REDO LOG failed")
	}
	context.global.conf.data_queue.unref()
	for _, dbt = range context.global.conf.table_list {
		context.global.checksum_ok = context.checksum_dbt(dbt, conn)
	}

	if context.global.checksum_mode != CHECKSUM_SKIP {
		var d *database
		for _, d = range context.global.db_hash {
			if d.schema_checksum != "" && !context.NoSchemas {
				context.global.checksum_ok = context.checksum_database_template(d.real_database, d.schema_checksum, cd.thrconn, "Schema create checksum", checksum_database_defaults)
			}
			if d.post_checksum != "" && !context.SkipPost {
				context.global.checksum_ok = context.checksum_database_template(d.real_database, d.post_checksum, cd.thrconn, "Post checksum", checksum_process_structure)
			}
			if d.triggers_checksum != "" && !context.SkipTriggers {
				context.global.checksum_ok = context.checksum_database_template(d.real_database, d.triggers_checksum, cd.thrconn, "Triggers checksum", checksum_trigger_structure_from_database)
			}
		}
	}
	var i uint
	for i = 1; i < context.NumThreads; i++ {
		context.close_restore_thread(false)
	}
	context.wait_restore_threads_to_close()
	if !context.global.checksum_ok {
		if context.global.checksum_mode == CHECKSUM_WARN {
			log.Warnf("Checksum failed")
		} else {
			log.Errorf("Checksum failed")
		}
	}

	if context.Stream && context.global.no_delete == false {
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
				var str = g_string_new(line[i])
				g_string_append(str, ";")
				m_query(cd.thrconn, str.str, m_warning, fmt.Sprintf("Sending CHANGE MASTER: %s", str.str))
				g_string_free(str, true)
			}
		}
	}
	g_async_queue_unref(context.global.conf.database_queue)
	g_async_queue_unref(context.global.conf.table_queue)
	g_async_queue_unref(context.global.conf.retry_queue)
	g_async_queue_unref(context.global.conf.pause_resume)
	g_async_queue_unref(context.global.conf.post_table_queue)
	g_async_queue_unref(context.global.conf.post_queue)
	execute_gstring(conn, context.global.set_global_back)
	err = cd.thrconn.close()
	context.free_loader_threads()
	if context.global.pmm {
		kill_pmm_thread()
	}
	print_errors(context)
	stop_signal_thread(context)

	if context.Logger != nil {
		context.Logger.Close()
	}
	if context.global.errors > 0 {
		os.Exit(EXIT_FAILURE)
	} else {
		os.Exit(EXIT_SUCCESS)
	}
}

func create_database(td *thread_data, database string) {

}
