package myloader

import (
	"bufio"
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_DELIMITER            string = ";\n"
	DEFAULT_MAX_TRANSACTION_SIZE uint64 = 1000
)

var (
	connection_pool              *GAsyncQueue
	restore_queues               *GAsyncQueue
	free_results_queue           *GAsyncQueue
	release_connection_statement *statement         = &statement{kind_of_statement: CLOSE}
	end_restore_thread           *io_restore_result = new(io_restore_result)
	restore_threads              []*GThread
	control_job_ended            bool
	restore_data_from_file       func(*thread_data, string, bool, *database) int
)

type kind_of_statement int

const (
	NOT_DEFINED kind_of_statement = iota
	INSERT
	OTHER
	CLOSE
)

type statement struct {
	result            int
	preline           uint
	buffer            *GString
	filename          string
	kind_of_statement kind_of_statement
	is_schema         bool
	err               string
	error_number      uint
	dbt               *db_table
	td                *thread_data
}

func initialize_restore() {
	load_data_list_mutex = G_mutex_new()
	load_data_list = make(map[string]*sync.Mutex)
}
func new_connection_data(thrconn *DBConnection) *connection_data {
	var cd *connection_data = new(connection_data)
	if thrconn != nil {
		cd.thrconn = thrconn
	} else {
		cd.thrconn = Mysql_init()
		M_connect(cd.thrconn)
	}
	cd.current_database = nil
	cd.thread_id = Mysql_thread_id(cd.thrconn)
	cd.ready = G_async_queue_new(BufferSize)
	cd.queue = nil
	cd.in_use = G_mutex_new()
	log.Infof("Executing set session")
	Execute_gstring(cd.thrconn, Set_session)
	G_async_queue_push(connection_pool, cd)
	return cd
}

func new_io_restore_result() *io_restore_result {
	var iors *io_restore_result = new(io_restore_result)
	iors.result = G_async_queue_new(BufferSize)
	iors.restore = G_async_queue_new(BufferSize)
	return iors
}
func initialize_connection_pool(thrconn *DBConnection) {
	if MySQLDump {
		restore_data_from_file = restore_data_from_mysqldump_file
	} else {
		restore_data_from_file = restore_data_from_mydumper_file
	}
	var n uint
	connection_pool = G_async_queue_new(BufferSize)
	restore_queues = G_async_queue_new(BufferSize)
	free_results_queue = G_async_queue_new(BufferSize)
	var iors *io_restore_result
	restore_threads = make([]*GThread, NumThreads)
	for n = 0; n < NumThreads; n++ {
		iors = new_io_restore_result()
		G_async_queue_push(restore_queues, iors)
	}
	for n = 0; n < 8*NumThreads; n++ {
		G_async_queue_push(free_results_queue, new_statement())
	}
}

func start_connection_pool() {
	var n uint
	for n = 0; n < NumThreads; n++ {
		restore_threads[n] = M_thread_new("myloader_conn", restore_thread, nil, "Restore thread could not be created")
	}
}
func wait_restore_threads_to_close() {
	var n uint
	var cd *connection_data
	for n = 0; n < NumThreads; n++ {
		cd = G_async_queue_pop(connection_pool).(*connection_data)
		G_async_queue_push(cd.ready, end_restore_thread)
	}
	for n = 0; n < NumThreads; n++ {
		G_thread_join(restore_threads[n])
	}
}

func reconnect_connection_data(cd *connection_data) {
	cd.thrconn.Close()
	cd.thrconn = Mysql_init()
	M_connect(cd.thrconn)
	cd.connection_id = int(Mysql_thread_id(cd.thrconn))
	execute_use(cd)
	Execute_gstring(cd.thrconn, Set_session)
}

func restore_data_in_gstring_by_statement(cd *connection_data, data *GString, is_schema bool, query_counter *uint) uint {
	en := Mysql_real_query(cd.thrconn, data.Str.String())
	if en != nil {
		if is_schema {
			log.Warnf("Thread %d using connection %d - ERROR %d: %s\n%s", cd.thread_id, cd.connection_id, Mysql_errno(cd.thrconn), Mysql_error(cd.thrconn), data.Str.String())
		} else {
			log.Warnf("Thread %d using connection %d - ERROR %d: %s", cd.thread_id, cd.connection_id, Mysql_errno(cd.thrconn), Mysql_error(cd.thrconn))
		}
		if Mysql_errno(cd.thrconn) != 0 && !slices.Contains(IgnoreErrorsList, Mysql_errno(cd.thrconn)) {
			if err := cd.thrconn.Conn.Ping(); err != nil {
				reconnect_connection_data(cd)
				if !is_schema && CommitCount > 1 {
					log.Criticalf("Thread %d using connection %d - ERROR %d: Lost connection error. %s", cd.thread_id, cd.connection_id, Mysql_errno(cd.thrconn), Mysql_error(cd.thrconn))
					errors++
					return 2
				}
			}
			atomic.AddUint64(&detailed_errors.retries, 1)
			if Mysql_real_query(cd.thrconn, data.Str.String()) != nil {
				if is_schema {
					log.Criticalf("Thread %d using connection %d - ERROR %d: %s\n%s", cd.thread_id, cd.connection_id, Mysql_errno(cd.thrconn), Mysql_error(cd.thrconn), data.Str.String())
				} else {
					log.Criticalf("Thread %d using connection %d - ERROR %d: %s", cd.thread_id, cd.connection_id, Mysql_errno(cd.thrconn), Mysql_error(cd.thrconn))
				}
				errors++
				return 1
			}
		}
	}
	*query_counter = *query_counter + 1
	data = nil
	return 0
}

func close_restore_thread(return_connection bool) *connection_data {
	var cd *connection_data = G_async_queue_pop(connection_pool).(*connection_data)
	G_async_queue_push(cd.ready, &end_restore_thread)
	if return_connection {
		return cd
	}
	return nil
}

func setup_connection(cd *connection_data, td *thread_data, io_restore_result *io_restore_result, start_transaction bool, use_database *database, header *GString) {
	log.Tracef("Thread %d: Connection %d granted", td.thread_id, cd.thread_id)
	if err := cd.thrconn.Ping(); err != nil {
		log.Warnf("Thread %d: Connection %d failed", td.thread_id, cd.thread_id)
		reconnect_connection_data(cd)
		log.Warnf("Thread %d: New connection %d established", td.thread_id, cd.thread_id)
	}
	cd.thread_id = uint64(td.thread_id)
	cd.transaction = start_transaction
	if use_database != nil {
		execute_use_if_needs_to(cd, use_database, "request_another_connection")
	}
	if td != nil {
		td.granted_connections++
	}
	if cd.transaction {
		M_query_warning(cd.thrconn, "START TRANSACTION", "START TRANSACTION failed")
	}
	cd.queue = io_restore_result
	if header != nil {
		Execute_gstring(cd.thrconn, header)
	}
	G_async_queue_push(cd.ready, cd.queue)
}

func wait_for_available_restore_thread(td *thread_data, start_transaction bool, use_database *database) *connection_data {
	var cd *connection_data = G_async_queue_pop(connection_pool).(*connection_data)
	setup_connection(cd, td, G_async_queue_pop(restore_queues).(*io_restore_result), start_transaction, use_database, nil)
	return cd
}

func request_another_connection(td *thread_data, io_restore_result *io_restore_result, start_transaction bool, use_database *database, header *GString) bool {
	if control_job_ended && td.granted_connections < td.dbt.max_threads && td.dbt.restore_job_list.Len() == 0 {
		var cd *connection_data = G_async_queue_try_pop(connection_pool).(*connection_data)
		if cd != nil {
			setup_connection(cd, td, io_restore_result, start_transaction, use_database, header)
			return true
		}
	}
	return false
}

func m_commit(cd *connection_data) int {
	if M_query_warning(cd.thrconn, "COMMIT", "COMMIT failed") {
		return 2
	}
	return 0
}

func m_commit_and_start_transaction(cd *connection_data, query_counter *uint) uint {
	if e := m_commit(cd); e != 0 {
		return uint(e)
	}
	*query_counter = 0
	M_query_warning(cd.thrconn, "START TRANSACTION", "START TRANSACTION failed")
	return 0
}

func restore_insert(cd *connection_data, td *thread_data, data *GString, query_counter *uint, offset_line uint, dbt *db_table) int {
	var next_line int
	nextLineIndex := strings.Index(data.Str.String(), "VALUES") + 6
	var insert_statement_prefix string = data.Str.String()[:nextLineIndex]
	var r uint
	var tr uint
	var current_offset_line uint = offset_line - 1
	var current_line = nextLineIndex
	next_line = strings.Index(data.Str.String()[current_line:], "\n")
	var new_insert = G_string_sized_new(len(insert_statement_prefix))
	var current_rows uint64
	var transaction_size uint64
	for {
		current_rows = 0
		G_string_set_size(new_insert, 0)
		if dbt.rows > 0 {
			G_string_printf(new_insert, "/* Completed: %d */ ", dbt.rows_inserted*100/dbt.rows)
		} else {
			G_string_printf(new_insert, "/* Completed: %d */ ", 0)
		}
		G_string_append(new_insert, insert_statement_prefix)
		var line_len int = 0
		for {
			if next_line == -1 {
				// EOF
				break
			}
			var line = data.Str.String()[current_line:next_line]
			line_len = len(line)
			current_rows++
			current_line = next_line + 1
			next_line = strings.Index(data.Str.String()[current_line:], "\n")
			current_offset_line++
			if Rows == 0 || current_rows < uint64(Rows) {
				break
			}
		}
		if current_rows > 1 || (current_rows == 1 && line_len > 0) {
			if cd.transaction && (MaxTransactionSize*1024*1024 < uint64(new_insert.Len)+transaction_size) {
				tr += m_commit_and_start_transaction(cd, query_counter)
				transaction_size = 0
			}
			transaction_size += uint64(new_insert.Len)
			tr = restore_data_in_gstring_by_statement(cd, new_insert, false, query_counter)
			time.Sleep(time.Duration(Throttle_time) * time.Millisecond)
			dbt.mutex.Lock()
			dbt.rows_inserted += current_rows
			dbt.mutex.Unlock()
			if cd.transaction && *query_counter == CommitCount {
				tr += m_commit_and_start_transaction(cd, query_counter)
				transaction_size = 0
			}
			if tr > 0 {
				log.Errorf("Thread %d with connection %d: Error occurs between lines: %d and %d in a splited INSERT: %s", td.thread_id, cd.connection_id, offset_line, current_offset_line, Mysql_error(cd.thrconn))
			}
			if Mysql_warning_count(cd.thrconn) != 0 {
				log.Warnf("Connection %d: Warnings found during INSERT between lines: %d and %d: %s", cd.connection_id, offset_line, current_offset_line, show_warnings_if_possible(cd.thrconn))
				detailed_errors.data_warnings += uint64(Mysql_warning_count(cd.thrconn))
			}
		} else {
			tr = 0
		}
		r += tr
		offset_line = current_offset_line + 1
		current_line++
		// 检查是否处理完所有行
		if next_line == -1 {
			break
		}
	}
	return int(r)
}

func restore_thread(c any) {
	conn := c.(*DBConnection)
	var cd *connection_data = new_connection_data(conn)
	var ir *statement
	var query_counter uint
	for {
		cd.queue = G_async_queue_pop(cd.ready).(*io_restore_result)
		if cd.queue.restore == nil {
			break
		}
		for {
			ir = G_async_queue_pop(cd.queue.restore).(*statement)
			if ir.kind_of_statement == CLOSE {
				log.Tracef("Releasing connection: %d", cd.thread_id)
				if cd.transaction && query_counter > 0 {
					m_commit(cd)
				}
				G_async_queue_push(cd.queue.result, ir)
				cd.queue = nil
				ir = nil
				break
			}
			if ir.kind_of_statement == INSERT {
				ir.result = restore_insert(cd, ir.td, ir.buffer, &query_counter, ir.preline, ir.dbt)
				if ir.result > 0 {
					ir.err = Mysql_error(cd.thrconn)
					ir.error_number = uint(Mysql_errno(cd.thrconn))
					if max_errors != 0 && errors > max_errors {
						if ir.filename == "" {
							log.Criticalf("Error occurs processing statement: %v", Mysql_error(cd.thrconn))
						} else {
							log.Criticalf("Error occurs starting at line: %d on file %s: %v", ir.preline, ir.filename, Mysql_error(cd.thrconn))
						}
					} else {
						if ir.filename == "" {
							log.Criticalf("Error occurs processing statement: %s", Mysql_error(cd.thrconn))
						} else {
							log.Criticalf("Error occurs between line: %d on file %s: %s", ir.preline, ir.filename, Mysql_error(cd.thrconn))
						}
					}
				}
				G_async_queue_push(cd.queue.result, ir)
			} else {
				ir.result = int(restore_data_in_gstring_by_statement(cd, ir.buffer, ir.is_schema, &query_counter))
				if ir.result > 0 {
					ir.err = Mysql_error(cd.thrconn)
					ir.error_number = uint(Mysql_errno(cd.thrconn))
				}
				G_async_queue_push(cd.queue.result, ir)
			}
		}
		log.Tracef("Returning connection to pool: %d", cd.connection_id)
		G_async_queue_push(connection_pool, cd)
	}
	return
}

func load_data_mutex_locate(filename string, mutex **sync.Mutex) bool {
	load_data_list_mutex.Lock()

	var orig_key string
	if _, ok := load_data_list[filename]; !ok {
		*mutex = G_mutex_new()
		(*mutex).Lock()
		load_data_list[filename] = *mutex
		load_data_list_mutex.Unlock()
		return true
	}
	if orig_key != "" {
		// delete(load_data_list, orig_key)
	}
	load_data_list_mutex.Unlock()
	return false
}

func release_load_data_as_it_is_close(filename string) {
	load_data_list_mutex.Lock()
	var mutex = load_data_list[filename]
	if mutex == nil {
		load_data_list[filename] = nil
	} else {
		mutex.Unlock()
	}
	load_data_list_mutex.Unlock()
}

func free_statement(s *statement) {
	G_string_free(s.buffer, true)
	s.err = ""
	s = nil
}

func initialize_statement(ir *statement) *statement {
	ir.result = 0
	ir.error_number = 0
	ir.err = ""
	return ir
}

func new_statement() *statement {
	var stmt *statement = new(statement)
	initialize_statement(stmt)
	stmt.filename = ""
	stmt.buffer = G_string_sized_new(30)
	return stmt
}

func assing_statement(ir *statement, td *thread_data, dbt *db_table, stmt string, preline uint, is_schema bool, kind_of_statement kind_of_statement) {
	initialize_statement(ir)
	ir.buffer = G_string_new(stmt)
	ir.preline = preline
	ir.is_schema = is_schema
	ir.kind_of_statement = kind_of_statement
	ir.dbt = dbt
	ir.td = td
}

func process_result_vstatement_pop(get_insert_result_queue *GAsyncQueue, ir **statement, log_fun func(string, ...any), g_async_queue_pop_fun func(queue *GAsyncQueue) any, msg string, args ...any) int {
	*ir = g_async_queue_pop_fun(get_insert_result_queue).(*statement)
	if *ir == nil {
		return 0
	}
	if (*ir).kind_of_statement != CLOSE && (*ir).result > 0 {
		var c = fmt.Sprintf(msg, args...)
		log_fun("%s: %v (%d)", c, (*ir).err, (*ir).error_number)
	}
	return (*ir).result
}

func process_result_vstatement(get_insert_result_queue *GAsyncQueue, ir **statement, log_fun func(string, ...any), msg string, args ...any) int {
	return process_result_vstatement_pop(get_insert_result_queue, ir, log_fun, G_async_queue_pop, msg, args...)
}
func process_result_statement(get_insert_result_queue *GAsyncQueue, ir **statement, log_fun func(string, ...any), msg string, args ...any) int {
	return process_result_vstatement(get_insert_result_queue, ir, log_fun, msg, args...)
}

/*
	func restore_data_from_file(td *thread_data, filename string, is_schema bool, use_database *database) int {
		var infile *osFile
		var eof bool
		var data *GString = G_string_sized_new(256)
		var err error
		var line int
		var preline uint
		var file_path string = path.Join(directory, filename)
		infile, err = myl_open(file_path, os.O_RDONLY)
		if err != nil {
			log.Criticalf("cannot open file %s (%v)", filename, err)
			errors++
			return 1
		}
		var r int
		var load_data_filename, new_load_data_fifo_filename string
		var cd *connection_data = wait_for_available_restore_thread(td, !is_schema && (CommitCount > 1), use_database)
		G_assert(G_async_queue_length(cd.queue.restore) <= 0)
		G_assert(G_async_queue_length(cd.queue.result) <= 0)
		var i uint
		var ir *statement = G_async_queue_pop(free_results_queue).(*statement)
		var results_added bool
		var header *GString = G_string_new("")
		var inBufio *bufio.Scanner = bufio.NewScanner(infile.file)
		for eof == false {
			if Read_data(inBufio, data, &eof, &line) {
				var length int
				if data.Len >= 5 {
					length = data.Len - 5
				}
				if strings.Contains(data.Str.String()[length:], ";\n") {
					if SkipDefiner && strings.HasPrefix(data.Str.String(), "CREATE") {
						Remove_definer(data)
					}
					if strings.HasPrefix(data.Str.String(), "INSERT") {
						request_another_connection(td, cd.queue, cd.transaction, use_database, header)
						if !results_added {
							results_added = true
							var other_ir *statement
							for i = 0; i < 7; i++ {
								other_ir = G_async_queue_pop(free_results_queue).(*statement)
								G_async_queue_push(cd.queue.result, initialize_statement(other_ir))
							}
						}
						assing_statement(ir, data.Str.String(), preline, false, INSERT)
						G_async_queue_push(cd.queue.restore, ir)
						ir = nil
						process_result_vstatement(cd.queue.restore, &ir, M_critical, "(2)Error occurs processing file %s", filename)
					} else if strings.HasPrefix(data.Str.String(), "LOAD DATA ") {
						var new_data *GString
						var from = strings.Index(data.Str.String(), "'")
						from++
						var to = strings.Index(data.Str.String()[from:], "'")
						load_data_filename = data.Str.String()[from : from+to]
						var mutex *sync.Mutex = G_mutex_new()
						if load_data_mutex_locate(load_data_filename, &mutex) {
							mutex.Lock()
						}
						var command []string
						var is_fifo bool = get_command_and_basename(load_data_filename, &new_load_data_fifo_filename)
						_ = command
						_ = is_fifo
						if is_fifo {
							_ = new_data
						}
						assing_statement(ir, data.Str.String(), preline, false, OTHER)
						G_async_queue_push(cd.queue.restore, ir)
						ir = nil
						process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)

					} else {
						if strings.HasPrefix(data.Str.String(), "/*!") {
							var from_equal = data.Str.String()[3:strings.Index(data.Str.String(), "=")]
							if from_equal != "" && IgnoreSet != "" {
								from_equal = ""
								if !is_in_ignore_set_list(data.Str.String()) {
									from_equal = "="
									G_string_append(header, data.Str.String())
								} else {
									from_equal = "="
								}
							} else {
								G_string_append(header, data.Str.String())
							}
						} else {
							header = nil
						}
						assing_statement(ir, data.Str.String(), preline, false, OTHER)
						G_async_queue_push(cd.queue.restore, ir)
						ir = nil
						process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
					}
					r |= ir.result
					G_string_set_size(data, 0)
					preline = uint(line) + 1
					if ir.result > 0 {
						log.Criticalf("(1)Error occurs processing file %s", filename)
					}
				}
			} else {
				log.Criticalf("error reading file %s (%v)", filename, err)
				errors++
				return 1
			}
		}
		var queue *io_restore_result = cd.queue
		G_async_queue_push(free_results_queue, ir)
		if results_added {
			for i = 0; i < 7; i++ {
				process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
				G_assert(ir.kind_of_statement != CLOSE)
				G_async_queue_push(free_results_queue, ir)
			}
		}
		for ; td.granted_connections > 0; td.granted_connections-- {
			G_async_queue_push(queue.restore, &release_connection_statement)
			process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
			G_assert(ir.kind_of_statement == CLOSE)
		}
		G_async_queue_push(restore_queues, queue)
		myl_close(filename, infile, true)
		return r
	}
*/

func restore_data_from_mysqldump_file(td *thread_data, filename string, is_schema bool, use_database *database) int {
	var infile *osFile
	var eof bool
	var data *GString = G_string_sized_new(256)
	var line int
	var preline uint
	var path = path.Join(directory, filename)
	var err error
	infile, err = myl_open(path, os.O_RDONLY)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		errors++
		return 1
	}
	var r uint = 0
	var cd *connection_data = wait_for_available_restore_thread(td, !is_schema && (CommitCount > 1), use_database)
	G_assert(G_async_queue_length(cd.queue.restore) <= 0)
	G_assert(G_async_queue_length(cd.queue.result) <= 0)
	var i uint
	var ir *statement = G_async_queue_pop(free_results_queue).(*statement)
	var results_added bool
	var delimiter = DEFAULT_DELIMITER
	infile_buffer := bufio.NewScanner(infile.file)
	for eof == false {
		if Read_data(infile_buffer, data, &eof, &line) {
			if strings.HasPrefix(data.Str.String(), "DELIMITER") {
				delimiter = ""
				delimiter = data.Str.String()[10:]
				preline = uint(line) + 1
				G_string_set_size(data, 0)
			} else if strings.HasPrefix(data.Str.String(), delimiter) {
				if SkipDefiner && strings.HasPrefix(data.Str.String(), "CREATE") {
					Remove_definer(data)
				}
				assing_statement(ir, td, td.dbt, data.Str.String(), preline, is_schema, OTHER)
				G_async_queue_push(cd.queue.restore, ir)
				ir = nil
				process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
				r |= uint(ir.result)
				G_string_set_size(data, 0)
				preline = uint(line) + 1
				if ir.result > 0 {
					log.Criticalf("(1)Error occurs processing file %s", filename)
				}
			}
		} else {
			log.Criticalf("error reading file %s (%v)", filename, err)
			errors++
			return 1
		}
	}
	var queue *io_restore_result = cd.queue
	G_async_queue_push(free_results_queue, ir)
	if results_added {
		for i = 0; i < 7; i++ {
			process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
			G_assert(ir.kind_of_statement != CLOSE)
			G_async_queue_push(free_results_queue, ir)
		}
	}
	for ; td.granted_connections > 0; td.granted_connections-- {
		G_async_queue_push(queue.restore, &release_connection_statement)
		process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
		G_assert(ir.kind_of_statement == CLOSE)
	}
	G_async_queue_push(restore_queues, queue)
	G_string_free(data, true)
	myl_close(filename, infile, true)
	return int(r)
}

func restore_data_from_mydumper_file(td *thread_data, filename string, is_schema bool, use_database *database) int {
	var infile *osFile
	var eof bool
	var data *GString = G_string_sized_new(256)
	var line int
	var preline uint
	var path = path.Join(directory, filename)
	var err error
	infile, err = myl_open(path, os.O_RDONLY)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		errors++
		return 1
	}
	var r uint = 0
	var load_data_filename, load_data_fifo_filename, new_load_data_fifo_filename string
	var cd *connection_data = wait_for_available_restore_thread(td, !is_schema && (CommitCount > 1), use_database)
	G_assert(G_async_queue_length(cd.queue.restore) <= 0)
	G_assert(G_async_queue_length(cd.queue.result) <= 0)
	var i uint
	var ir *statement = G_async_queue_pop(free_results_queue).(*statement)
	var results_added bool
	var header *GString = G_string_sized_new(256)
	var inBufio *bufio.Scanner = bufio.NewScanner(infile.file)
	for eof == false {
		if Read_data(inBufio, data, &eof, &line) {
			if strings.HasSuffix(data.Str.String(), ";\n") {
				if SkipDefiner && strings.HasPrefix(data.Str.String(), "CREATE") {
					Remove_definer(data)
				}

				if strings.HasPrefix(data.Str.String(), "INSERT") {
					request_another_connection(td, cd.queue, cd.transaction, use_database, header)
					if !results_added {
						results_added = true
						var other_ir *statement
						for i = 0; i < 7; i++ {
							other_ir = G_async_queue_pop(free_results_queue).(*statement)
							G_async_queue_push(cd.queue.result, initialize_statement(other_ir))
						}
					}
					assing_statement(ir, td, td.dbt, data.Str.String(), preline, false, INSERT)
					G_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
				} else if strings.HasPrefix(data.Str.String(), "LOAD DATA ") {
					var new_data *GString
					var from = strings.Index(data.Str.String(), "'")
					from++
					var to = strings.Index(data.Str.String()[from:], "'")
					load_data_filename = data.Str.String()[from : to-from]
					var mutex *sync.Mutex = G_mutex_new()
					if load_data_mutex_locate(load_data_filename, &mutex) {
						mutex.Lock()
					}
					var command []string
					var is_fifo bool = get_command_and_basename(load_data_filename, &command, &new_load_data_fifo_filename)
					if is_fifo {
						if FifoDirectory != "" {
							new_data = G_string_new("")
							G_string_append(new_data, FifoDirectory)
							G_string_append_c(new_data, '/')
							G_string_append(new_data, data.Str.String()[from:])
							from = strings.Index(new_data.Str.String(), "'") + 1
							G_string_free(data, true)
							data = new_data
							to = strings.Index(new_data.Str.String()[from:], "'")
							var a int
							for ; a < len(load_data_filename)-len(load_data_fifo_filename); i++ {
								// replica the path
								to--
							}
							// to = '\''
							if FifoDirectory != "" {
								new_load_data_fifo_filename = fmt.Sprintf("%s/%s", FifoDirectory, new_load_data_fifo_filename)
								load_data_fifo_filename = new_load_data_fifo_filename
							}
							if err = os.MkdirAll(load_data_fifo_filename, 0666); err != nil {
								log.Criticalf("cannot create named pipe `%s': %v", load_data_fifo_filename, err)
							}
							execute_file_per_thread(load_data_filename, load_data_fifo_filename, command)
							release_load_data_as_it_is_close(load_data_fifo_filename)
						}
					}
					assing_statement(ir, td, td.dbt, data.Str.String(), preline, false, OTHER)
					G_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
					if is_fifo {
						M_remove("", load_data_fifo_filename)
					} else {
						M_remove("", load_data_filename)
					}
				} else {
					if strings.HasPrefix(data.Str.String(), "/*!") {
						var from_equal = strings.Index(data.Str.String(), "=")
						if from_equal != -1 && ignore_set_list != nil {
							var from = data.Str.String()[3:from_equal]
							if from != "" && !is_in_ignore_set_list(data.Str.String()) {
								G_string_append(header, data.Str.String())
								from = "="
							} else {
								from = "="
								goto STMT_IGNORED
							}
						} else {
							G_string_append(header, data.Str.String())
						}
					} else {
						header = nil
					}
					assing_statement(ir, td, td.dbt, data.Str.String(), preline, is_schema, OTHER)
					G_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_statement(cd.queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
				}
				r |= uint(ir.result)
				if ir.result > 0 {
					log.Criticalf("(1)Error occurs processing file %s", filename)
				}
			STMT_IGNORED:
				G_string_set_size(data, 0)
				preline = uint(line) + 1
			}
		} else {
			log.Criticalf("error reading file %s", filename)
			errors++
			return 1
		}
	}
	var queue *io_restore_result = cd.queue
	G_async_queue_push(free_results_queue, ir)
	if results_added {
		for i = 0; i < 7; i++ {
			process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
			G_assert(ir.kind_of_statement != CLOSE)
			G_async_queue_push(free_results_queue, ir)
		}
	}
	for ; td.granted_connections > 0; td.granted_connections-- {
		G_async_queue_push(queue.restore, &release_connection_statement)
		process_result_statement(queue.result, &ir, M_critical, "(2)Error occurs processing file %s", filename)
		G_assert(ir.kind_of_statement == CLOSE)
	}
	G_async_queue_push(restore_queues, queue)
	G_string_free(data, true)
	myl_close(filename, infile, true)
	return int(r)
}

func restore_data_in_gstring_extended(td *thread_data, data *GString, is_schema bool, use_database *database, log_fun func(string, ...any), msg string, args ...any) bool {
	var cd *connection_data = wait_for_available_restore_thread(td, !is_schema && (CommitCount > 1), use_database)
	var queue *io_restore_result = cd.queue
	var ir *statement = G_async_queue_pop(free_results_queue).(*statement)
	var i int
	var r int
	if data != nil && data.Len > 4 {
		var line []string = strings.Split(data.Str.String(), ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				assing_statement(ir, td, td.dbt, line[i], 0, is_schema, OTHER)
				if ir.err != "" {
					ir.err = ""
				}
				G_async_queue_push(queue.restore, ir)
				r += process_result_vstatement(queue.result, &ir, log_fun, msg, args...)
			}
		}
	}
	G_async_queue_push(free_results_queue, ir)
	G_async_queue_push(queue.restore, &release_connection_statement)
	td.granted_connections--
	r += process_result_vstatement(queue.result, &ir, log_fun, msg, args...)
	G_assert(G_async_queue_length(queue.restore) <= 0)
	G_assert(G_async_queue_length(queue.result) <= 0)
	G_async_queue_push(restore_queues, queue)
	return r != 0
}

func restore_data_in_gstring(td *thread_data, data *GString, is_schema bool, use_database *database) bool {
	return restore_data_in_gstring_extended(td, data, is_schema, use_database, M_warning, "Failed to execute statement")
}
