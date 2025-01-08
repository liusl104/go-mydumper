package myloader

import (
	"bufio"
	"fmt"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	connection_pool              *GAsyncQueue
	restore_queues               *GAsyncQueue
	free_results_queue           *GAsyncQueue
	ignore_errors                string
	release_connection_statement *statement         = &statement{kind_of_statement: CLOSE}
	end_restore_thread           *io_restore_result = new(io_restore_result)
	restore_threads              []*GThreadFunc
	control_job_ended            bool
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
	buffer            *strings.Builder
	filename          string
	kind_of_statement kind_of_statement
	is_schema         bool
	err               error
	error_number      uint
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
	var n uint
	if ignore_errors != "" {
		var tmp_ignore_errors_list = strings.Split(IgnoreErrors, ",")
		for tmp_ignore_errors_list[n] != "" {
			code, _ := strconv.Atoi(strings.TrimSpace(tmp_ignore_errors_list[n]))
			Ignore_errors_list = append(Ignore_errors_list, uint16(code))
			n++
		}
	}
	connection_pool = G_async_queue_new(BufferSize)
	restore_queues = G_async_queue_new(BufferSize)
	free_results_queue = G_async_queue_new(BufferSize)
	var iors *io_restore_result
	restore_threads = make([]*GThreadFunc, NumThreads)
	for n = 0; n < NumThreads; n++ {
		iors = new_io_restore_result()
		G_async_queue_push(restore_queues, iors)
		var thread = new(sync.WaitGroup)
		restore_threads[n] = G_thread_new("myloader_conn", thread, n)
		go restore_thread(thrconn, n)

	}
	for n = 0; n < 8*NumThreads; n++ {
		G_async_queue_push(free_results_queue, new_statement())
	}
}
func wait_restore_threads_to_close() {
	var n uint
	for n = 0; n < NumThreads; n++ {
		restore_threads[n].Thread.Wait()
	}
}

func reconnect_connection_data(cd *connection_data) {
	cd.thrconn.Close()
	cd.thrconn = Mysql_init()
	M_connect(cd.thrconn)
	cd.thread_id = Mysql_thread_id(cd.thrconn)
	execute_use(cd)
	Execute_gstring(cd.thrconn, Set_session)
}

func restore_data_in_gstring_by_statement(cd *connection_data, data string, is_schema bool, query_counter *uint) uint {
	en := cd.thrconn.Execute(data)
	if cd.thrconn.Err != nil {
		if is_schema {
			log.Warnf("Connection %d -  - ERROR %v\n%s", cd.thread_id, cd.thrconn.Err, data)
		} else {
			log.Warnf("Connection %d -  - ERROR %v", cd.thread_id, cd.thrconn.Err)
		}
		if cd.thrconn.Code != 0 && !slices.Contains(Ignore_errors_list, cd.thrconn.Code) {
			if err := cd.thrconn.Ping(); err != nil {
				reconnect_connection_data(cd)
				if !is_schema && CommitCount > 1 {
					log.Criticalf("Connection %d - ERROR %d: Lost connection error. %v", cd.thread_id, cd.thrconn.Code, cd.thrconn.Err)
					errors++
					return 2
				}
			}
			atomic.AddUint64(&detailed_errors.retries, 1)
			en = cd.thrconn.Execute(data)
			_ = en
			if cd.thrconn.Err != nil {
				if is_schema {
					log.Criticalf("Connection %d -  - ERROR %v\n%s", cd.thread_id, cd.thrconn.Err, data)
				} else {
					log.Criticalf("Connection %d -  - ERROR %v", cd.thread_id, cd.thrconn.Err)
				}
				errors++
				return 1
			}
		}
	}
	*query_counter = *query_counter + 1
	data = ""
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
	cd.transaction = start_transaction
	if use_database != nil {
		execute_use_if_needs_to(cd, use_database, "request_another_connection")
	}
	if td != nil {
		td.granted_connections++
	}
	if cd.transaction {
		m_query(cd.thrconn, "START TRANSACTION", M_warning, "START TRANSACTION failed")
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

func m_commit(cd *connection_data) uint {
	if !m_query(cd.thrconn, "COMMIT", M_warning, "COMMIT failed") {
		errors++
		return 2
	}
	return 0
}

func m_commit_and_start_transaction(cd *connection_data, query_counter *uint) uint {
	if e := m_commit(cd); e != 0 {
		return 1
	}
	*query_counter = 0
	m_query(cd.thrconn, "START TRANSACTION", M_warning, "START TRANSACTION failed")
	return 0
}

func restore_insert(cd *connection_data, data string, query_counter *uint, offset_line uint) int {
	var next_line string
	nextLineIndex := strings.Index(data, "VALUES") + 6
	var insert_statement_prefix string = data[nextLineIndex:]
	var r uint
	var tr uint
	var current_offset_line uint = offset_line - 1
	var current_line = data[nextLineIndex:]
	next_line = current_line[strings.Index(current_line, "\n"):]
	var new_insert *strings.Builder = new(strings.Builder)
	var current_rows uint
	for {
		if next_line == "" {
			break
		}
		current_rows = 0
		new_insert.Reset()
		new_insert.WriteString(insert_statement_prefix)
		var line_len int
		for {
			// TODO char *line=g_strndup(current_line, next_line - current_line);
			if (Rows == 0 || current_rows < uint(Rows)) && next_line != "" {
				break
			}
			var line = ""
			line_len = len(line)
			new_insert.WriteString(line)
			current_rows++
			// TODO current_line=next_line+1; next_line=g_strstr_len(current_line, -1, "\n");
			current_offset_line++
		}
		if current_rows > 1 || (current_rows == 1 && line_len > 0) {
			tr = restore_data_in_gstring_by_statement(cd, new_insert.String(), false, query_counter)
			if cd.transaction && *query_counter == CommitCount {
				tr += m_commit_and_start_transaction(cd, query_counter)
			}
			if tr > 0 {
				log.Criticalf("Connection %d: Error occurs between lines: %d and %d in a splited INSERT: %v", cd.thread_id, offset_line, current_offset_line, cd.thrconn.Err)
			}
			if cd.thrconn.Warning != 0 {
				log.Warnf("Connection %d: Warnings found during INSERT between lines: %d and %d: %s", cd.thread_id, offset_line, current_offset_line, Show_warnings_if_possible(cd.thrconn))
			}
		} else {
			tr = 0
		}
		r += tr
		offset_line = current_offset_line + 1
		current_line = current_line[1:]

	}
	return int(r)
}

func restore_thread(conn *DBConnection, thread_id uint) {
	defer restore_threads[thread_id].Thread.Done()
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
				ir.result = restore_insert(cd, ir.buffer.String(), &query_counter, ir.preline)
				if ir.result > 0 {
					ir.err = cd.thrconn.Err
					ir.error_number = uint(cd.thrconn.Code)
					if max_errors != 0 && errors > max_errors {
						if ir.filename == "" {
							log.Criticalf("Error occurs processing statement: %v", cd.thrconn.Err)
						} else {
							log.Criticalf("Error occurs starting at line: %d on file %s: %v", ir.preline, ir.filename, cd.thrconn.Err)
						}
					} else {
						if ir.filename == "" {
							log.Critical("Error occurs processing statement: %v", cd.thrconn.Err)
						} else {
							log.Criticalf("Error occurs between line: %d on file %s: %v", ir.preline, ir.filename, cd.thrconn.Err)
						}
					}
				}
				G_async_queue_push(cd.queue.result, ir)
			} else {
				ir.result = int(restore_data_in_gstring_by_statement(cd, ir.buffer.String(), ir.is_schema, &query_counter))
				if ir.result > 0 {
					ir.err = cd.thrconn.Err
					ir.error_number = uint(cd.thrconn.Code)
				}
				G_async_queue_push(cd.queue.result, ir)
			}
		}
		log.Tracef("Returning connection to pool: %d", cd.thread_id)
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
	} else {
		orig_key = filename
	}
	if orig_key != "" {
		delete(load_data_list, orig_key)
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
	s.buffer.Reset()
	s.err = nil
	s = nil
}

func initialize_statement(ir *statement) *statement {
	ir.result = 0
	ir.error_number = 0
	ir.err = nil
	return ir
}

func new_statement() *statement {
	var stmt *statement = new(statement)
	initialize_statement(stmt)
	stmt.filename = ""
	stmt.buffer = new(strings.Builder)
	return stmt
}

func assing_statement(ir *statement, stmt string, preline uint, is_schema bool, kind_of_statement kind_of_statement) {
	initialize_statement(ir)
	ir.buffer.Reset()
	ir.buffer.WriteString(stmt)
	ir.preline = preline
	ir.is_schema = is_schema
	ir.kind_of_statement = kind_of_statement
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

func restore_data_from_file(td *thread_data, filename string, is_schema bool, use_database *database) int {
	var infile *osFile
	var eof bool
	var data *GString
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
	_ = load_data_filename
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
			if data.Len > 5 {
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
					var new_data string
					var from = strings.Index(data.Str.String(), "'") + 1
					var to = strings.Index(data.Str.String()[from:], "'")
					load_data_filename = data.Str.String()[from : from+to]
					var mutex *sync.Mutex
					_ = mutex
					_ = new_data
					_ = new_load_data_fifo_filename
					// var command []string
					// TODO
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
				data.Str.Reset()
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

func restore_data_in_gstring_extended(td *thread_data, data *GString, is_schema bool, use_database *database, log_fun func(string, ...any), msg string, args ...any) int {
	var cd *connection_data = wait_for_available_restore_thread(td, !is_schema && (CommitCount > 1), use_database)
	var queue *io_restore_result = cd.queue
	var ir *statement = G_async_queue_pop(free_results_queue).(*statement)
	var i int
	var r int
	if data != nil && data.Len > 4 {
		var line []string = strings.Split(data.Str.String(), ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				assing_statement(ir, line[i], 0, is_schema, OTHER)
				if ir.err != nil {
					ir.err = nil
				}
				G_async_queue_push(queue.restore, ir)
				r += process_result_vstatement(queue.result, &ir, log_fun, msg, args)
			}
		}
	}
	G_async_queue_push(free_results_queue, ir)
	G_async_queue_push(queue.restore, &release_connection_statement)
	td.granted_connections--
	r += process_result_vstatement(queue.result, &ir, log_fun, msg, args)
	G_assert(G_async_queue_length(queue.restore) <= 0)
	G_assert(G_async_queue_length(queue.result) <= 0)
	G_async_queue_push(restore_queues, queue)
	return r
}

func restore_data_in_gstring(td *thread_data, data *GString, is_schema bool, use_database *database) int {
	return restore_data_in_gstring_extended(td, data, is_schema, use_database, M_warning, "Failed to execute statement", nil)
}
