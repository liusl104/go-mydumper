package myloader

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type GThreadFunc struct {
	thread    *sync.WaitGroup
	name      string
	thread_id uint
}
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

func (o *OptionEntries) new_connection_data(thrconn *db_connection) *connection_data {
	var cd *connection_data = new(connection_data)
	if thrconn != nil {
		cd.thrconn = thrconn
	} else {
		cd.thrconn = o.m_connect()
	}
	cd.current_database = nil
	cd.thread_id = mysql_thread_id(cd.thrconn)
	cd.ready = g_async_queue_new(o.BufferSize)
	cd.queue = nil
	cd.in_use = g_mutex_new()
	execute_gstring(cd.thrconn, o.global.set_session)
	o.global.connection_pool.push(cd)
	return cd
}

func (o *OptionEntries) new_io_restore_result() *io_restore_result {
	var iors *io_restore_result = new(io_restore_result)
	iors.result = g_async_queue_new(o.BufferSize)
	iors.restore = g_async_queue_new(o.BufferSize)
	return iors
}

func (o *OptionEntries) initialize_connection_pool(thrconn *db_connection) {
	var n uint
	if o.global.ignore_errors != "" {
		var tmp_ignore_errors_list = strings.Split(o.IgnoreErrors, ",")
		for tmp_ignore_errors_list[n] != "" {
			code, _ := strconv.Atoi(strings.TrimSpace(tmp_ignore_errors_list[n]))
			o.global.ignore_errors_list = append(o.global.ignore_errors_list, uint16(code))
			n++
		}
	}
	o.global.connection_pool = g_async_queue_new(o.BufferSize)
	o.global.restore_queues = g_async_queue_new(o.BufferSize)
	o.global.free_results_queue = g_async_queue_new(o.BufferSize)
	var iors *io_restore_result
	o.global.restore_threads = make([]*GThreadFunc, o.NumThreads)
	for n = 0; n < o.NumThreads; n++ {
		iors = o.new_io_restore_result()
		o.global.restore_queues.push(iors)
		var thread = new(sync.WaitGroup)
		o.global.restore_threads[n] = g_thread_new("myloader_conn", thread, n)
		go o.restore_thread(thrconn, n)

	}
	for n = 0; n < 8*o.NumThreads; n++ {
		o.global.free_results_queue.push(o.new_statement())
	}
}
func (o *OptionEntries) wait_restore_threads_to_close() {
	var n uint
	for n = 0; n < o.NumThreads; n++ {
		o.global.restore_threads[n].thread.Wait()
	}
}

func (o *OptionEntries) reconnect_connection_data(cd *connection_data) {
	cd.thrconn.close()
	cd.thrconn = o.m_connect()
	cd.thread_id = mysql_thread_id(cd.thrconn)
	execute_use(cd)
	execute_gstring(cd.thrconn, o.global.set_session)
}

func (o *OptionEntries) restore_data_in_gstring_by_statement(cd *connection_data, data string, is_schema bool, query_counter *uint) uint {
	en := cd.thrconn.Execute(data)
	if cd.thrconn.err != nil {
		if is_schema {
			log.Warnf("Connection %d -  - ERROR %v\n%s", cd.thread_id, cd.thrconn.err, data)
		} else {
			log.Warnf("Connection %d -  - ERROR %v", cd.thread_id, cd.thrconn.err)
		}
		if cd.thrconn.code != 0 && !slices.Contains(o.global.ignore_errors_list, cd.thrconn.code) {
			if err := cd.thrconn.ping(); err != nil {
				o.reconnect_connection_data(cd)
				if !is_schema && o.CommitCount > 1 {
					log.Errorf("Connection %d - ERROR %d: Lost connection error. %v", cd.thread_id, cd.thrconn.code, cd.thrconn.err)
					o.global.errors++
					return 2
				}
			}
			atomic.AddUint64(&o.global.detailed_errors.retries, 1)
			en = cd.thrconn.Execute(data)
			_ = en
			if cd.thrconn.err != nil {
				if is_schema {
					log.Errorf("Connection %d -  - ERROR %v\n%s", cd.thread_id, cd.thrconn.err, data)
				} else {
					log.Errorf("Connection %d -  - ERROR %v", cd.thread_id, cd.thrconn.err)
				}
				o.global.errors++
				return 1
			}
		}
	}
	*query_counter = *query_counter + 1
	data = ""
	return 0
}

func (o *OptionEntries) close_restore_thread(return_connection bool) *connection_data {
	var cd *connection_data = o.global.connection_pool.pop().(*connection_data)
	cd.ready.push(&o.global.end_restore_thread)
	if return_connection {
		return cd
	}
	return nil
}

func (o *OptionEntries) setup_connection(cd *connection_data, td *thread_data, io_restore_result *io_restore_result, start_transaction bool, use_database *database, header *GString) {
	log.Tracef("Thread %d: Connection %d granted", td.thread_id, cd.thread_id)
	if err := cd.thrconn.ping(); err != nil {
		log.Warnf("Thread %d: Connection %d failed", td.thread_id, cd.thread_id)
		o.reconnect_connection_data(cd)
		log.Warnf("Thread %d: New connection %d established", td.thread_id, cd.thread_id)
	}
	cd.transaction = start_transaction
	if use_database != nil {
		o.execute_use_if_needs_to(cd, use_database, "request_another_connection")
	}
	if td != nil {
		td.granted_connections++
	}
	if cd.transaction {
		m_query(cd.thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed")
	}
	cd.queue = io_restore_result
	if header != nil {
		execute_gstring(cd.thrconn, header)
	}
	cd.ready.push(cd.queue)
}

func (o *OptionEntries) wait_for_available_restore_thread(td *thread_data, start_transaction bool, use_database *database) *connection_data {
	var cd *connection_data = o.global.connection_pool.pop().(*connection_data)
	o.setup_connection(cd, td, o.global.restore_queues.pop().(*io_restore_result), start_transaction, use_database, nil)
	return cd
}

func (o *OptionEntries) request_another_connection(td *thread_data, io_restore_result *io_restore_result, start_transaction bool, use_database *database, header *GString) bool {
	if o.global.control_job_ended && td.granted_connections < td.dbt.max_threads && td.dbt.restore_job_list.Len() == 0 {
		var cd *connection_data = o.global.connection_pool.try_pop().(*connection_data)
		if cd != nil {
			o.setup_connection(cd, td, io_restore_result, start_transaction, use_database, header)
			return true
		}
	}
	return false
}

func (o *OptionEntries) m_commit(cd *connection_data) uint {
	if !m_query(cd.thrconn, "COMMIT", m_warning, "COMMIT failed") {
		o.global.errors++
		return 2
	}
	return 0
}

func (o *OptionEntries) m_commit_and_start_transaction(cd *connection_data, query_counter *uint) uint {
	if e := o.m_commit(cd); e != 0 {
		return 1
	}
	*query_counter = 0
	m_query(cd.thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed")
	return 0
}

func (o *OptionEntries) restore_insert(cd *connection_data, data string, query_counter *uint, offset_line uint) int {
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
			if (o.Rows == 0 || current_rows < uint(o.Rows)) && next_line != "" {
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
			tr = o.restore_data_in_gstring_by_statement(cd, new_insert.String(), false, query_counter)
			if cd.transaction && *query_counter == o.CommitCount {
				tr += o.m_commit_and_start_transaction(cd, query_counter)
			}
			if tr > 0 {
				log.Fatalf("Connection %d: Error occurs between lines: %d and %d in a splited INSERT: %v", cd.thread_id, offset_line, current_offset_line, cd.thrconn.err)
			}
			if cd.thrconn.warning != 0 {
				log.Warnf("Connection %d: Warnings found during INSERT between lines: %d and %d: %s", cd.thread_id, offset_line, current_offset_line, o.show_warnings_if_possible(cd.thrconn))
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

func (o *OptionEntries) restore_thread(conn *db_connection, thread_id uint) {
	defer o.global.restore_threads[thread_id].thread.Done()
	var cd *connection_data = o.new_connection_data(conn)
	var ir *statement
	var query_counter uint
	for {
		cd.queue = g_async_queue_pop(cd.ready).(*io_restore_result)
		if cd.queue.restore == nil {
			break
		}
		for {
			ir = g_async_queue_pop(cd.queue.restore).(*statement)
			if ir.kind_of_statement == CLOSE {
				log.Tracef("Releasing connection: %d", cd.thread_id)
				if cd.transaction && query_counter > 0 {
					o.m_commit(cd)
				}
				g_async_queue_push(cd.queue.result, ir)
				cd.queue = nil
				ir = nil
				break
			}
			if ir.kind_of_statement == INSERT {
				ir.result = o.restore_insert(cd, ir.buffer.String(), &query_counter, ir.preline)
				if ir.result > 0 {
					ir.err = cd.thrconn.err
					ir.error_number = uint(cd.thrconn.code)
					if o.global.max_errors != 0 && o.global.errors > o.global.max_errors {
						if ir.filename == "" {
							log.Fatalf("Error occurs processing statement: %v", cd.thrconn.err)
						} else {
							log.Fatalf("Error occurs starting at line: %d on file %s: %v", ir.preline, ir.filename, cd.thrconn.err)
						}
					} else {
						if ir.filename == "" {
							log.Fatalf("Error occurs processing statement: %v", cd.thrconn.err)
						} else {
							log.Fatalf("Error occurs between line: %d on file %s: %v", ir.preline, ir.filename, cd.thrconn.err)
						}
					}
				}
				g_async_queue_push(cd.queue.result, ir)
			} else {
				ir.result = int(o.restore_data_in_gstring_by_statement(cd, ir.buffer.String(), ir.is_schema, &query_counter))
				if ir.result > 0 {
					ir.err = cd.thrconn.err
					ir.error_number = uint(cd.thrconn.code)
				}
				g_async_queue_push(cd.queue.result, ir)
			}
		}
		log.Tracef("Returning connection to pool: %d", cd.thread_id)
		g_async_queue_push(o.global.connection_pool, cd)
	}
	return
}

func (o *OptionEntries) load_data_mutex_locate(filename string, mutex **sync.Mutex) bool {
	o.global.load_data_list_mutex.Lock()

	var orig_key string
	if _, ok := o.global.load_data_list[filename]; !ok {
		*mutex = g_mutex_new()
		(*mutex).Lock()
		o.global.load_data_list[filename] = *mutex
		o.global.load_data_list_mutex.Unlock()
		return true
	} else {
		orig_key = filename
	}
	if orig_key != "" {
		delete(o.global.load_data_list, orig_key)
	}
	o.global.load_data_list_mutex.Unlock()
	return false
}

func (o *OptionEntries) release_load_data_as_it_is_close(filename string) {
	o.global.load_data_list_mutex.Lock()
	var mutex = o.global.load_data_list[filename]
	if mutex == nil {
		o.global.load_data_list[filename] = nil
	} else {
		mutex.Unlock()
	}
	o.global.load_data_list_mutex.Unlock()
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

func (o *OptionEntries) new_statement() *statement {
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

func process_result_vstatement_pop(get_insert_result_queue *asyncQueue, ir **statement, log_fun func(string, ...any), g_async_queue_pop_fun func(*asyncQueue) any, msg string, args ...any) int {
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

func process_result_vstatement(get_insert_result_queue *asyncQueue, ir **statement, log_fun func(string, ...any), msg string, args ...any) int {
	return process_result_vstatement_pop(get_insert_result_queue, ir, log_fun, g_async_queue_pop, msg, args...)
}
func process_result_statement(get_insert_result_queue *asyncQueue, ir **statement, log_fun func(string, ...any), msg string, args ...any) int {
	return process_result_vstatement(get_insert_result_queue, ir, log_fun, msg, args...)
}

func (o *OptionEntries) restore_data_from_file(td *thread_data, filename string, is_schema bool, use_database *database) int {
	var infile *osFile
	var eof bool
	var data *GString
	var err error
	var line int
	var preline uint
	var file_path string = path.Join(o.global.directory, filename)
	infile, err = o.myl_open(file_path, os.O_RDONLY)
	if err != nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		o.global.errors++
		return 1
	}
	var r int
	var load_data_filename, new_load_data_fifo_filename string
	_ = load_data_filename
	var cd *connection_data = o.wait_for_available_restore_thread(td, !is_schema && (o.CommitCount > 1), use_database)
	g_assert(g_async_queue_length(cd.queue.restore) <= 0)
	g_assert(g_async_queue_length(cd.queue.result) <= 0)
	var i uint
	var ir *statement = g_async_queue_pop(o.global.free_results_queue).(*statement)
	var results_added bool
	var header *GString = g_string_new("")
	var inBufio *bufio.Scanner = bufio.NewScanner(infile.file)
	for eof == false {
		if read_data(inBufio, data, &eof, &line) {
			var length int
			if data.len > 5 {
				length = data.len - 5
			}
			if strings.Contains(data.str[length:], ";\n") {
				if o.SkipDefiner && strings.HasPrefix(data.str, "CREATE") {
					remove_definer(data)
				}
				if strings.HasPrefix(data.str, "INSERT") {
					o.request_another_connection(td, cd.queue, cd.transaction, use_database, header)
					if !results_added {
						results_added = true
						var other_ir *statement
						for i = 0; i < 7; i++ {
							other_ir = g_async_queue_pop(o.global.free_results_queue).(*statement)
							g_async_queue_push(cd.queue.result, initialize_statement(other_ir))
						}
					}
					assing_statement(ir, data.str, preline, false, INSERT)
					g_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_vstatement(cd.queue.restore, &ir, m_critical, "(2)Error occurs processing file %s", filename)
				} else if strings.HasPrefix(data.str, "LOAD DATA ") {
					var new_data string
					var from = strings.Index(data.str, "'") + 1
					var to = strings.Index(data.str[from:], "'")
					load_data_filename = data.str[from : from+to]
					var mutex *sync.Mutex
					_ = mutex
					_ = new_data
					_ = new_load_data_fifo_filename
					// var command []string
					// TODO
					assing_statement(ir, data.str, preline, false, OTHER)
					g_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_statement(cd.queue.result, &ir, m_critical, "(2)Error occurs processing file %s", filename)

				} else {
					if strings.HasPrefix(data.str, "/*!") {
						var from_equal = data.str[3:strings.Index(data.str, "=")]
						if from_equal != "" && o.IgnoreSet != "" {
							from_equal = ""
							if !o.is_in_ignore_set_list(data.str) {
								from_equal = "="
								g_string_append(header, data.str)
							} else {
								from_equal = "="
							}
						} else {
							g_string_append(header, data.str)
						}
					} else {
						header = nil
					}
					assing_statement(ir, data.str, preline, false, OTHER)
					g_async_queue_push(cd.queue.restore, ir)
					ir = nil
					process_result_statement(cd.queue.result, &ir, m_critical, "(2)Error occurs processing file %s", filename)
				}
				r |= ir.result
				data.str = ""
				data.len = 0
				preline = uint(line) + 1
				if ir.result > 0 {
					log.Fatalf("(1)Error occurs processing file %s", filename)
				}
			}
		} else {
			log.Errorf("error reading file %s (%v)", filename, err)
			o.global.errors++
			return 1
		}
	}
	var queue *io_restore_result = cd.queue
	g_async_queue_push(o.global.free_results_queue, ir)
	if results_added {
		for i = 0; i < 7; i++ {
			process_result_statement(queue.result, &ir, m_critical, "(2)Error occurs processing file %s", filename)
			g_assert(ir.kind_of_statement != CLOSE)
			g_async_queue_push(o.global.free_results_queue, ir)
		}
	}
	for ; td.granted_connections > 0; td.granted_connections-- {
		g_async_queue_push(queue.restore, &o.global.release_connection_statement)
		process_result_statement(queue.result, &ir, m_critical, "(2)Error occurs processing file %s", filename)
		g_assert(ir.kind_of_statement == CLOSE)
	}
	g_async_queue_push(o.global.restore_queues, queue)
	o.myl_close(filename, infile, true)
	return r
}

func (o *OptionEntries) restore_data_in_gstring_extended(td *thread_data, data *GString, is_schema bool, use_database *database, log_fun func(string, ...any), msg string, args ...any) int {
	var cd *connection_data = o.wait_for_available_restore_thread(td, !is_schema && (o.CommitCount > 1), use_database)
	var queue *io_restore_result = cd.queue
	var ir *statement = g_async_queue_pop(o.global.free_results_queue).(*statement)
	var i int
	var r int
	if data != nil && data.len > 4 {
		var line []string = strings.Split(data.str, ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				assing_statement(ir, line[i], 0, is_schema, OTHER)
				if ir.err != nil {
					ir.err = nil
				}
				g_async_queue_push(queue.restore, ir)
				r += process_result_vstatement(queue.result, &ir, log_fun, msg, args)
			}
		}
	}
	g_async_queue_push(o.global.free_results_queue, ir)
	g_async_queue_push(queue.restore, &o.global.release_connection_statement)
	td.granted_connections--
	r += process_result_vstatement(queue.result, &ir, log_fun, msg, args)
	g_assert(g_async_queue_length(queue.restore) <= 0)
	g_assert(g_async_queue_length(queue.result) <= 0)
	g_async_queue_push(o.global.restore_queues, queue)
	return r
}

func (o *OptionEntries) restore_data_in_gstring(td *thread_data, data *GString, is_schema bool, use_database *database) int {
	return o.restore_data_in_gstring_extended(td, data, is_schema, use_database, m_warning, "Failed to execute statement", nil)
}
