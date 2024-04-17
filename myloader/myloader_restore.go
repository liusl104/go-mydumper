package myloader

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
)

func restore_data_in_gstring_by_statement(o *OptionEntries, td *thread_data, data string, is_schema bool, query_counter *uint) int {
	_, err := td.thrconn.Execute(data)
	if err != nil {
		if is_schema {
			log.Warnf("Thread %d: Error restoring %s: %v", td.thread_id, data, err)
		} else {
			log.Warnf("Thread %d: Error restoring %s: %v", td.thread_id, data, err)
		}
		err = td.thrconn.Ping()
		if err != nil {
			td.thrconn, err = m_connect(o)
			execute_gstring(td.thrconn, o.global.set_session)
			execute_use(td)
			if !is_schema && o.Statement.CommitCount > 1 {
				log.Errorf("Thread %d: Lost connection error", td.thread_id)
				return 2
			}
		}
		log.Warnf("Thread %d: Retrying last failed executed statement", td.thread_id)
		atomic.AddUint64(&o.global.detailed_errors.retries, 1)
		_, err = td.thrconn.Execute(data)
		if err != nil {
			log.Errorf("Thread %d: Error restoring: %v", td.thread_id, err)
			return 1
		}

	}
	*query_counter = *query_counter + 1
	if is_schema == false {
		if o.Statement.CommitCount > 1 {
			if *query_counter == o.Statement.CommitCount {
				*query_counter = 0
				if !m_query(td.thrconn, "COMMIT", m_warning, "COMMIT failed") {
					return 2
				}
				m_query(td.thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed")
			}
		}
	}
	return 0
}

func restore_data_in_gstring(o *OptionEntries, td *thread_data, data string, is_schema bool, query_counter *uint) int {
	var i, r int
	if data != "" && len(data) > 4 {
		var line = strings.Split(data, ";\n")
		for i = 0; i < len(line); i++ {
			if len(line[i]) > 2 {
				var str = line[i]
				str += ";"
				r += restore_data_in_gstring_by_statement(o, td, str, is_schema, query_counter)
			}
		}

	}
	return r
}

func split_and_restore_data_in_gstring_by_statement(o *OptionEntries, td *thread_data, data string, is_schema bool, query_counter *uint, offset_line uint) uint {
	var index = strings.Index(data, "VALUES")
	var next_line = data[index+6:]
	var insert_statement_prefix = data[:index]
	var insert_statement_prefix_len = len(insert_statement_prefix)
	var r uint
	var tr, current_rows int
	var current_offset_line = offset_line - 1
	var current_line = next_line
	next_line = current_line[:strings.Index(current_line, "\n")]
	var new_insert string

	for next_line != "" {
		current_rows = 0
		new_insert = insert_statement_prefix
		for current_rows < o.Statement.Rows && next_line != "" {
			var line = current_line[strings.Index(current_line, "\n"):]
			new_insert += line
			current_rows++
			current_line = next_line[1:]
			next_line = current_line[strings.Index(current_line, "\n"):]
			current_offset_line++
		}
		if len(new_insert) > insert_statement_prefix_len {
			tr = restore_data_in_gstring_by_statement(o, td, new_insert, is_schema, query_counter)
		} else {
			tr = 0
		}
		r += uint(tr)
		if tr > 0 {
			log.Fatalf("Error occurs between lines: %d and %d in a splited INSERT", offset_line, current_offset_line)
		}
		offset_line = current_offset_line + 1
		current_line = current_line[1:]
	}
	insert_statement_prefix = ""
	data = ""
	return r
}

func load_data_mutex_locate(o *OptionEntries, filename string, mutex **sync.Mutex) bool {
	o.global.load_data_list_mutex.Lock()
	var ok bool
	*mutex, ok = o.global.load_data_list[filename]
	if !ok {
		*mutex = g_mutex_new()
		(*mutex).Lock()
		o.global.load_data_list[filename] = *mutex
		o.global.load_data_list_mutex.Unlock()
		return true
	}
	if *mutex != nil {
		delete(o.global.load_data_list, filename)
	}
	o.global.load_data_list_mutex.Unlock()
	return false
}

func wait_til_data_file_is_close(o *OptionEntries, filename string) {
	var mutex *sync.Mutex
	if load_data_mutex_locate(o, filename, &mutex) {
		mutex.Lock()
	}
}

func release_load_data_as_it_is_close(o *OptionEntries, filename string) {
	o.global.load_data_list_mutex.Lock()
	mutex, _ := o.global.load_data_list[filename]
	if mutex == nil {
		o.global.load_data_list[filename] = nil
	} else {
		mutex.Unlock()
	}
	o.global.load_data_list_mutex.Unlock()
}

func restore_data_from_file(o *OptionEntries, td *thread_data, database string, table string, filename string, is_schema bool) int {
	var infile *os.File
	var r int
	var eof bool
	var query_counter uint
	var data string
	var line, preline uint
	var err error
	var pt = path.Join(o.global.directory, filename)
	infile, err = myl_open(o, pt, os.O_RDONLY)
	if infile == nil {
		log.Errorf("cannot open file %s (%v)", filename, err)
		return 1
	}
	if !is_schema && o.Statement.CommitCount > 1 {
		m_query(td.thrconn, "START TRANSACTION", m_warning, "START TRANSACTION failed")
	}
	var tr uint
	var load_data_filename string
	var load_data_fifo_filename string
	var new_load_data_fifo_filename string
	for eof == false {
		reader := bufio.NewReader(infile)
		if read_data(reader, &data, &eof, &line) {
			var length int
			if len(data) >= 5 {
				length = len(data) - 5
			} else {
				length = len(data)
			}
			if strings.Contains(data[length:], ";\n") {
				if o.Statement.SkipDefiner && strings.HasPrefix(data, "CREATE") {
					data = remove_definer(data)
				}
				if o.Statement.Rows > 0 && data[:6] == "INSERT" {
					tr = split_and_restore_data_in_gstring_by_statement(o, td, data, is_schema, &query_counter, preline)
				} else {
					if data[:10] == "LOAD DATA " {
						var new_data string
						var from = data[strings.Index(data, "'")+1:]
						var to = from[strings.Index(from, "'"):]
						load_data_filename = data[:len(from)-len(to)]
						if o.Common.FifoDirectory != "" {
							new_data += o.Common.FifoDirectory
							new_data += "/"
							new_data += from
							from = new_data[:strings.Index(new_data, "'")+1]
							data = new_data
							to = from[:strings.Index(from, "'")]
						}
						wait_til_data_file_is_close(o, load_data_filename)
						if get_command_and_basename(o, load_data_filename, &load_data_fifo_filename) {
							to = data[len(load_data_filename)-len(load_data_fifo_filename)+1:]
							to = "'" + to
							if o.Common.FifoDirectory != "" {
								new_load_data_fifo_filename = fmt.Sprintf("%s/%s", o.Common.FifoDirectory, load_data_fifo_filename)
								load_data_fifo_filename = new_load_data_fifo_filename
							}
							execute_file_per_thread(load_data_filename, load_data_fifo_filename, "")
							release_load_data_as_it_is_close(o, load_data_fifo_filename)
						}
						tr = uint(restore_data_in_gstring_by_statement(o, td, data, is_schema, &query_counter))
						if load_data_fifo_filename != "" {
							m_remove(o, "", load_data_fifo_filename)
						} else {
							m_remove(o, "", load_data_filename)
						}
					} else {
						tr = uint(restore_data_in_gstring_by_statement(o, td, data, is_schema, &query_counter))
					}
				}
				r += int(tr)
				if tr > 0 {
					log.Fatalf("Error occurs between lines: %d and %d on file %s", preline, line, filename)
				}
				preline = line + 1
			}
		} else {
			log.Errorf("error reading file %s", filename)
			return r
		}
	}
	if !is_schema && (o.Statement.CommitCount > 1) && !m_query(td.thrconn, "COMMIT", m_warning, "COMMIT failed") {
		log.Fatalf("Error committing data for %s.%s from file %s", database, table, filename)
	}
	myl_close(o, filename, infile)
	return r
}
