package myloader

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

const (
	STREAM_BUFFER_SIZE           = 1000000
	STREAM_BUFFER_SIZE_NO_STREAM = 100
)

var (
	stream_thread         *GThread
	metadata_header_mutex *sync.Mutex
	metadata_header_cond  *sync.Cond
	metadata_header_done  bool
	m_close_stream        func(*os.File) error
)

// initialize_stream starts the stream thread (process_stream) and initializes metadata_header mutex/cond.
func initialize_stream(c *configuration) {
	stream_thread = M_thread_new("myloader_stream", process_stream, c, "Stream thread could not be created")
	metadata_header_mutex = G_mutex_new()
	metadata_header_cond = sync.NewCond(metadata_header_mutex)
	metadata_header_done = false
	m_close_stream = func(file *os.File) error {
		return file.Close()
	}
}

// wait_stream_to_finish joins the stream thread if it was started.
func wait_stream_to_finish() {
	if stream_thread != nil {
		G_thread_join(stream_thread)
	}
}

// wait_stream_to_process_metadata_header blocks until metadata_header_done is true (signaled by metadata_has_been_processed).
func wait_stream_to_process_metadata_header() {
	if metadata_header_mutex == nil {
		// If stream is not initialized, return (should not happen, but add safety check)
		return
	}
	metadata_header_mutex.Lock()
	for !metadata_header_done {
		metadata_header_cond.Wait()
	}
	metadata_header_mutex.Unlock()
}

// metadata_has_been_processed sets metadata_header_done and signals waiters (stream metadata header is done).
func metadata_has_been_processed() {
	if metadata_header_mutex == nil {
		// If stream is not initialized, return (should not happen, but add safety check)
		return
	}
	metadata_header_mutex.Lock()
	metadata_header_done = true
	metadata_header_cond.Signal()
	metadata_header_mutex.Unlock()
}

// read_stream_line reads up to c_to_read bytes from stdin into buffer; returns bytes read.
func read_stream_line(buffer []byte, c_to_read int) int {
	n, _ := os.Stdin.Read(buffer[:c_to_read])
	return n
}

// flush writes buffer[from:to+1] to file and adds the length to total_size.
func flush(buffer []byte, from int, to int, file *os.File, total_size *uint) {
	if file != nil {
		data := buffer[from : to+1]
		written, err := file.Write(data)
		if err != nil || written != len(data) {
			log.Criticalf("Error on writing")
		}
		*total_size += uint(to - from + 1)
	}
}

// has_mydumper_suffix returns true if the line looks like a mydumper output filename (.dat, .sql, metadata.partial, or metadata).
func has_mydumper_suffix(line string) bool {
	return m_filename_has_suffix(line, ".dat") ||
		m_filename_has_suffix(line, ".sql") ||
		strings.Contains(line, "metadata.partial") ||
		strings.HasPrefix(line, "metadata")
}

// process_stream reads from stdin (or small buffer if No_stream), parses mydumper stream format, and dispatches filenames to the intermediate queue.
func process_stream(c any) {
	stream_conf := c.(*configuration)
	var filename, real_filename, previous_filename string
	var stream_buffer_size uint
	if No_stream {
		stream_buffer_size = STREAM_BUFFER_SIZE_NO_STREAM
	} else {
		stream_buffer_size = STREAM_BUFFER_SIZE
	}
	buffer := make([]byte, stream_buffer_size)
	var file *os.File
	var pos, buffer_len uint
	var diff, i, line_from, line_end uint
	var initial_pos uint
	var total_size uint
	var file_size_from_stream uint
	set_buffer := G_string_sized_new(1000)
	G_string_set_size(set_buffer, 0)
	writing_set := true
	var database_name string
	if DB != "" {
		database_name = DB
	}
	var table_name string
	for i = 0; i < stream_buffer_size; i++ {
		buffer[i] = 0
	}
	var new_filename, new_real_filename, kind string
	var num int

	for {
		// Reads from stdin and fills the buffer from last position
	read_more:
		n := read_stream_line(buffer[int(diff):], int(stream_buffer_size-1)-int(diff))
		buffer_len = uint(n) + diff

		if buffer_len == diff {
			// This means that there is nothing else to read from stdin
			// so, we need to flush and EXIT.
			flush(buffer, 0, int(buffer_len-1), file, &total_size)
			break
		}

		if buffer_len == 0 {
			// We read nothing, we have to EXIT
			break
		}

		if MySQLDump {
			// We have data to process
			// we always start reading from the beginning of the buffer
			pos = 0
			diff = 0
			for pos < buffer_len {
				initial_pos = pos
				if buffer[pos] == '\n' {
					// new lines means new file header, new header of file content or new file content
					pos++

					if set_buffer.Len > 0 {
						// SET has been written
						writing_set = false
						if file == nil {
							if strings.HasPrefix(string(buffer[line_from:]), "--") {
								// after writing the SET and when file is nil, we should be reading the header of the file
								// we create a temporary filename
								if strings.HasPrefix(string(buffer[initial_pos:]), "\nUSE ") {
									parts := strings.Split(string(buffer[initial_pos:]), "`")
									if len(parts) >= 2 {
										database_name = parts[1]
									}
								} else {
									filename = fmt.Sprintf("mydumper_tmp.table_%d.sql", num)
									num++

									real_filename = path.Join(directory, filename)

									var err error
									file, err = os.OpenFile(real_filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
									if err != nil {
										log.Criticalf("Cannot open file %s: %v", real_filename, err)
									}
									table_name = ""
									kind = ""

									flush([]byte(set_buffer.Str.String()), 0, set_buffer.Len-1, file, &total_size)
									flush(buffer, int(initial_pos), int(line_end-1), file, &total_size)
								}
							}
						} else {
							// File content was being written, we might need to flush from initial_pos to line_from
							if initial_pos < line_from {
								// flushing from initial_pos to line_from - 1
								flush(buffer, int(initial_pos), int(line_from-1), file, &total_size)
							}
							m_close_stream(file)
							file = nil
							if table_name != "" {
								if database_name != "" {
									new_filename = fmt.Sprintf("%s.%s%s.sql", database_name, table_name, kind)
								} else {
									new_filename = fmt.Sprintf("%s%s.sql", table_name, kind)
								}
								new_real_filename = path.Join(directory, new_filename)
								os.Rename(real_filename, new_real_filename)
								log.Debugf("renaming: %s -> %s", real_filename, new_real_filename)
							} else {
								new_filename = filename
							}
							filename = ""
							// sending previous file for processing
							if !strings.HasPrefix(new_filename, "mydumper_tmp") {
								intermediate_queue_new(new_filename)
							}
							new_filename = ""
						}
					}
				}

				// we need to determine the right line_from
				if initial_pos != pos {
					line_from = pos - 1
				} else {
					line_from = pos
				}

				// We process by line to correctly detect the new file
				for pos < buffer_len && buffer[pos] != '\n' {
					pos++
				}

				line_end = pos
				// At this point we know:
				// - line_from
				// - line_end

				if file != nil {
					if (line_end-line_from < 20) && (buffer[pos] != '\n') && (line_from >= 20) {
						// this is not a line, which means pos == buffer_len, so we are at the end of the buffer
						// we need to copy the first 20 chars to the beginning of the buffer to get relevant info
						log.Infof("Copying")
						diff = line_end - line_from
						copy(buffer, buffer[line_from:line_end+1])
						continue
					}
					// Can we get relevant info?
					line_str := string(buffer[line_from:])
					if strings.HasPrefix(line_str, "CREATE TABLE ") ||
						strings.HasPrefix(line_str, "/*!50001 CREATE VIEW") ||
						strings.HasPrefix(line_str, "/*!50001 VIEW") {
						parts := strings.Split(line_str, "`")
						if len(parts) >= 2 {
							table_name = parts[1]
						}
						kind = "-schema"
					} else if strings.HasPrefix(line_str, "INSERT INTO ") {
						parts := strings.Split(line_str, "`")
						if len(parts) >= 2 {
							table_name = parts[1]
						}
						kind = fmt.Sprintf(".000%d", num)
					}

					if buffer[line_end] == '\n' {
						flush(buffer, int(initial_pos), int(line_end), file, &total_size)
						pos++
					} else {
						flush(buffer, int(initial_pos), int(line_end-1), file, &total_size)
					}
					continue
				} else {
					if writing_set {
						// file was nil, this must be the header of the mysqldump
						if buffer[line_end] == '\n' {
							if !strings.HasPrefix(string(buffer[line_from:]), "--") && initial_pos != line_end {
								G_string_append(set_buffer, string(buffer[initial_pos:line_end+1]))
							}
							pos++
						} else {
							diff = buffer_len - initial_pos
							copy(buffer, buffer[initial_pos:initial_pos+diff+1])
							goto read_more
						}
					} else {
						pos++
					}
				}
			}
		} else {
			// mydumper stream

			// We have data to process
			// we always start reading from the beginning of the buffer
			pos = 0
			diff = 0
			for pos < buffer_len {
				initial_pos = pos
				for pos < buffer_len && buffer[pos] == '\n' {
					// local new lines are ignored at this point, it will be written
					pos++
				}

				// we need to determine the right line_from
				if initial_pos != pos {
					line_from = pos - 1
				} else {
					line_from = pos
				}

				// We process by line to correctly detect the header
				for pos < buffer_len && buffer[pos] != '\n' {
					pos++
				}

				line_end = pos

				// At this point we know:
				// - line_from
				// - line_end

				// is it a line?
				if buffer[line_end] == '\n' {
					// As it is a line we need to detect if it is a header
					line_str := string(buffer[line_from:])
					if strings.HasPrefix(line_str, "\n-- ") {
						// header tag detected
						if file != nil {
							// Another file was being written, we might need to flush from initial_pos to line_from
							if initial_pos < line_from {
								// flushing from initial_pos to line_from - 1
								flush(buffer, int(initial_pos), int(line_from-1), file, &total_size)
							}
							if !No_stream {
								// Content of the file are coming from stdin, it is not sharing the backup dir
								if total_size < file_size_from_stream {
									// The file size reported in the header is not the same that the amount of data written
									// this means that the content of the file has the header tag
									// we need to flush and continue
									flush(buffer, int(line_from), int(line_end-1), file, &total_size)
									log.Infof("Different file size in %s. Should be: %d | Written: %d. But continuing", filename, file_size_from_stream, total_size)
									continue
								} else if total_size > file_size_from_stream {
									// we wrote on the file more data than the file size reported in the header
									log.Criticalf("Different file size in %s. Should be: %d | Written: %d", filename, file_size_from_stream, total_size)
								} else {
									// The amount of data written and the file size reported in the header match!
									total_size = 0
								}
							} else {
								// we do not expect file size reported on the header in this case
								if total_size > 0 {
									log.Criticalf("Different file size in %s. Should be: 0 | Written: %d", filename, total_size)
								}
							}
							previous_filename = filename
							filename = ""
						}
						// processing header
						parts := strings.Split(line_str, " ")
						if len(parts) >= 2 {
							filename = parts[1]
						}
						if len(parts) >= 3 {
							// detecting file size reported on the header
							file_size_from_stream_uint64, _ := strconv.ParseUint(parts[2], 10, 64)
							file_size_from_stream = uint(file_size_from_stream_uint64)
						}

						// sending previous file for processing
						real_filename = path.Join(directory, filename)
						if file != nil {
							m_close_stream(file)
						}
						if previous_filename != "" {
							intermediate_queue_new(previous_filename)
							previous_filename = ""
						}
						if G_file_test(real_filename) {
							if No_stream {
								if total_size > 0 {
									log.Criticalf("Different file size in %s. Should be: 0 | Written: %d", filename, total_size)
								}
								intermediate_queue_new(filename)
							} else {
								log.Warnf("Stream Thread: File %s exists in datadir, we are not replacing", real_filename)
								file = nil
							}
						} else {
							if No_stream {
								log.Criticalf("File %s not found in backup dir when using NO_STREAM.", filename)
							}
							var err error
							file, err = os.OpenFile(real_filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
							if err != nil {
								log.Criticalf("Cannot open file %s: %v", real_filename, err)
							}
							m_close_stream = func(f *os.File) error {
								return f.Close()
							}
						}
						if !has_mydumper_suffix(filename) {
							log.Debugf("Not a mydumper file: %s", filename)
						}
						pos++
						continue
					}
					// this was a common line, flushing to disk
					flush(buffer, int(initial_pos), int(line_end-1), file, &total_size)
					continue
				} else {
					// It reached end of buffer
					//
					// this data doesn't end with new line
					// but we need to check if starts with --

					if line_end-line_from >= 4 {
						// In the buffer remains more than 4 chars
						if strings.HasPrefix(string(buffer[line_from:]), "\n-- ") {
							// It could be a header, so we copied to the beginning of the buffer
							diff = buffer_len - initial_pos
							copy(buffer, buffer[initial_pos:initial_pos+diff+1])
							// diff remains set to do not overwrite the buffer
						} else {
							// it is safe to flush it all the content of the buffer
							flush(buffer, int(initial_pos), int(line_end-1), file, &total_size)
							diff = 0
							// the buffer will start empty
						}
					} else {
						tmp_len := 4
						if int(line_end-line_from) < tmp_len {
							tmp_len = int(line_end - line_from)
						}
						tmp := string(buffer[line_from : line_from+uint(tmp_len)])
						if len(tmp) >= 1 && strings.Contains("\n-- ", tmp) {
							// we need to move to the beginning of the buffer and reprocess
							diff = buffer_len - initial_pos
							copy(buffer, buffer[initial_pos:initial_pos+diff+1])
						} else {
							flush(buffer, int(initial_pos), int(line_end-1), file, &total_size)
							diff = 0
						}
					}
					goto read_more
				}
			}
		}
	}
	if MySQLDump {
		if file != nil {
			m_close_stream(file)
		}
		if !No_stream && filename != "" {
			intermediate_queue_new(filename)
		}
	} else {
		if file != nil {
			m_close_stream(file)
		}
		if !No_stream && filename != "" {
			intermediate_queue_new(filename)
		}
	}
	intermediate_queue_end()
	var n uint
	for n = 0; n < NumThreads; n++ {
		// g_async_queue_push(stream_conf.data_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(stream_conf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(stream_conf.post_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(stream_conf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
	return
}
