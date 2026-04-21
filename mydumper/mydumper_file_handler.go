package mydumper

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	close_file_queue *GAsyncQueue
	available_pids   *GAsyncQueue
	fifo_table_mutex *sync.Mutex
	pipe_creation    *sync.Mutex
	open_pipe        int64
	cft              *GThread
	is_pipe          bool
	fifo_hash        map[string]string
)

// filename_queue_element holds a table, output filename, and a done queue for stream mode.
type filename_queue_element struct {
	dbt      *db_table
	filename string
	done     *GAsyncQueue
}

// m_open_file opens a file for reading or writing. Mode "w" creates/truncates for write, otherwise opens read-only.
func m_open_file(filename *string, t string) (f *file_write, err error) {

	f = new(file_write)
	var ff *os.File
	f.filename = *filename
	if strings.ToLower(t) == "w" {
		ff, err = os.OpenFile(*filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
		if err != nil {
			log.Criticalf("open file %s failed: %v", *filename, err)
			return
		}
	} else {
		ff, err = os.OpenFile(*filename, os.O_RDONLY, 0660)
		if err != nil {
			log.Criticalf("open file %s failed: %v", *filename, err)
			return
		}
	}
	f.status = 1
	f.write = ff.Write
	f.close = ff.Close
	f.flush = ff.Sync

	return
}

// m_close_file closes the file handle, optionally removes empty files, and on stream mode pushes filename to stream queue.
func m_close_file(thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	var err error
	if file != nil {
		log.Tracef("Thread %d: Closing file %s", thread_id, filename)
		err = file.close()
		file.status = 0
		if size > 0 {
			if Stream != "" {
				stream_queue_push(dbt, filename)
			}
		} else if !BuildEmptyFiles {
			err = os.Remove(filename)
			if err != nil {
				log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
			} else {
				log.Debugf("Thread %d: File removed: %s", thread_id, filename)
			}
		}
	} /*else {
		M_critical("Trying to close %s with thread: %d", filename, thread_id)
	}*/

	return err
}

// close_file_queue_push pushes a fifo to the close queue and waits for child pid if present.
func close_file_queue_push(f *fifo) {
	G_async_queue_push(close_file_queue, f)
	if f.child_pid > 0 {
		// var status int
		var pid int
		var b bool = true
		for b {
			for pid == -1 {
				pipe_creation.Lock()
				// TODO
				pid = 1
				pipe_creation.Unlock()
				if pid > 0 {
					b = false
					break
				} else if pid == -1 {
					b = false
					break
				}
			}
		}
	}
	return
}

// release_pid returns a PID slot to the available pool.
func release_pid() {
	G_async_queue_push(available_pids, 1)
}

// execute_file_per_thread runs per-thread file execution (e.g. compression). Currently a stub returning 0.
func execute_file_per_thread(sql_fn string, sql_fn3 string) int {
	// TODO
	/*
		// Create a file to save compressed data
		outFile, err := os.Create("list.gz")
		if err != nil {
			panic(err)
		}
		defer outFile.Close()

		// Create ls command
		lsCmd := exec.Command("ls")

		// Create gzip command
		gzipCmd := exec.Command("gzip", "-c")

		// Set gzip command stdout to file
		gzipCmd.Stdout = outFile

		// Create pipe
		pipeReader, pipeWriter := io.Pipe()

		// Set ls command stdout to pipe write end
		lsCmd.Stdout = pipeWriter
		// Set gzip command stdin to pipe read end
		gzipCmd.Stdin = pipeReader

		// Start ls command
		if err := lsCmd.Start(); err != nil {
			panic(err)
		}

		// Start gzip command
		if err := gzipCmd.Start(); err != nil {
			panic(err)
		}

		// Wait for ls command to finish and close pipe write end
		if err := lsCmd.Wait(); err != nil {
			panic(err)
		}
		pipeWriter.Close()

		// Wait for gzip command to finish
		if err := gzipCmd.Wait(); err != nil {
			panic(err)
		}*/
	return 0
}

// m_open_pipe opens a pipe for writing (e.g. to gzip/zstd) and returns a file_write that writes to the pipe stdin.
func m_open_pipe(filename *string, mode string) (*file_write, error) {
	*filename = fmt.Sprintf("%s%s", *filename, ExecPerThreadExtension)
	var flag int
	if strings.ToLower(mode) == "w" {
		flag = os.O_CREATE | os.O_WRONLY
	} else if strings.ToLower(mode) == "r" {
		flag = os.O_RDONLY
	}
	file, err := os.OpenFile(*filename, flag, 0660)
	if err != nil {
		log.Fatalf("open file %s fail:%v", *filename, err)
	}
	var compressFile *gzip.Writer
	var compressEncode *zstd.Encoder
	var f = new(file_write)
	f.filename = *filename
	f.status = 1
	switch strings.ToUpper(compress_method) {
	case GZIP:
		compressFile, err = gzip.NewWriterLevel(file, gzip.DefaultCompression)
		if err != nil {
			return nil, err
		}
		f.flush = compressFile.Flush
		f.write = compressFile.Write
		f.close = compressFile.Close
		return f, err
	case ZSTD:
		compressEncode, err = zstd.NewWriter(file)
		if err != nil {
			return nil, err
		}
		f.flush = compressEncode.Flush
		f.write = compressEncode.Write
		f.close = compressEncode.Close
		return f, err

	default:
		if err != nil {
			return nil, err
		}

		f.flush = file.Sync
		f.write = file.Write
		f.writeStr = file.WriteString
		f.close = file.Close
		return f, err
	}

}

// m_close_pipe closes the pipe file, releases a PID, and optionally removes empty file or pushes to stream queue.
func m_close_pipe(thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	release_pid()
	var err error
	err = file.close()
	file.status = 0
	if size > 0 {
		if Stream != "" {
			stream_queue_push(dbt, "")
		}
	} else if !BuildEmptyFiles {
		err = os.Remove(filename)
		if err != nil {
			log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}

	return err
}

// final_step_close_file handles post-close for pipe: pushes to stream queue if size > 0, or removes empty file.
func final_step_close_file(thread_id uint, filename string, f *fifo, size float64, dbt *db_table) error {
	if size > 0 {
		if Stream != "" {
			stream_queue_push(dbt, f.stdout_filename)
		}
	} else if !BuildEmptyFiles {
		if os.Remove(f.stdout_filename) != nil {
			log.Warnf("Thread %d: Failed to remove empty file: %s", thread_id, f.stdout_filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}
	return nil
}

// close_file_thread is the worker that pops from close_file_queue, closes pipe/file handles, and calls final_step_close_file.
func close_file_thread(c any) {
	_ = c
	var f *fifo
	var err error
	for {
		if G_async_queue_length(close_file_queue) == 0 {
			return
		}
		f = G_async_queue_pop(close_file_queue).(*fifo)
		if f.gpid == -10 {
			// TODO
			break
		}
		pipe_creation.Lock()
		f.pipe[1].close()
		f.pipe[0].close()
		pipe_creation.Unlock()
		f.out_mutes.Lock()
		// TODO
		err = f.fout.flush()
		if err != nil {
			log.Errorf("\"while syncing file %s (%v)", f.stdout_filename, err)
		}
		f.fout.close()
		release_pid()
		final_step_close_file(0, f.filename, f, f.size, f.dbt)
		G_atomic_int_dec_and_test(&open_pipe)
	}
	return
}

// wait_close_files sends a shutdown sentinel to the close queue and waits for the close file thread to finish.
func wait_close_files() {
	var f *fifo = new(fifo)
	f.gpid = -10
	f.child_pid = -10
	f.filename = ""
	close_file_queue_push(f)
	cft.Thread.Wait()
}

// set_pipe_backup sets is_pipe to true so that m_open/m_close use pipe-based backup.
func set_pipe_backup() {
	is_pipe = true
}

// initialize_file_handler sets m_open/m_close to file or pipe variants, creates PID pool and close_file_queue, and starts close_file_thread.
func initialize_file_handler() {
	if is_pipe {
		m_open = m_open_pipe
		m_close = m_close_pipe
	} else {
		m_open = m_open_file
		m_close = m_close_file
	}
	available_pids = G_async_queue_new("available_pids")
	close_file_queue = G_async_queue_new("close_file_queue")
	var i uint = 0
	for i = 0; i < NumThreads*2; i++ {
		release_pid()
	}
	pipe_creation = G_mutex_new()
	file_hash = make(map[string]map[string][]string)
	fifo_table_mutex = G_mutex_new()
	cft = M_thread_new("close_file_thread", close_file_thread, nil, "Close file thread could not be created")
}

// new_filename_queue_element allocates a filename_queue_element for stream queue (dbt, filename, done queue).
func new_filename_queue_element(dbt *db_table, filename string, done *GAsyncQueue) *filename_queue_element {
	var sf = new(filename_queue_element)
	sf.dbt = dbt
	sf.filename = filename
	sf.done = done
	return sf
}
