package mydumper

import (
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/siddontang/go-log/log"
	. "go-mydumper/src"
	"os"
	"strings"
	"sync"
)

var (
	close_file_queue *GAsyncQueue
	available_pids   *GAsyncQueue
	fifo_table_mutex *sync.Mutex
	pipe_creation    *sync.Mutex
	open_pipe        int64
	cft              *GThreadFunc
	fifo_hash        map[string]string
)

func m_open_file(filename *string, t string) (f *file_write, err error) {

	f = new(file_write)
	var ff *os.File
	if strings.ToLower(t) == "w" {
		ff, err = os.OpenFile(*filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
		if err != nil {
			log.Errorf("open file %s failed: %v", *filename, err)
			return
		}
	} else {
		ff, err = os.OpenFile(*filename, os.O_RDONLY, 0660)
		if err != nil {
			log.Errorf("open file %s failed: %v", *filename, err)
			return
		}
	}
	f.status = 1
	f.write = ff.Write
	f.close = ff.Close
	f.flush = ff.Sync

	return
}

// , thread_id uint, file *os.File, filename string, size uint, dbt *DB_Table
func m_close_file(thread_id uint, file *file_write, filename string, size float64, dbt *DB_Table) error {
	var err error
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
	return err
}

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

func wait_close_files() {
	var f *fifo = new(fifo)
	f.gpid = -10
	f.child_pid = -10
	f.filename = ""
	close_file_queue_push(f)
	cft.Thread.Wait()
}

func release_pid() {
	G_async_queue_push(available_pids, 1)
}

func execute_file_per_thread(sql_fn string, sql_fn3 string) int {
	// TODO
	/*
	 // 创建一个文件用于保存压缩后的数据
	    outFile, err := os.Create("list.gz")
	    if err != nil {
	        panic(err)
	    }
	    defer outFile.Close()

	    // 创建ls命令
	    lsCmd := exec.Command("ls")

	    // 创建gzip命令
	    gzipCmd := exec.Command("gzip", "-c")

	    // 设置gzip命令的标准输出到文件
	    gzipCmd.Stdout = outFile

	    // 创建管道
	    pipeReader, pipeWriter := io.Pipe()

	    // 设置ls命令的标准输出到管道的写入端
	    lsCmd.Stdout = pipeWriter
	    // 设置gzip命令的标准输入为管道的读取端
	    gzipCmd.Stdin = pipeReader

	    // 开始执行ls命令
	    if err := lsCmd.Start(); err != nil {
	        panic(err)
	    }

	    // 开始执行gzip命令
	    if err := gzipCmd.Start(); err != nil {
	        panic(err)
	    }

	    // 等待ls命令完成，并关闭管道的写入端
	    if err := lsCmd.Wait(); err != nil {
	        panic(err)
	    }
	    pipeWriter.Close()

	    // 等待gzip命令完成
	    if err := gzipCmd.Wait(); err != nil {
	        panic(err)
	    }*/
	return 0
}

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
		f.close = file.Close
		return f, err
	}

}

func m_close_pipe(thread_id uint, file *file_write, filename string, size float64, dbt *DB_Table) error {
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

func final_step_close_file(thread_id uint, filename string, f *fifo, size float64, dbt *DB_Table) error {
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

func close_file_thread() {
	defer cft.Thread.Done()
	var f *fifo
	var err error
	for {
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

func initialize_file_handler(is_pipe bool) {
	if is_pipe {
		m_open = m_open_pipe
		m_close = m_close_pipe
	} else {
		m_open = m_open_file
		m_close = m_close_file
	}
	available_pids = G_async_queue_new(BufferSize)
	close_file_queue = G_async_queue_new(BufferSize)
	var i uint = 0
	for i = 0; i < NumThreads*2; i++ {
		release_pid()
	}
	pipe_creation = G_mutex_new()
	file_hash = make(map[string]map[string][]string)
	fifo_table_mutex = G_mutex_new()
	go close_file_thread()
}
