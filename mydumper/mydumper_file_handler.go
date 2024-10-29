package mydumper

import (
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/siddontang/go-log/log"
	"os"
	"strings"
)

func m_open_file(o *OptionEntries, filename *string, t string) (f *file_write, err error) {
	_ = o
	f = new(file_write)
	var ff *os.File
	if strings.ToLower(t) == "w" {
		ff, err = os.OpenFile(*filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
		if err != nil {
			log.Errorf("open file %s failed: %v", filename, err)
			return
		}
	} else {
		ff, err = os.OpenFile(*filename, os.O_RDONLY, 0660)
		if err != nil {
			log.Errorf("open file %s failed: %v", filename, err)
			return
		}
	}
	f.write = ff.Write
	f.close = ff.Close
	f.flush = ff.Sync

	return
}

// o *OptionEntries, thread_id uint, file *os.File, filename string, size uint, dbt *db_table
func m_close_file(o *OptionEntries, thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	var err error
	err = file.close()
	if size > 0 {
		if o.Stream.Stream {
			stream_queue_push(o, dbt, filename)
		}
	} else if !o.Extra.BuildEmptyFiles {
		err = os.Remove(filename)
		if err != nil {
			log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}
	return err
}

func close_file_queue_push(o *OptionEntries, f *fifo) {
	o.global.close_file_queue.push(f)
	if f.child_pid > 0 {
		// var status int
		var pid int
		var b bool = true
		for b {
			for pid == -1 {
				o.global.pipe_creation.Lock()
				// TODO
				pid = 1
				o.global.pipe_creation.Unlock()
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

func wait_close_files(o *OptionEntries) {
	var f *fifo = new(fifo)
	f.gpid = -10
	f.child_pid = -10
	f.filename = ""
	close_file_queue_push(o, f)
	o.global.thread_pool["cft"].join.Wait()
}

func release_pid(o *OptionEntries) {
	o.global.available_pids.push(1)
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

func m_open_pipe(o *OptionEntries, filename *string, mode string) (*file_write, error) {
	*filename = fmt.Sprintf("%s%s", *filename, o.Exec.ExecPerThreadExtension)
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
	switch strings.ToUpper(o.Extra.CompressMethod) {
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

func m_close_pipe(o *OptionEntries, thread_id uint, file *file_write, filename string, size float64, dbt *db_table) error {
	release_pid(o)
	var err error
	err = file.close()
	if size > 0 {
		if o.Stream.Stream {
			stream_queue_push(o, dbt, "")
		}
	} else if !o.Extra.BuildEmptyFiles {
		err = os.Remove(filename)
		if err != nil {
			log.Warnf("Thread %d: Failed to remove empty file : %s", thread_id, filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}

	return err
}

func final_step_close_file(o *OptionEntries, thread_id uint, filename string, f *fifo, size float64, dbt *db_table) error {
	if size > 0 {
		if o.Stream.Stream {
			stream_queue_push(o, dbt, f.stdout_filename)
		}
	} else if !o.Extra.BuildEmptyFiles {
		if os.Remove(f.stdout_filename) != nil {
			log.Warnf("Thread %d: Failed to remove empty file: %s", thread_id, f.stdout_filename)
		} else {
			log.Debugf("Thread %d: File removed: %s", thread_id, filename)
		}
	}
	return nil
}

func close_file_thread(o *OptionEntries) {
	_ = o
	var f *fifo
	var err error
	for {
		f = o.global.close_file_queue.pop().(*fifo)
		if f.gpid == -10 {
			// TODO
			break
		}
		o.global.pipe_creation.Lock()
		f.pipe[1].close()
		f.pipe[0].close()
		o.global.pipe_creation.Unlock()
		f.out_mutes.Lock()
		// TODO
		err = f.fout.flush()
		if err != nil {
			log.Errorf("\"while syncing file %s (%v)", f.stdout_filename, err)
		}
		f.fout.close()
		release_pid(o)
		final_step_close_file(o, 0, f.filename, f, f.size, f.dbt)
		g_atomic_int_dec_and_test(&o.global.open_pipe)
	}
	return

}

func initialize_file_handler(o *OptionEntries, is_pipe bool) {
	if is_pipe {
		m_open = m_open_pipe
		m_close = m_close_pipe
	} else {
		m_open = m_open_file
		m_close = m_close_file
	}
	o.global.available_pids = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.close_file_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	var i uint = 0
	for i = 0; i < o.Common.NumThreads*2; i++ {
		release_pid(o)
	}
	o.global.pipe_creation = g_mutex_new()
	o.global.file_hash = make(map[string]map[string][]string)
	o.global.fifo_table_mutex = g_mutex_new()
	g_thread_create(o, close_file_thread, "cft", true)
}
