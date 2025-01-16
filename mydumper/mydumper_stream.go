package mydumper

import (
	"bufio"
	"fmt"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"io"
	"os"
	"path"
	"slices"
	"sync"
	"time"
)

const (
	METADATA_PARTIAL_INTERVAL = 2
	STREAM_BUFFER_SIZE        = 1000000
)

var (
	initial_metadata_queue         *GAsyncQueue
	initial_metadata_lock_queue    *GAsyncQueue
	metadata_partial_queue         *GAsyncQueue
	metadata_partial_writer_alive  bool
	metadata_partial_writer_thread *GThreadFunc
	stream_thread                  *GThreadFunc
)

type stream_queue_element struct {
	dbt      *DB_Table
	filename string
}

func metadata_partial_queue_push(dbt *DB_Table) {
	if dbt != nil {
		G_async_queue_push(metadata_partial_queue, dbt)
	}
}

func new_stream_queue_element(dbt *DB_Table, filename string) *stream_queue_element {
	var sf = new(stream_queue_element)
	sf.dbt = dbt
	sf.filename = filename
	return sf
}

func get_stream_queue_length(queue *GAsyncQueue) int64 {
	return G_async_queue_length(queue)
}

func stream_queue_push(dbt *DB_Table, filename string) {
	var done = G_async_queue_new(BufferSize)
	G_async_queue_push(Stream_queue, new_stream_queue_element(dbt, filename))
	G_async_queue_pop(done)
	G_async_queue_unref(done)
	metadata_partial_queue_push(dbt)
}

func process_stream(data any) {
	defer stream_thread.Thread.Done()
	var f *os.File
	var buf []byte = make([]byte, STREAM_BUFFER_SIZE)
	var buflen int
	var total_size uint64
	var total_start_time = time.Now()
	var diff, total_diff float64
	var length int
	var datetime time.Time
	var err error
	for {
		task := G_async_queue_pop(Stream_queue)
		sf := task.(*stream_queue_element)
		if len(sf.filename) == 0 {
			break
		}
		var used_filemame = path.Base(sf.filename)
		length, _ = os.Stdout.WriteString("\n-- ")
		length, _ = os.Stdout.WriteString(used_filemame)
		length, _ = os.Stdout.WriteString(" ")
		total_size += 5
		total_size += uint64(len(used_filemame))
		used_filemame = ""
		if No_stream == false {
			f, err = os.Open(sf.filename)
			if err != nil {
				log.Errorf("File failed to open: %s", sf.filename)
			} else {
				if f == nil {
					log.Criticalf("File failed to open: %s. Reetrying", sf.filename)
					f, err = os.Open(sf.filename)
					if err != nil {
						log.Errorf("File failed to open: %s. Cancelling", sf.filename)
						os.Exit(EXIT_FAILURE)
					}
				}
				fd, _ := f.Stat()
				size := fd.Size()
				var c = fmt.Sprintf("%d", size)
				length, err = os.Stdout.WriteString(c)
				length, err = os.Stdout.WriteString("\n")
				total_size += uint64(len(c) + 1)
				c = ""
				var total_len uint
				var start_time = time.Now()
				reader := bufio.NewReader(f)
				buflen, err = reader.Read(buf)
				if buflen < STREAM_BUFFER_SIZE {
					buf = buf[:buflen]
				}
				for buflen > 0 {
					length, err = os.Stdout.Write(buf)
					total_len = total_len + uint(buflen)
					if length != buflen {
						log.Errorf("Stream failed during transmition of file: %s", sf.filename)
					}
					buf = make([]byte, STREAM_BUFFER_SIZE)
					buflen, err = reader.Read(buf)
					if err == io.EOF {
						break
					}
					if buflen < STREAM_BUFFER_SIZE {
						buf = buf[:buflen]
					}
				}
				datetime = time.Now()
				diff = datetime.Sub(start_time).Seconds()
				total_diff = datetime.Sub(total_start_time).Seconds()
				if diff > 0 {
					if total_diff != 0 {
						log.Infof("File %s transferred in %.2f seconds at %.2f MB/s | Global: %.2f MB/s",
							sf.filename, diff, float64(total_len)/1024/1024/diff, float64(total_size)/1024/1024/total_diff)
					} else {
						log.Infof("File %s transferred in %.2f seconds at %.2f MB/s | Global: %.2f MB/s",
							sf.filename, diff, float64(total_len)/1024/1024/diff, float64(total_size)/1024/1024)
					}
				} else {
					if total_diff != 0 {
						log.Infof("File %s transferred | Global: %.2f MB/s", sf.filename, float64(total_size)/1024/1024/total_diff)
					} else {
						log.Infof("File %s transferred | Global: %.2f MB/s", sf.filename, float64(total_size)/1024/1024)
					}
				}
				total_size += uint64(total_len)
				f.Close()
			}
		}
		if No_delete == false {
			os.Remove(sf.filename)
		}
		sf.filename = ""
		sf = nil
	}
	datetime = time.Now()
	total_diff = datetime.Sub(total_start_time).Seconds()
	if total_diff != 0 {
		log.Infof("All data transferred was %d at a rate of %.2f MB/s", total_size, float64(total_size)/1024/1024/total_diff)
	} else {
		log.Infof("All data transferred was %d at a rate of %.2f MB/s", total_size, float64(total_size)/1024/1024)
	}
	metadata_partial_writer_alive = false
	metadata_partial_queue_push(nil)
	metadata_partial_writer_thread.Thread.Wait()
	return
}

func send_initial_metadata() {
	G_async_queue_push(initial_metadata_queue, 1)
	G_async_queue_pop(initial_metadata_lock_queue)
}

func metadata_partial_writer(data any) {
	defer metadata_partial_writer_thread.Thread.Done()
	_ = data
	var dbt *DB_Table
	var dbt_list []*DB_Table
	var output *GString = G_string_sized_new(256)
	var i uint
	var filename string
	var err error
	for i = 0; i < NumThreads; i++ {
		G_async_queue_pop(initial_metadata_queue)
	}
	var task any
	task = G_async_queue_try_pop(metadata_partial_queue)
	for task != nil {
		dbt = task.(*DB_Table)
		dbt_list = append(dbt_list, dbt)
		task = G_async_queue_try_pop(metadata_partial_queue)
	}
	G_string_set_size(output, 0)
	for _, dbt = range dbt_list {
		print_dbt_on_metadata_gstring(dbt, output)
	}
	filename = make_partial_filename(0)
	err = os.WriteFile(filename, []byte(output.Str.String()), 0644)
	stream_queue_push(nil, filename)
	for i = 0; i < NumThreads; i++ {
		G_async_queue_push(initial_metadata_lock_queue, 1)
	}
	i = 1
	var prev_datetime = time.Now()
	var current_datetime time.Time
	var diff float64
	G_string_set_size(output, 0)
	filename = ""
	task = G_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL*1000000)
	if task == nil {
		dbt = nil
	} else {
		dbt = task.(*DB_Table)
	}
	for metadata_partial_writer_alive {
		if dbt != nil {
			f := slices.Contains(dbt_list, dbt)
			if !f {
				dbt_list = append(dbt_list, dbt)
			}
		}
		current_datetime = time.Now()
		diff = current_datetime.Sub(prev_datetime).Seconds()
		if diff > METADATA_PARTIAL_INTERVAL {
			if len(dbt_list) > 0 {
				filename = fmt.Sprintf("metadata.partial.%d", i)
				i++
				for _, dbt = range dbt_list {
					print_dbt_on_metadata_gstring(dbt, output)
					err = os.WriteFile(filename, []byte(output.Str.String()), 0644)
					stream_queue_push(nil, filename)
					filename = ""
					G_string_set_size(output, 0)
					dbt_list = nil
				}
			}
			prev_datetime = current_datetime
		}
		task = G_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL*1000000)
		if task == nil {
			dbt = nil
		} else {
			dbt = task.(*DB_Table)
		}
	}
	_ = err

}

func make_partial_filename(i uint) string {
	return fmt.Sprintf("metadata.partial.%d", i)

}

func initialize_stream() {
	initial_metadata_queue = G_async_queue_new(BufferSize)
	initial_metadata_lock_queue = G_async_queue_new(BufferSize)
	Stream_queue = G_async_queue_new(BufferSize)
	metadata_partial_queue = G_async_queue_new(BufferSize)
	stream_thread = G_thread_new("stream_thread", new(sync.WaitGroup), 0)
	metadata_partial_writer_thread = G_thread_new("metadata_partial_writer_thread", new(sync.WaitGroup), 0)
	metadata_partial_writer_alive = true
	go process_stream(nil)
	go metadata_partial_writer(nil)
}

func wait_stream_to_finish() {
	stream_thread.Thread.Wait()
}
