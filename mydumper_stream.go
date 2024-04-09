package mydumper

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
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

type stream_queue_element struct {
	dbt      *db_table
	filename string
}

func metadata_partial_queue_push(o *OptionEntries, dbt *db_table) {
	if dbt != nil {
		o.global.metadata_partial_queue.push(dbt)
	}
}

func new_stream_queue_element(dbt *db_table, filename string) *stream_queue_element {
	var sf = new(stream_queue_element)
	sf.dbt = dbt
	sf.filename = filename
	return sf
}

func get_stream_queue_length(queue *asyncQueue) uint {
	return uint(queue.length)
}

func stream_queue_push(o *OptionEntries, dbt *db_table, filename string) {
	if dbt != nil {
		log.Infof("New stream file: %s for dbt: %s ", filename, dbt.table)
	} else {
		log.Infof("New stream file: %s with null dbt: ", filename)
	}
	o.global.stream_queue.push(new_stream_queue_element(dbt, filename))
	metadata_partial_queue_push(o, dbt)
}

func process_stream(o *OptionEntries, data any) {
	o.global.stream_thread.Add(1)
	defer o.global.stream_thread.Done()
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
		task := o.global.stream_queue.pop()
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
		if o.global.no_stream == false {
			f, err = os.Open(sf.filename)
			if err != nil {
				log.Errorf("File failed to open: %s", sf.filename)
			} else {
				if f == nil {
					log.Errorf("File failed to open: %s. Reetrying", sf.filename)
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
		if o.global.no_delete == false {
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
	o.global.metadata_partial_writer_alive = false
	metadata_partial_queue_push(o, nil)
	o.global.metadata_partial_writer_thread.Wait()
	return
}

func send_initial_metadata(o *OptionEntries) {
	o.global.initial_metadata_queue.push(1)
	o.global.initial_metadata_lock_queue.pop()
}

func metadata_partial_writer(o *OptionEntries, data any) {
	o.global.metadata_partial_writer_thread.Add(1)
	defer o.global.metadata_partial_writer_thread.Done()
	_ = data
	var dbt *db_table
	var dbt_list []*db_table
	var output string
	var i uint
	var filename string
	var err error
	for i = 0; i < o.Common.NumThreads; i++ {
		o.global.initial_metadata_queue.pop()
	}
	var task any
	task = o.global.metadata_partial_queue.try_pop()
	for task != nil {
		if task != nil {
			dbt = task.(*db_table)
			dbt_list = append(dbt_list, dbt)
			task = o.global.metadata_partial_queue.try_pop()
		}
	}
	for _, dbt = range dbt_list {
		print_dbt_on_metadata_gstring(dbt, &output)
	}
	filename = fmt.Sprintf("metadata.partial.%d", 0)
	err = os.WriteFile(filename, []byte(output), 0644)
	stream_queue_push(o, nil, filename)
	for i = 0; i < o.Common.NumThreads; i++ {
		o.global.initial_metadata_lock_queue.push(1)
	}
	i = 1
	var prev_datetime = time.Now()
	var current_datetime time.Time
	var diff float64
	output = ""
	filename = ""
	task = o.global.metadata_partial_queue.timeout_pop(METADATA_PARTIAL_INTERVAL * 1000000)
	if task == nil {
		dbt = nil
	} else {
		dbt = task.(*db_table)
	}
	for o.global.metadata_partial_writer_alive {
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
					print_dbt_on_metadata_gstring(dbt, &output)
					err = os.WriteFile(filename, []byte(output), 0644)
					stream_queue_push(o, nil, filename)
					filename = ""
					output = ""
					dbt_list = nil
				}
			}
			prev_datetime = current_datetime
		}
		task = o.global.metadata_partial_queue.timeout_pop(METADATA_PARTIAL_INTERVAL * 1000000)
		if task == nil {
			dbt = nil
		} else {
			dbt = task.(*db_table)
		}
	}
	_ = err

}

func initialize_stream(o *OptionEntries) {
	o.global.initial_metadata_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.initial_metadata_lock_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.stream_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.metadata_partial_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.stream_thread = new(sync.WaitGroup)
	o.global.metadata_partial_writer_thread = new(sync.WaitGroup)
	o.global.metadata_partial_writer_alive = true
	go process_stream(o, nil)
	go metadata_partial_writer(o, nil)
}

func wait_stream_to_finish(o *OptionEntries) {
	o.global.stream_thread.Wait()
}
