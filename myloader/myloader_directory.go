package myloader

import (
	"bufio"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"strings"
)

var (
	metadata_sync_queue *GAsyncQueue
)

// initialize_directory creates the metadata_sync_queue for directory processing.
func initialize_directory() {
	metadata_sync_queue = G_async_queue_new()
}

// wait_directory_to_process_metadata blocks until the directory thread pushes to metadata_sync_queue, then unreferences it.
func wait_directory_to_process_metadata() {
	G_async_queue_pop(metadata_sync_queue)
	G_async_queue_unref(metadata_sync_queue)
}

// process_directory checks for the metadata file, runs process_metadata_global, and pushes to metadata_sync_queue; fatal if metadata missing.
func process_directory(c any) {
	cnf := c.(*configuration)
	var err error
	var filename string
	var fileMode os.FileInfo
	fileMode, err = os.Stat("metadata")
	if err == nil && fileMode.Mode().IsRegular() {
		process_metadata_global("metadata")
		G_async_queue_push(metadata_sync_queue, 1)
		log.Infof("metadata pushed")
	} else {
		// Consistent with C: g_error is fatal; use log.Criticalf
		log.Criticalf("metadata file was not found")
	}
	if Resume {
		log.Info("Using resume file")
		var file *os.File
		file, err = os.OpenFile("resume", os.O_RDONLY, 0660)
		if err != nil {
			log.Fatalf("open resume file fail:%v", err)
		}
		var i int
		var data *GString = G_string_sized_new(256)
		var eof bool
		var line int
		reader := bufio.NewScanner(file)
		Read_data(reader, data, &eof, &line)
		var split []string
		for !eof {
			Read_data(reader, data, &eof, &line)
			split = strings.Split(data.Str.String(), "\n")
			for i = 0; i < len(split); i++ {
				if len(split[i]) > 2 {
					filename = split[i]
					intermediate_queue_new(filename)
				}
			}
			G_string_set_size(data, 0)
		}
		err = file.Close()
	} else {
		var dir []os.DirEntry
		dir, err = os.ReadDir(directory)
		if err != nil {
			log.Criticalf("fail read directory : %v", err)
		}
		for _, f := range dir {
			filename = f.Name()
			if strings.Compare(filename, "metadata") != 0 {
				intermediate_queue_new(filename)
			}
		}
	}
	intermediate_queue_end()
	var n uint = 0
	for n = 0; n < NumThreads; n++ {
		G_async_queue_push(cnf.data_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(cnf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(cnf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}
