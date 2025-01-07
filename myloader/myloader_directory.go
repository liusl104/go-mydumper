package myloader

import (
	"bufio"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os"
	"strings"
)

func process_directory(conf *configuration) {
	var err error
	var filename string
	if Resume {
		log.Info("Using resume file")
		var file *os.File
		file, err = os.OpenFile("resume", os.O_RDONLY, 0660)
		if err != nil {
			log.Fatalf("open resume file fail:%v", err)
		}
		var i int
		var data *GString = new(GString)
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
			data = nil
		}
		err = file.Close()
	} else {
		var fileInfo os.FileInfo
		if fileInfo, err = os.Stat("metadata"); err == nil {
			if fileInfo.Mode().IsRegular() {
				process_metadata_global("metadata")
			}
		}
		var dir []os.DirEntry
		dir, err = os.ReadDir(directory)
		if err != nil {
			log.Fatalf("fail read directory : %v", err)
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
		G_async_queue_push(conf.data_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(conf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		G_async_queue_push(conf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}
