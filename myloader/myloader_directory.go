package myloader

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

func (o *OptionEntries) process_directory(conf *configuration) {
	var err error
	var filename string
	if o.Resume {
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
		read_data(reader, data, &eof, &line)
		var split []string
		for !eof {
			read_data(reader, data, &eof, &line)
			split = strings.Split(data.str, "\n")
			for i = 0; i < len(split); i++ {
				if len(split[i]) > 2 {
					filename = split[i]
					o.intermediate_queue_new(filename)
				}
			}
			data = nil
		}
		err = file.Close()
	} else {
		var fileInfo os.FileInfo
		if fileInfo, err = os.Stat("metadata"); err == nil {
			if fileInfo.Mode().IsRegular() {
				o.process_metadata_global("metadata")
			}
		}
		var dir []os.DirEntry
		dir, err = os.ReadDir(o.global.directory)
		if err != nil {
			log.Fatalf("fail read directory : %v", err)
		}
		for _, f := range dir {
			filename = f.Name()
			if strings.Compare(filename, "metadata") != 0 {
				o.intermediate_queue_new(filename)
			}
		}
	}
	o.intermediate_queue_end()
	var n uint = 0
	for n = 0; n < o.NumThreads; n++ {
		g_async_queue_push(conf.data_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		g_async_queue_push(conf.post_table_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
		g_async_queue_push(conf.view_queue, new_control_job(JOB_SHUTDOWN, nil, nil))
	}
}
