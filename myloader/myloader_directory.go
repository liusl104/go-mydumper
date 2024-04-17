package myloader

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

func process_directory(o *OptionEntries, conf *configuration) {
	var err error
	var filename string
	if o.Common.Resume {
		log.Info("Using resume file")
		var file *os.File
		file, err = os.Open("resume")
		if err != nil {
			log.Fatalf("open resume file fail:%v", err)
		}
		var i int
		var data string
		var eof bool
		var line uint
		reader := bufio.NewReader(file)
		read_data(reader, &data, &eof, &line)
		var split []string
		for !eof {
			read_data(reader, &data, &eof, &line)
			split = strings.Split(data, "\n")
			for i = 0; i < len(split); i++ {
				if len(split[i]) > 2 {
					filename = split[i]
					log.Debugf("Resuming file: %s", filename)
					intermediate_queue_new(o, filename)
				}
			}
		}
		err = file.Close()
	} else {
		var dir []os.DirEntry
		dir, err = os.ReadDir(o.global.directory)
		if err != nil {
			log.Fatalf("fail read directory : %v", err)
		}
		for _, f := range dir {
			filename = f.Name()
			intermediate_queue_new(o, filename)
		}
	}
	intermediate_queue_end(o)
	var n uint = 0
	for n = 0; n < o.Common.NumThreads; n++ {
		conf.data_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
		conf.post_table_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
		conf.view_queue.push(new_job(JOB_SHUTDOWN, nil, ""))
	}
}
