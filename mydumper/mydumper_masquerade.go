package mydumper

import (
	"bufio"
	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
)

var (
	file_hash                 map[string]map[string][]string
	pp                        *Function_pointer
	identity_function_pointer *Function_pointer = &Function_pointer{identity_function, false, "", nil, nil, nil, false, 0, 0, nil, false}
)

func initialize_masquerade() {
	file_hash = make(map[string]map[string][]string)
}
func identity_function(str string) mysql.FieldValue {
	return mysql.FieldValue{Type: mysql.BLOB_FLAG}
}

func finalize_masquerade() {
	file_hash = nil
}

func load_file_content(filename string) map[string]string {
	var file_content = make(map[string]string)
	var file *os.File
	var err error
	file, err = os.Open(filename)
	if err != nil {
		log.Criticalf("Couldn't open %s (%v)", filename, err)
		return file_content
	}
	var fileBuffer *bufio.Scanner
	fileBuffer = bufio.NewScanner(file)
	var data = G_string_sized_new(256)
	var eof bool
	var line int
	for !eof {
		Read_data(fileBuffer, data, &eof, &line)
	}
	return file_content
}

func init_function_pointer(value string) *Function_pointer {
	return nil
}
