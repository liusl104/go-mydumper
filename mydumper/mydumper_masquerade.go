package mydumper

import (
	"bufio"
	"os"

	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	file_hash                 map[string]map[string][]string
	pp                        *Function_pointer
	identity_function_pointer *Function_pointer = &Function_pointer{
		Fun_ptr: identity_function,
	}
)

// initialize_masquerade initializes file_hash for masquerade configuration.
func initialize_masquerade() {
	file_hash = make(map[string]map[string][]string)
}

// identity_function returns an empty FieldValue (no transformation).
func identity_function(str string) FieldValue {
	return FieldValue{}
}

// finalize_masquerade clears file_hash.
func finalize_masquerade() {
	file_hash = nil
}

// load_file_content reads filename and returns a map of key-value pairs; currently returns empty map (Read_data not filling file_content).
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
		if ok := Read_data(fileBuffer, data, &eof, &line); !ok {
			break
		}
	}
	return file_content
}

// init_function_pointer builds a Function_pointer from a value string; currently returns nil.
func init_function_pointer(value string) *Function_pointer {
	return nil
}
