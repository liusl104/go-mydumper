package mydumper

import (
	"bufio"
	"fmt"
	log "go-mydumper/src/logrus"
	"os"
	"slices"
	"strings"
)

var tables_skiplist []string

func Read_tables_skiplist(filename string) error {
	var err error
	var read_open *os.File
	read_open, err = os.Open(filename)
	if err != nil {
		log.Critical("cannot read/open file %s, %v", filename, err)
		return err
	}
	defer read_open.Close()
	var tablesSkipListChannel = bufio.NewScanner(read_open)
	for tablesSkipListChannel.Scan() {
		line := strings.Trim(tablesSkipListChannel.Text(), "\n")
		tables_skiplist = append(tables_skiplist, line)
	}
	slices.Sort(tables_skiplist)
	log.Infof("Omit list file contains %d tables to skip", len(tables_skiplist))
	return nil
}

func Check_skiplist(database string, table string) bool {
	var k = fmt.Sprintf("%s.%s", database, table)
	return slices.Contains(tables_skiplist, k)
}
