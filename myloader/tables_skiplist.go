package myloader

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"slices"
	"strings"
)

func check_skiplist(o *OptionEntries, database string, table string) bool {
	var k = fmt.Sprintf("%s.%s", database, table)
	return slices.Contains(o.global.tables_skiplist, k)
}

func (o *OptionEntries) read_tables_skiplist(filename string) error {
	var err error
	var read_open *os.File
	read_open, err = os.Open(filename)
	if err != nil {
		log.Errorf("cannot read/open file %s, %v", filename, err)
		return err
	}
	var tablesSkipListChannel = bufio.NewScanner(read_open)
	for tablesSkipListChannel.Scan() {
		line := strings.Trim(tablesSkipListChannel.Text(), "\n")
		o.global.tables_skiplist = append(o.global.tables_skiplist, line)
	}
	defer read_open.Close()
	slices.Sort(o.global.tables_skiplist)
	log.Infof("Omit list file contains %d tables to skip", len(o.global.tables_skiplist))
	return nil
}
