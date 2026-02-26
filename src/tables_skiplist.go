package mydumper

import (
	"bufio"
	"fmt"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"slices"
	"strings"
)

var tables_skiplist []string

// Read_tables_skiplist loads database.table entries from filename (one per line), sorts them, and increments *errors on open failure.
func Read_tables_skiplist(filename string, errors *int) {
	var err error
	var read_open *os.File
	read_open, err = os.Open(filename)
	if err != nil {
		*errors++
		return
	}
	defer read_open.Close()
	var tablesSkipListChannel = bufio.NewScanner(read_open)
	for tablesSkipListChannel.Scan() {
		line := strings.Trim(tablesSkipListChannel.Text(), "\n")
		tables_skiplist = append(tables_skiplist, line)
	}
	slices.Sort(tables_skiplist)
	log.Infof("Omit list file contains %d tables to skip", len(tables_skiplist))
	return
}

// Check_skiplist returns true if "database.table" is in the loaded skiplist (omit list).
func Check_skiplist(database string, table string) bool {
	var k = fmt.Sprintf("%s.%s", database, table)
	return slices.Contains(tables_skiplist, k)
}
