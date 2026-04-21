package mydumper

import (
	"bufio"
	"fmt"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"slices"
	"strings"
	"sync"
)

var (
	tables_skiplist       []string
	tables_skiplist_mutex *sync.Mutex
)

// Read_tables_skiplist loads database.table entries from filename (one per line), sorts them, and increments *errors on open failure.
func Read_tables_skiplist(filename string, errors *int) {
	if tables_skiplist == nil {
		tables_skiplist = make([]string, 0)
		tables_skiplist_mutex = &sync.Mutex{}
	}
	var err error
	var read_open *os.File
	read_open, err = os.Open(filename)
	if err != nil {
		log.Criticalf("cannot read/open file %s, %v", filename, err)
		*errors++
		return
	}
	defer read_open.Close()
	var tablesSkipListChannel = bufio.NewScanner(read_open)
	for tablesSkipListChannel.Scan() {
		line := strings.TrimRight(tablesSkipListChannel.Text(), "\r\n")
		tables_skiplist = append(tables_skiplist, line)
	}
	slices.Sort(tables_skiplist)
	log.Infof("Omit list file contains %d tables to skip", len(tables_skiplist))
	return
}

// Check_skiplist returns true if database alone or "database.table" is in the loaded skiplist.
func Check_skiplist(database string, table string) bool {
	tables_skiplist_mutex.Lock()
	defer tables_skiplist_mutex.Unlock()
	b := slices.Contains(tables_skiplist, database)
	if table == "" || b {
		return b
	}
	var k = fmt.Sprintf("%s.%s", database, table)
	return slices.Contains(tables_skiplist, k)
}
