package mydumper

import (
	log "github.com/sirupsen/logrus"
	. "go-mydumper/src"
	"strconv"
	"strings"
)

var (
	OutputDirectoryParam string
	ClearDumpDir         bool
	DirtyDumpDir         bool
	DaemonMode           bool
	PidFile              string
	SkipConstraints      bool
	SkipIndexes          bool
	shutdown_triggered   bool
	dump_directory       string
	output_directory     string
	errors               int
)

func parse_disk_limits() {
	strsplit := strings.SplitN(DiskLimits, ":", 3)
	if len(strsplit) != 2 {
		log.Fatalf("Parse limit failed")
	}
	p_at, err := strconv.Atoi(strsplit[0])
	if err != nil {
		log.Fatalf("Parse limit failed")
	}
	r_at, err := strconv.Atoi(strsplit[1])
	if err != nil {
		log.Fatalf("Parse limit failed")
	}
	set_disk_limits(uint(p_at), uint(r_at))
}

func CommandDump() error {
	load_contex_entries()
	if !DaemonMode {
		err := StartDump()
		if err != nil {
			return err
		}
	}
	if LogFile != "" {
		_ = Log_output.Close()
	}
	return nil
}
