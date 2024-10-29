package mydumper

import (
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

func parse_disk_limits(o *OptionEntries) {
	strsplit := strings.SplitN(o.CommonOptionEntries.DiskLimits, ":", 3)
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
	o.set_disk_limits(uint(p_at), uint(r_at))
}

func CommandDump() error {
	var context *OptionEntries
	context = newEntries()
	commandEntries(context)
	if !context.Daemon.DaemonMode {
		err := StartDump(context)
		if err != nil {
			return err
		}
	}
	if context.CommonOptionEntries.LogFile != "" {
		_ = context.global.log_output.Close()
	}
	return nil
}
