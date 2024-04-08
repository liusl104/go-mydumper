package mydumper

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"os"
	"strconv"
	"strings"
	"time"
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
	CommandEntries(context)
	if context.CommonOptionEntries.Help {
		pflag.PrintDefaults()
		os.Exit(EXIT_SUCCESS)
	}
	if context.Common.ProgramVersion {
		print_version(MYDUMPER)
		os.Exit(EXIT_SUCCESS)
	}
	if context.Common.Debug {
		set_debug(context)
		context.set_verbose()
	} else {
		context.set_verbose()
	}
	log.Infof("MyDumper backup version: %s", VERSION)
	initialize_common_options(context, MYDUMPER)
	hide_password(context)
	ask_password(context)
	if context.CommonOptionEntries.Output_directory_param == "" {
		datetimestr := time.Now().Format("20060102-150405")
		context.global.output_directory = fmt.Sprintf("%s-%s", DIRECTORY, datetimestr)
	} else {
		context.global.output_directory = context.CommonOptionEntries.Output_directory_param
	}
	create_backup_dir(context.global.output_directory)
	if context.CommonOptionEntries.DiskLimits != "" {
		parse_disk_limits(context)
	}
	if context.Daemon.DaemonMode {
		initialize_daemon_thread()
		run_daemon()
	} else {
		context.global.dump_directory = context.global.output_directory
		start_dump(context)
	}
	if context.CommonOptionEntries.LogFile != "" {
		context.global.log_output.Close()
	}

	return nil
}
