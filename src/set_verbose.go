package mydumper

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"time"
)

func set_format(isJson bool) {
	if isJson {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.SetFormatter(&log.JSONFormatter{
				TimestampFormat:   time.DateTime,
				DisableTimestamp:  false,
				DisableHTMLEscape: true,
				CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
					fileName := path.Base(frame.File)
					fileNameLine := fmt.Sprintf("%s:%d", fileName, frame.Line)
					return frame.Function, fileNameLine
				},
			})
		} else {
			log.SetFormatter(&log.JSONFormatter{
				TimestampFormat:   time.DateTime,
				DisableTimestamp:  false,
				DisableHTMLEscape: true,
			})

		}

	}
}

func Set_verbose() error {
	var err error
	if Logger == nil {
		if LogFile != "" {
			Log_output, err = os.OpenFile(LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Errorf("Could not open log file '%s' for writing: %v", LogFile, err)
				return err
			}
		} else {
			Log_output = os.Stdout
		}
	} else {
		Log_output = Logger
	}

	log.SetOutput(Log_output)
	switch Verbose {
	case 0:
		log.SetLevel(log.FatalLevel)
	case 1:
		log.SetLevel(log.ErrorLevel)
	case 2:
		log.SetLevel(log.WarnLevel)
	case 3:
		log.SetLevel(log.InfoLevel)
	case 4:
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.WarnLevel)
	}
	if log.IsLevelEnabled(log.DebugLevel) {
		Set_debug()
		log.SetFormatter(&log.TextFormatter{
			DisableColors:   true,
			FullTimestamp:   true,
			TimestampFormat: time.DateTime,
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				fileName := path.Base(frame.File)
				fileNameLine := fmt.Sprintf("%s:%d", fileName, frame.Line)
				return frame.Function, fileNameLine
			},
		})
	} else {
		log.SetFormatter(&log.TextFormatter{
			DisableColors:   true,
			FullTimestamp:   true,
			TimestampFormat: time.DateTime,
		})
	}

	return nil
}

func Set_debug() {
	Verbose = 4
	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(true)
}
