package myloader

import (
	"fmt"
	"regexp"
)
import log "github.com/sirupsen/logrus"

var filename_regex = "^[\\w\\-_ ]+$"

/*
	func check_filename_regex(o *OptionEntries, word string) bool {
		return o.global.filename_re.MatchString(word)
	}
*/
func init_regex(rule string) (*regexp.Regexp, error) {
	var err error
	var r *regexp.Regexp
	r, err = regexp.Compile(rule)
	if err != nil {
		log.Errorf("Regular expression fail: %v", err)
		return nil, err
	}
	return r, nil
}

func initialize_regex(o *OptionEntries, partition_regex string) error {
	var err error
	if o.Regex.Regex != "" {
		o.global.re, err = init_regex(o.Regex.Regex)
		if err != nil {
			return err
		}
	}
	o.global.filename_re, err = init_regex(filename_regex)
	if err != nil {
		return err
	}
	if partition_regex != "" {
		o.global.partition_re, err = init_regex(partition_regex)
		if err != nil {
			return err
		}
	}
	return nil
}

/* check_regex Check database.table string against regular expression */
func check_regex(tre *regexp.Regexp, database string, table string) bool {
	var p = fmt.Sprintf("%s.%s", database, table)
	return tre.MatchString(p)
}

func eval_regex(o *OptionEntries, a, b string) bool {
	if o.global.re != nil {
		return check_regex(o.global.re, a, b)
	}
	return true
}

/*
	func eval_partition_regex(o *OptionEntries, word string) bool {
		if o.global.partition_re != nil {
			return o.global.partition_re.MatchString(word)
		}
		return true
	}
*/
func free_regex() {

}
