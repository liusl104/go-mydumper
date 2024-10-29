package mydumper

import (
	"fmt"
	"regexp"
)
import log "github.com/sirupsen/logrus"

var filename_regex = "^[\\w\\-_ ]+$"

func regex_arguments_callback(o *OptionEntries) {
	o.global.regex_list = append(o.global.regex_list, o.Regex.Regex)
}

func is_regex_being_used(o *OptionEntries) bool {
	if len(o.global.regex_list) > 0 {
		return true
	}
	return false
}

func check_filename_regex(o *OptionEntries, word string) bool {
	return o.global.filename_re.MatchString(word)
}

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
	var _re *regexp.Regexp
	for _, l := range o.global.regex_list {
		_re, err = init_regex(l)
		if err != nil {
			return err
		}
		o.global.re_list = append(o.global.re_list, _re)
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

func eval_regex(o *OptionEntries, _database_name, _table_name string) bool {
	if len(o.global.re_list) > 0 {
		var r = false
		for _, re := range o.global.re_list {
			r = check_regex(re, _database_name, _table_name)
		}
		return r
	}
	return true
}

func eval_pcre_regex(p *regexp.Regexp, word string) bool {
	return p.MatchString(word)
}
func eval_partition_regex(o *OptionEntries, word string) bool {
	if o.global.partition_re != nil {
		return o.global.partition_re.MatchString(word)
	}
	return true
}

func free_regex(o *OptionEntries) {
	o.global.filename_re = nil
}
