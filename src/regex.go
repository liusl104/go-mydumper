package mydumper

import (
	"fmt"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

const filename_regex string = "^[\\w\\-_ ]+$"

var Regex_list []string
var re_list []*regexp.Regexp
var filename_re *regexp.Regexp
var partition_re *regexp.Regexp

func regex_arguments_callback() {

}

func Is_regex_being_used() bool {
	return Regex_list != nil
}

func regex_entries() {
	// regex_entries
	pflag.StringVarP(&Regex, "regex", "x", "", "Regular expression for 'db.table' matching")
}

func Check_filename_regex(word string) bool {
	return filename_re.MatchString(word)
}

func init_regex(r **regexp.Regexp, str string) {
	// var Err error
	if *r == nil {
		*r = regexp.MustCompile(str)
		if *r == nil {
			log.Fatalf("Regular expression fail")
		}
	}
}

func InitializeRegex(partition_regex string) {
	for _, l := range Regex_list {
		var _re *regexp.Regexp
		init_regex(&_re, l)
		re_list = append(re_list, _re)
	}
	init_regex(&filename_re, filename_regex)
	if partition_re != nil {
		init_regex(&partition_re, partition_regex)
	}
}

func check_regex(tre *regexp.Regexp, _database_name string, _table_name string) bool {
	var p = fmt.Sprintf("%s.%s", _database_name, _table_name)
	return tre.MatchString(p)
}

func Eval_regex(_database_name string, _table_name string) bool {
	if re_list != nil {
		return true
	}
	return true
}
func eval_pcre_regex(p *regexp.Regexp, word string) bool {
	return p.MatchString(word)
}

func Eval_partition_regex(word string) bool {
	if partition_re != nil {
		return eval_pcre_regex(partition_re, word)
	}
	return true
}

func Free_regex() {
	filename_re = nil
}
