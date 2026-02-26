package mydumper

import (
	"fmt"
	"regexp"

	"github.com/spf13/pflag"
)

const filename_regex string = "^[\\w\\-_ ]+$"

var Regex_list []string
var re_list []*regexp.Regexp
var filename_re *regexp.Regexp
var partition_re *regexp.Regexp

// regex_arguments_callback appends the current Regex flag value to Regex_list. Called after flag parse.
func regex_arguments_callback() {
	Regex_list = append(Regex_list, Regex)
}

// Is_regex_being_used returns true if any regex was configured (Regex_list is not nil).
func Is_regex_being_used() bool {
	return Regex_list != nil
}

// regex_entries registers the --regex flag for db.table matching.
func regex_entries() {
	pflag.StringVarP(&Regex, "regex", "x", "", "Regular expression for 'db.table' matching")
}

// load_regex_entries calls regex_entries to register the regex flag.
func load_regex_entries() {
	regex_entries()
}

// Check_filename_regex returns true if word matches the filename regex (allowed characters).
func Check_filename_regex(word string) bool {
	return filename_re.MatchString(word)
}

// init_regex compiles str into *r; on failure calls M_critical. Idempotent if *r already set.
func init_regex(r **regexp.Regexp, str string) {
	var err error
	if *r == nil {
		*r, err = regexp.Compile(str)
		if *r == nil {
			M_critical("Regular expression fail: %s (%v)", str, err)
		}
	}
}

// InitializeRegex compiles filename_regex, all Regex_list patterns, and optionally partition_regex into package regexes.
func InitializeRegex(partition_regex string) {
	init_regex(&filename_re, filename_regex)
	for _, l := range Regex_list {
		var _re *regexp.Regexp
		init_regex(&_re, l)
		re_list = append(re_list, _re)
	}
	if partition_regex != "" {
		init_regex(&partition_re, partition_regex)
	}
}

// check_regex returns true if "database.table" matches the given regex.
func check_regex(tre *regexp.Regexp, _database_name string, _table_name string) bool {
	var p = fmt.Sprintf("%s.%s", _database_name, _table_name)
	return tre.MatchString(p)
}

// Eval_regex returns true if any of the configured db.table regexes match; if no regexes, returns true.
func Eval_regex(_database_name string, _table_name string) bool {
	if re_list != nil {
		var r bool
		for _, l := range re_list {
			r = check_regex(l, _database_name, _table_name)
		}
		return r
	}
	return true
}

// eval_pcre_regex returns true if word matches the regex p.
func eval_pcre_regex(p *regexp.Regexp, word string) bool {
	return p.MatchString(word)
}

// Eval_partition_regex returns true if partition_re is nil or word matches it; used for partition name filtering.
func Eval_partition_regex(word string) bool {
	if partition_re != nil {
		return eval_pcre_regex(partition_re, word)
	}
	return true
}

// Free_regex clears the filename_re regex (nil).
func Free_regex() {
	filename_re = nil
}
