package mydumper

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/spf13/pflag"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// GString is a growable string buffer with explicit length (GLib-style).
type GString struct {
	Str *strings.Builder
	Len int
}

type GFileTest int

const (
	G_FILE_TEST_EXISTS     GFileTest = 1 << 0
	G_FILE_TEST_IS_REGULAR GFileTest = 1 << 1
	G_FILE_TEST_IS_DIR     GFileTest = 1 << 2
	G_FILE_TEST_IS_SYMLINK GFileTest = 1 << 3
)

// G_file_test returns true if the given path satisfies any of the test flags
// (like C g_file_test which ORs all flags). Default (no flags) checks existence.
func G_file_test(filename string, flags ...GFileTest) bool {
	info, err := os.Lstat(filename)
	if err != nil {
		return false
	}
	if len(flags) == 0 {
		return true
	}
	var flag GFileTest
	for _, f := range flags {
		flag |= f
	}
	if flag&G_FILE_TEST_IS_SYMLINK != 0 && info.Mode()&os.ModeSymlink != 0 {
		return true
	}
	info, err = os.Stat(filename)
	if err != nil {
		return false
	}
	if flag&G_FILE_TEST_IS_DIR != 0 && info.IsDir() {
		return true
	}
	if flag&G_FILE_TEST_IS_REGULAR != 0 && info.Mode().IsRegular() {
		return true
	}
	if flag&G_FILE_TEST_EXISTS != 0 {
		return true
	}
	return false
}

// g_atomic_int_dec_and_test decrements *a atomically and returns true if the result is 0.
func g_atomic_int_dec_and_test(a *int64) bool {
	return atomic.AddInt64(a, -1) == 0
}

// G_string_append_printf appends a formatted string to s and updates s.Len.
func G_string_append_printf(s *GString, msg string, args ...any) {
	s.Str.WriteString(fmt.Sprintf(msg, args...))
	s.Len = s.Str.Len()
}

// G_string_append appends str to s and updates s.Len.
func G_string_append(s *GString, str string) {
	s.Str.WriteString(str)
	s.Len = s.Str.Len()
}

// G_string_append_c appends a single byte to s and updates s.Len.
func G_string_append_c(s *GString, b byte) {
	s.Str.Write([]byte{b})
	s.Len = s.Str.Len()
}

// G_string_append_b appends bytes to s and updates s.Len.
func G_string_append_b(s *GString, b []byte) {
	s.Str.Write(b)
	s.Len = s.Str.Len()
}

// G_string_set_size resets s and optionally grows capacity to size (or clears if size is 0).
func G_string_set_size(s *GString, size int) {
	s.Str.Reset()
	if size > 0 {
		s.Str.Grow(size)
	}
	s.Len = 0
}

// G_string_assign replaces s contents with str and updates s.Len.
func G_string_assign(s *GString, str string) {
	s.Str.Reset()
	s.Str.WriteString(str)
	s.Len = s.Str.Len()
}

// G_string_printf resets s and writes a formatted string, then updates s.Len.
func G_string_printf(s *GString, msg string, args ...any) {
	s.Str.Reset()
	s.Str.WriteString(fmt.Sprintf(msg, args...))
	s.Len = s.Str.Len()
}

// G_string_new creates a new GString initialized with str.
func G_string_new(str string) *GString {
	var s = new(GString)
	s.Str = new(strings.Builder)
	s.Str.WriteString(str)
	s.Len = s.Str.Len()
	return s
}

// G_string_sized_new creates a new GString with pre-allocated capacity size.
func G_string_sized_new(size int) *GString {
	var s = new(GString)
	s.Str = new(strings.Builder)
	s.Str.Grow(size)
	return s
}

// G_string_free resets the string and optionally nils the pointer.
func G_string_free(str *GString, free bool) {
	str.Str.Reset()
	str.Len = 0
	if free {
		str = nil
	}
}

// G_key_file_get_value returns the string value for the given group and key from the ini file.
func G_key_file_get_value(kf *ini.File, group string, key string) string {
	return kf.Section(group).Key(key).String()
}

// G_ascii_strtoull parses s as an unsigned 64-bit decimal integer; returns 0 on error.
func G_ascii_strtoull(s string) uint64 {
	s = strings.TrimSpace(s)
	r, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return r
}

// G_get_current_dir returns the current working directory.
func G_get_current_dir() string {
	current_dir, _ := os.Getwd()
	return current_dir
}

// G_key_file_has_group returns true if the ini file has the given section/group.
func G_key_file_has_group(kf *ini.File, group string) bool {
	// Use ini.File HasSection to check if the specified group exists.
	return kf.HasSection(group)
}

// g_get_num_processors returns the number of CPUs available to the process.
func g_get_num_processors() uint {
	return uint(runtime.NumCPU())
}

// G_rec_mutex_new creates a new mutex. NOTE: unlike C GRecMutex, Go sync.Mutex
// is NOT recursive — re-locking from the same goroutine will deadlock.
// Current usage (ready_table_dump_mutex) does not re-enter, so this is safe.
func G_rec_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

// G_string_replace replaces occurrences of old with new in str; limit=0 means all.
func G_string_replace(str *GString, old, new string, limit ...int) {
	n := -1
	if len(limit) > 0 && limit[0] > 0 {
		n = limit[0]
	}
	t := strings.Replace(str.Str.String(), old, new, n)
	str.Str.Reset()
	str.Str.WriteString(t)
	str.Len = str.Str.Len()
}

// G_mutex_new creates a new mutex.
func G_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

// GThread holds a goroutine and its metadata (name, id, wait group).
type GThread struct {
	Thread        *sync.WaitGroup
	Func          func(any)
	Args          any
	Name          string
	Thread_id     int
	thread_number int
}

// ThreadFunc is the type of a function run by G_thread_new.
type ThreadFunc func(data any)

// G_thread_new starts a new goroutine that runs f(data) and returns a GThread to join later.
func G_thread_new(thread_name string, f func(any), data any, thread_id int) *GThread {
	var gtf = new(GThread)
	gtf.Thread = new(sync.WaitGroup)
	gtf.Name = thread_name
	gtf.Func = f
	gtf.Args = data
	gtf.Thread_id = thread_id
	gtf.Thread.Add(1)
	go func() {
		gtf.Func(data)
		gtf.Thread.Done()
	}()
	return gtf
}

// G_thread_join blocks until the thread's goroutine has finished.
func G_thread_join(t *GThread) {
	t.Thread.Wait()
}

// G_thread_unref is a no-op (thread cleanup if needed later).
func G_thread_unref(t *GThread) {
}

// G_hash_table_unref is a no-op for hash table cleanup (placeholder for C compatibility).
func G_hash_table_unref(h any) {
	h = nil
}

// G_atomic_int_dec_and_test decrements *a atomically and returns true if the result is 0.
func G_atomic_int_dec_and_test(a *int64) bool {
	return atomic.AddInt64(a, -1) == 0
}

// G_assert panics with "Assertion failed" if r is false.
func G_assert(r bool) {
	if !r {
		panic("Assertion failed")
	}
}

// G_option_context_parse appends clist to os.Args and runs pflag.Parse; returns true.
func G_option_context_parse(clist []string) bool {
	os.Args = append(os.Args, clist...)
	pflag.Parse()
	return true
}
