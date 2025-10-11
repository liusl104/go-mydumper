package mydumper

import (
	"fmt"
	"github.com/spf13/pflag"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-ini/ini"
)

type GString struct {
	Str *strings.Builder
	Len int
}

func G_file_test(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	return false
}

func g_atomic_int_dec_and_test(a *int64) bool {
	atomic.AddInt64(a, -1)
	if *a <= 0 {
		return true
	}
	return false

}

func G_string_append_printf(s *GString, msg string, args ...any) {
	s.Str.WriteString(fmt.Sprintf(msg, args...))
	s.Len = s.Str.Len()
}
func G_string_append(s *GString, str string) {
	s.Str.WriteString(str)
	s.Len = s.Str.Len()
}
func G_string_append_c(s *GString, b byte) {
	s.Str.Write([]byte{b})
	s.Len = s.Str.Len()
}
func G_string_append_b(s *GString, b []byte) {
	s.Str.Write(b)
	s.Len = s.Str.Len()
}
func G_string_set_size(s *GString, size int) {
	if size == 0 {
		s.Str.Reset()
		s.Len = 0
		return
	}
	s.Str.Reset()
	s.Str.Grow(size)
	s.Len = size
}

func G_string_assign(s *GString, str string) {
	s.Str.Reset()
	s.Str.WriteString(str)
	s.Len = s.Str.Len()
}

func G_string_printf(s *GString, msg string, args ...any) {
	s.Str.Reset()
	s.Str.WriteString(fmt.Sprintf(msg, args...))
	s.Len = s.Str.Len()

}

func G_string_new(str string, args ...any) *GString {
	var s = new(GString)
	s.Str = new(strings.Builder)
	s.Str.WriteString(fmt.Sprintf(str, args...))
	s.Len = s.Str.Len()
	return s
}
func G_string_sized_new(size int) *GString {
	var s = new(GString)
	s.Str = new(strings.Builder)
	s.Str.Grow(size)
	return s
}
func G_string_free(str *GString, free bool) {
	str.Str.Reset()
	str.Len = 0
	if free {
		str = nil
	}
}

func G_key_file_get_value(kf *ini.File, group string, key string) string {
	return kf.Section(group).Key(key).String()
}

func G_ascii_strtoull(s string) int {
	r, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return r
}

func G_get_current_dir() string {
	current_dir, _ := os.Getwd()
	return current_dir
}

func G_key_file_has_group(kf *ini.File, group string) bool {
	// 使用 ini.File 的 HasSection 方法检查是否存在指定组。
	return kf.HasSection(group)
}

func g_get_num_processors() uint {
	return uint(runtime.NumCPU())
}

func G_rec_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

func G_string_replace(str *GString, old, new string) {
	t := strings.Replace(str.Str.String(), old, new, -1)
	str.Str.Reset()
	str.Str.WriteString(t)
	str.Len = str.Str.Len()
}

func G_mutex_new() *sync.Mutex {
	return new(sync.Mutex)
}

type GThread struct {
	Thread        *sync.WaitGroup
	Func          func(any)
	Args          any
	Name          string
	Thread_id     int
	thread_number int
}
type ThreadFunc func(data any)

func G_thread_new(thread_name string, f func(any), data any, thread_id int) *GThread {
	var gtf = new(GThread)
	gtf.Thread = new(sync.WaitGroup)
	gtf.Name = thread_name
	gtf.Func = f
	gtf.Args = data
	gtf.Thread_id = thread_id
	if thread_id >= 0 {
		gtf.thread_number = 1
		gtf.Thread.Add(1)
	}
	go func() {
		defer gtf.Thread.Done()
		gtf.Func(data)
	}()
	return gtf
}
func G_thread_join(t *GThread) {
	t.Thread.Wait()
}

func G_thread_unref(t *GThread) {

}
func G_hash_table_unref(h any) {
	h = nil
}
func G_atomic_int_dec_and_test(a *int64) bool {
	atomic.AddInt64(a, -1)
	if *a == 0 {
		return true
	}
	return false

}

func G_assert(r bool) {
	if !r {
		panic("Assertion failed")
	}
}

func G_option_context_parse(clist []string) bool {
	os.Args = append(os.Args, clist...)
	pflag.Parse()
	return true
}
