package mydumper

import (
	"fmt"
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
func G_string_append_c(s *GString, b []byte) {
	s.Str.Write(b)
	s.Len = s.Str.Len()
}
func G_string_set_size(s *GString, size int) {
	if size == 0 {
		s.Str.Reset()
		s.Len = 0
		return
	}
	t := s.Str.String()
	s.Str.Reset()
	s.Str.WriteString(t[:size])
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

type GThreadFunc struct {
	Thread    *sync.WaitGroup
	Name      string
	Thread_id uint
}

func G_thread_new(thread_name string, thread *sync.WaitGroup, thread_id uint) *GThreadFunc {
	var gtf = new(GThreadFunc)
	gtf.Thread = thread
	gtf.Name = thread_name
	gtf.Thread_id = thread_id
	gtf.Thread.Add(1)
	return gtf

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
