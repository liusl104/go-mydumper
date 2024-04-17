package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"testing"
)

func TestM_replace_char_with_char(t *testing.T) {
	data := `\'^%"`
	data = mysql.Escape(data)

	a := m_replace_char_with_char('\\', ':', []rune(data))
	t.Logf("%s", a)
}

func TestM_escape_char_with_char(t *testing.T) {
	data := []byte("a-dfa-dfa\n-d")
	t.Logf("%s", string(data))
	// data = m_escape_char_with_char([]byte(""\n"), '\t', data)
	t.Logf("%s", string(data))
}

func TestM_real_escape_string(t *testing.T) {
	data := "a-dfa-dfa\n-d"
	t.Logf("%s", string(data))
	data = m_real_escape_string(data)
	t.Logf("%s", data)
}
func TestFormat(t *testing.T) {
	n := fmt.Sprintf("%0*d", 6, 123)
	t.Logf(n)
}
