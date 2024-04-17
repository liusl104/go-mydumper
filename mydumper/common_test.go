package mydumper

import (
	"encoding/binary"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"math"
	"path"
	"testing"
)

func TestReplace_escaped_strings(t *testing.T) {
	msg := replace_escaped_strings("d\\n\\\tadf")
	t.Log(msg)
}

func TestStrcount(t *testing.T) {
	t.Log(strcount("adfadf\nadfa\n"))
}

func TestStr(t *testing.T) {
	data := "fadf\tadf"

	t.Log(escape_tab_with(data))
}

func TestRemove_definer_from_gchar(t *testing.T) {
	t.Log(remove_definer_from_gchar("Some text DEFINER=someone some other text"))
}

func TestMysqlRealEscapeString(t *testing.T) {
	msg := mysqlRealEscapeString("d\\n\\\tadf")
	t.Log(msg)
}

func TestMysql_get_server_version(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "mysql")
	if err != nil {
		t.Fail()
	}
	t.Log(mysql_get_server_version(conn))
	defer conn.Close()
}

func TestHide_password(t *testing.T) {
	hide_password(nil)
}

func TestBaseFile(t *testing.T) {
	filename := "/Users/liu/GolandProjects/go-mydumper/connection.go"
	basename := path.Base(filename)
	t.Log(basename)
}

func TestNumber(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "mysql")

	result, err := conn.Execute("select id from test2.t2")
	if err != nil {
		t.Fatalf("%v", err)
	}
	fmt.Println(result.FieldNames)
	//
	fields := result.Fields
	for _, row := range result.Values {
		for i, column := range row {

			flag := mysql.NUM_FLAG & int(fields[i].Flag) // int(fields[i].Flag)
			if intToBool(flag) {
				t.Log(column.AsInt64())
			}
		}

	}
}

func TestBinary(t *testing.T) {
	var p = []uint8{3, 100, 101, 102, 5, 116, 101, 115, 116, 50,
		2, 116, 50, 2, 116, 50, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0}
	t.Log(len(p))
	// v := binary.LittleEndian.Uint16(p[30:])
	for i := 0; i < len(p)-1; i++ {
		v := binary.LittleEndian.Uint16(p[i:])
		t.Log(v)
	}

}

func TestFilter_sequence_schemas(t *testing.T) {
	tt := "create table `test`.`t1` ()"
	t.Log(filter_sequence_schemas(tt))
}

func TestByte(t *testing.T) {
	// s := "abcd"
	var cresult []byte
	var cu2 byte = 48
	var cu1 byte = 90
	var part uint = 2
	abs := math.Abs(float64(cu2) - float64(cu1))
	cresult = append(cresult, cu2+uint8(int(abs)/int(part)))
	// data := uint8(48 + math.Abs(48-90)/2)

	t.Log(fmt.Sprintf("%s", cresult))

}

func TestG_key_file_has_group(t *testing.T) {
	kf := load_config_file("/Users/liu/GolandProjects/go-mydumper/cmd/config.ini")
	b := g_key_file_has_group(kf, "mydumper")
	if b {
		t.Log("ok")
	} else {
		t.Fail()
	}
}
