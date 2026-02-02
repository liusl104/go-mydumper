package mydumper

import (
	"errors"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var Errors int

type MYSQL_RES struct {
	Result    *mysql.Result
	Rows      chan []mysql.FieldValue
	RecNumber int
}

type M_ROW struct {
	Res *MYSQL_RES
	Row []mysql.FieldValue
}

func (m *MYSQL_RES) Next() ([]mysql.FieldValue, bool) {
	val, ok := <-m.Rows
	return val, ok
}
func Mysql_fetch_row(m *MYSQL_RES) []mysql.FieldValue {
	val, ok := <-m.Rows
	if !ok {
		return nil
	}
	return val
}

func Mysql_fetch_fields(res *MYSQL_RES) []*mysql.Field {
	for {
		if res.RecNumber == 0 && !res.Result.StreamingDone {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		break

	}
	return res.Result.Fields
}

func Mysql_num_fields(res *MYSQL_RES) uint {
	for {
		if res.RecNumber == 0 && !res.Result.StreamingDone {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		break

	}
	return uint(res.Result.ColumnNumber())
}
func Mysql_error(conn *DBConnection) string {
	if conn.Err == nil {
		return ""
	}
	return conn.Err.Error()
}

func Mysql_errno(conn *DBConnection) int16 {
	return conn.Code
}

func Mysql_ping(conn *DBConnection) bool {
	if err := conn.Ping(); err != nil {
		return false
	}
	return true
}

func Mysql_free_result(m *MYSQL_RES) {
	if m == nil {
		return
	}
	if m.Result != nil {
		m.Result.Close()
		m.Result = nil
	}

}

func Mysql_real_query(conn *DBConnection, data string) *mysql.Result {
	conn.Result, conn.Err = conn.Conn.Execute(data)
	return conn.Result
}

func init_result() *MYSQL_RES {
	return &MYSQL_RES{
		Rows:      make(chan []mysql.FieldValue),
		RecNumber: 0,
		Result:    new(mysql.Result),
	}
}

// Mysql_store_result 一次性加载全部结果到客户端内存，适合小结果集，后续操作无需与服务器交互，速度快，但内存占用可能较高
func Mysql_store_result(conn *DBConnection) *MYSQL_RES {
	var res *mysql.Result
	var err error
	var r *MYSQL_RES = init_result()
	res, err = conn.Stmt.Execute()
	if err != nil {
		conn.Err = err
		conn.Code = -1
		return nil
	}
	r.Result = res
	go func() {
		for _, row := range res.Values {
			select {
			default:
				r.RecNumber++
				r.Rows <- row
			}
		}
		close(r.Rows)
	}()
	return r
}

// Mysql_use_result 流式获取结果（逐行从服务器读取），内存占用低，但需保持连接，且必须尽快处理（否则会阻塞服务器），适合大结果集
func Mysql_use_result(conn *DBConnection) *MYSQL_RES {
	var res = init_result()
	var err error
	go func(cn *DBConnection) {
		err = cn.Stmt.ExecuteSelectStreaming(res.Result, func(row []mysql.FieldValue) error {
			res.RecNumber++
			rowCopy := make([]mysql.FieldValue, len(row))
			// 这里是浅拷贝
			copy(rowCopy, row)
			// 清空数据，防止后续修改数据，主要原因是ExecuteSelectStreaming底层是通过指针传递的
			for i := 0; i < len(row); i++ {
				row[i] = mysql.FieldValue{}
			}
			res.Rows <- rowCopy
			return nil
		}, nil)
		// res.Result, err = conn.Stmt.Execute()
		if err != nil {
			cn.Err = err
			cn.Code = -1
			return
		}
		close(res.Rows)
		return
	}(conn)
	return res
}

func Mysql_warning_count(conn *DBConnection) int {
	res, _ := conn.Conn.Execute("SHOW WARNINGS")
	return len(res.Values)
}
func (d *DBConnection) Ping() error {
	d.Err = d.Conn.Ping()
	if d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		if myError == nil {
			return d.Err
		}
		d.Code = int16(myError.Code)
	} else {
		d.Code = 0
	}
	return d.Err
}

func Mysql_query_verbose(conn *DBConnection, q string) error {
	_ = conn.Execute(q)
	if conn.Err == nil {
		log.Infof("%s: OK", q)

	} else {
		log.Errorf("%s: %v", q, conn.Err)
	}
	return conn.Err
}

func Mysql_num_rows(r *MYSQL_RES) int {
	return r.Result.RowNumber()
}

func (d *DBConnection) UseDB(dbName string) bool {
	if d.Err = d.Conn.UseDB(dbName); d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		d.Code = int16(myError.Code)
		return false
	}
	d.Code = 0
	return true
}
func (d *DBConnection) GetConnectionID() uint32 {
	return d.Conn.GetConnectionID()
}

func (d *DBConnection) Close() error {
	return d.Conn.Close()
}

func (d *DBConnection) Execute(command string, args ...any) (result *mysql.Result) {
	// log.Debugf("Executing: %s", command)
	d.Result, d.Err = d.Conn.Execute(command, args...)
	if d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		d.Code = int16(myError.Code)
	} else {
		d.Code = 0
	}
	if d.Result != nil {
		d.Warning = d.Result.Warnings
	}
	return d.Result
}
func Mysql_get_server_version() uint {
	return uint(Get_major()*10000 + Get_secondary()*100 + Get_revision())
}

func M_store_result_row_free(mr *M_ROW) {
	mr.Res = nil
	mr.Row = nil
}
