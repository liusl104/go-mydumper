package mydumper

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var Number []string = []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "BOOL"}
var Float []string = []string{"FLOAT", "DOUBLE", "DECIMAL"}

// MySQLTypeAliasMap maps MySQL type names to standard type names
var MySQLTypeAliasMap map[string]string = map[string]string{
	"TINYINT":   "MYSQL_TYPE_TINY",
	"SMALLINT":  "MYSQL_TYPE_SHORT",
	"MEDIUMINT": "MYSQL_TYPE_INT24",
	"INT":       "MYSQL_TYPE_LONG",
	"INTEGER":   "MYSQL_TYPE_LONG",
	"BIGINT":    "MYSQL_TYPE_LONGLONG",
	"DECIMAL":   "MYSQL_TYPE_DECIMAL",
	"NUMERIC":   "MYSQL_TYPE_DECIMAL",
	"FLOAT":     "MYSQL_TYPE_FLOAT",
	"DOUBLE":    "MYSQL_TYPE_DOUBLE",
	"REAL":      "MYSQL_TYPE_DOUBLE",
	"BIT":       "MYSQL_TYPE_BIT",
	// String types:
	"CHAR":       "MYSQL_TYPE_STRING",
	"VARCHAR":    "MYSQL_TYPE_VARCHAR",
	"TINYTEXT":   "MYSQL_TYPE_TINY_BLOB",
	"TEXT":       "MYSQL_TYPE_BLOB",
	"MEDIUMTEXT": "MYSQL_TYPE_MEDIUM_BLOB",
	"LONGTEXT":   "MYSQL_TYPE_LONG_BLOB",
	// Binary types:
	"BINARY":     "MYSQL_TYPE_STRING",
	"VARBINARY":  "MYSQL_TYPE_VARCHAR",
	"TINYBLOB":   "MYSQL_TYPE_TINY_BLOB",
	"BLOB":       "MYSQL_TYPE_BLOB",
	"MEDIUMBLOB": "MYSQL_TYPE_MEDIUM_BLOB",
	"LONGBLOB":   "MYSQL_TYPE_LONG_BLOB",
	// Date and time types:
	"DATE":      "MYSQL_TYPE_DATE",
	"TIME":      "MYSQL_TYPE_TIME",
	"DATETIME":  "MYSQL_TYPE_DATETIME",
	"TIMESTAMP": "MYSQL_TYPE_TIMESTAMP",
	"YEAR":      "MYSQL_TYPE_YEAR",
	// Other types:
	"ENUM":     "MYSQL_TYPE_ENUM",
	"SET":      "MYSQL_TYPE_SET",
	"JSON":     "MYSQL_TYPE_JSON",
	"GEOMETRY": "MYSQL_TYPE_GEOMETRY",
}

// GetStandardType converts input type string to standard type (e.g., INTEGER → INT)
func GetStandardType(typeStr string) string {
	// Convert to uppercase to avoid case sensitivity issues
	upperType := strings.ToUpper(typeStr)
	// Remove UNSIGNED from type string (e.g., "UNSIGNED BIGINT" or "INT UNSIGNED" → "BIGINT" or "INT")
	upperType = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(upperType, "UNSIGNED ", ""), " UNSIGNED", ""))
	// Look up in the mapping table, return original string if not found
	if standardType, ok := MySQLTypeAliasMap[upperType]; ok {
		return standardType
	}
	return typeStr
}

// IsNumber returns true if str is a numeric type name (e.g. TINYINT, or contains UNSIGNED).
func IsNumber(str string) bool {
	if strings.Contains(str, "UNSIGNED") {
		return true
	}
	if slices.Contains(Number, str) {
		return true
	}
	return false
}
func IsUnsigned(str string) bool {
	if strings.Contains(str, "UNSIGNED") {
		return true
	}
	return false
}

func IsFloat(str string) bool {
	return slices.Contains(Float, str)
}

// IsMySQLType checks if the input string matches the specified MySQL type
func IsMySQLType(typeStr string, mysqlType string) bool {
	return GetStandardType(typeStr) == mysqlType
}

// IsColumnUnsigned checks if sql.ColumnType is UNSIGNED type
// For go-sql-driver/mysql, UNSIGNED types' DatabaseTypeName() returns a string containing "UNSIGNED"
func IsColumnUnsigned(columnType *sql.ColumnType) bool {
	if columnType == nil {
		return false
	}
	typeName := columnType.DatabaseTypeName()
	return strings.Contains(strings.ToUpper(typeName), "UNSIGNED")
}

// Errors counts the number of errors encountered
var Errors int

// Result represents a query result with status, warnings, and affected rows
type Result struct {
	Status       uint16
	Warnings     uint16
	InsertId     uint64
	AffectedRows uint64
	*Resultset
}

// FieldValue represents a MySQL field value that can be either numeric or string
type FieldValue struct {
	value any
}

// Resultset represents a MySQL result set with rows, fields, and values
type Resultset struct {
	Rows       *sql.Rows
	Row        *sql.Row
	Fields     []*sql.ColumnType
	FieldNames map[string]int
	Values     [][]FieldValue
}

func (f *FieldValue) Length() int {
	return len(f.AsString())
}

// Value returns the raw value, returns nil if the value is NULL (both fields are empty)
func (f *FieldValue) Value() any {
	return f.value
}

// String returns string representation
func (f *FieldValue) String() string {
	if f == nil {
		return ""
	}
	switch f.value.(type) {
	case []uint8:
		return string(f.value.([]uint8))
	default:
		return fmt.Sprintf("%s", f.value)
	}
}

// AsUint64 returns uint64 value
// If str is not empty, attempts to parse string as uint64; otherwise returns value
func (f *FieldValue) AsUint64() uint64 {
	switch f.value.(type) {
	case uint64:
		return f.value.(uint64)
	case uint:
		return uint64(f.value.(uint))
	case uint8:
		return uint64(f.value.(uint8))
	case uint16:
		return uint64(f.value.(uint16))
	case uint32:
		return uint64(f.value.(uint32))
	case []byte:
		number, _ := strconv.ParseUint(string(f.value.([]byte)), 10, 64)
		return number
	}
	return 0
}

// AsString returns byte array representation
// If str is not empty, returns str; otherwise converts value to byte array of string
func (f *FieldValue) AsString() []byte {
	if f == nil {
		return nil
	}
	switch f.value.(type) {
	case []uint8:
		return f.value.([]uint8)
	}
	return nil
}

// Int64 returns int64 value
// If str is not empty, attempts to parse string as int64; otherwise converts value to int64 (note sign extension)
func (f *FieldValue) Int64() int64 {
	switch f.value.(type) {
	case int64:
		return f.value.(int64)
	case int:
		return int64(f.value.(int))
	case int8:
		return int64(f.value.(int8))
	case int16:
		return int64(f.value.(int16))
	case int32:
		return int64(f.value.(int32))
	case []byte:
		number, _ := strconv.ParseInt(string(f.value.([]byte)), 10, 64)
		return number

	}
	return 0
}

// AsInt64 returns int64 value (same as Int64)
func (f *FieldValue) AsInt64() int64 {
	return f.Int64()
}

// AsFloat64 returns the value as float64. float32 is rounded to 6 decimal places to strip
// spurious mantissa bits from the cast (e.g. 12345.67 as float32 → 12345.669921875 → 12345.669922).
// Uses math only (no string alloc) for performance in hot paths.
func (f *FieldValue) AsFloat64() float64 {
	if f == nil {
		return 0
	}
	switch f.value.(type) {
	case float64:
		return f.value.(float64)
	case float32:
		return float64(f.value.(float32))
	case []byte:
		v, e := strconv.ParseFloat(string(f.value.([]byte)), 64)
		if e == nil {
			return v
		}
	}
	return 0
}

// QueryRows closes any previous Rows, runs the query, and stores result in d.Rows; sets d.Err on error.
func (d *DBConnection) QueryRows(query string) {
	err := d.Rows.Close()
	if err != nil {
		log.Warnf("Error closing rows: %s", err.Error())
	}
	d.Rows, d.Err = d.Conn.Query(query)
	if d.Err != nil {
		d.RealError()
	}
}

// QueryRow runs a single-row query and returns a Result wrapping the row.
func (d *DBConnection) QueryRow(query string) *Result {
	var res = new(Result)
	res.Row = d.Conn.QueryRow(query)
	return res
}

// MYSQL_RES represents a MySQL result set structure
type MYSQL_RES struct {
	RowCount    uint64
	FieldValues [][]FieldValue
	Lengths     int64 // Column lengths of current row
	CurrentRow  int64
	Result      chan []FieldValue
	Fields      []*sql.ColumnType
	conn        *DBConnection // back-reference for use_result cleanup
	done        chan struct{} // signals use_result goroutine to stop
}

// M_ROW represents a MySQL row with its result set
type M_ROW struct {
	Res *MYSQL_RES
	Row []FieldValue
}

// Mysql_fetch_row fetches a row from the result set
// Supports two modes: store result (data in FieldValues) and stream result (data via Result channel)
func Mysql_fetch_row(m *MYSQL_RES) []FieldValue {
	if m == nil {
		return nil
	}
	// Store result mode: data pre-loaded in FieldValues by Mysql_store_result
	if m.CurrentRow < int64(len(m.FieldValues)) {
		row := m.FieldValues[m.CurrentRow]
		m.CurrentRow++
		return row
	}
	// Stream mode (Mysql_use_result): data delivered via Result channel
	if m.Result != nil {
		res, ok := <-m.Result
		if !ok || res == nil {
			return nil
		}
		m.CurrentRow++
		return res
	}
	return nil
}

// Mysql_fetch_fields returns the fields (columns) of the result set
func Mysql_fetch_fields(res *MYSQL_RES) []*sql.ColumnType {
	return res.Fields
}

// Mysql_num_fields returns the number of fields (columns) in the result set
func Mysql_num_fields(res *MYSQL_RES) uint {
	return uint(len(res.Fields))
}

// Mysql_error returns the error message from the connection
func Mysql_error(conn *DBConnection) string {
	if conn.Err == nil {
		return ""
	}
	return conn.Err.Error()
}

// Mysql_errno returns the MySQL error number for the most recent operation.
// Like C mysql_errno, returns 0 after a successful operation.
func Mysql_errno(conn *DBConnection) uint16 {
	if conn.Err == nil {
		conn.Code = 0
		return 0
	}
	if conn.Code == 0 {
		var myErr *mysql.MySQLError
		if errors.As(conn.Err, &myErr) {
			conn.Code = myErr.Number
			conn.Message = myErr.Message
		} else {
			conn.Code = 2000 // CR_UNKNOWN_ERROR
			conn.Message = conn.Err.Error()
		}
	}
	return conn.Code
}

// Mysql_ping pings the database connection
func Mysql_ping(conn *DBConnection) bool {
	if err := conn.Conn.Ping(); err != nil {
		conn.Err = err
		return false
	}
	return true
}

// Mysql_free_result frees the result set. For use_result, it signals the
// producer goroutine to stop, drains the channel, and releases conn.Rows.
// For store_result, conn.Rows is already closed by Mysql_store_result so
// this only clears the in-memory data.
func Mysql_free_result(m *MYSQL_RES) {
	if m == nil {
		return
	}
	// use_result path: stop the producer goroutine
	if m.done != nil {
		select {
		case <-m.done:
			// already closed
		default:
			close(m.done)
		}
		// drain any pending values so the goroutine can exit
		if m.Result != nil {
			for range m.Result {
			}
		}
		if m.conn != nil && m.conn.Rows != nil {
			m.conn.Rows.Close()
			m.conn.Rows = nil
		}
	}
	m.FieldValues = nil
	m.Result = nil
	m.conn = nil
}

// Mysql_real_query executes a query via Exec (no result set).
func Mysql_real_query(conn *DBConnection, query string) (result sql.Result) {
	if conn.Rows != nil {
		_ = conn.Rows.Close()
		conn.Rows = nil
	}
	result, conn.Err = conn.Conn.Exec(query)
	conn.RealError()
	return
}

// init_result initializes a new MYSQL_RES structure.
func init_result() *MYSQL_RES {
	return &MYSQL_RES{
		Result: make(chan []FieldValue),
	}
}

// Mysql_store_result loads all results into client memory at once.
// conn.Rows is always closed (both on success and error) so the single
// pool connection (MaxOpenConns=1) is available for the next query.
func Mysql_store_result(conn *DBConnection) *MYSQL_RES {
	if conn == nil || conn.Rows == nil {
		return nil
	}

	// Ensure conn.Rows is closed on every exit path.
	var success bool
	defer func() {
		if !success && conn.Rows != nil {
			conn.Rows.Close()
			conn.Rows = nil
		}
	}()

	var r *MYSQL_RES = init_result()

	columnTypes, err := conn.Rows.ColumnTypes()
	if err != nil {
		conn.Err = err
		return nil
	}

	r.Fields = columnTypes
	r.Lengths = int64(len(columnTypes))
	r.FieldValues = make([][]FieldValue, 0)

	n := int(r.Lengths)
	row := make([]any, n)
	scanArgs := make([]any, n)
	for i := range row {
		scanArgs[i] = &row[i]
	}

	for conn.Rows.Next() {
		conn.Err = conn.Rows.Scan(scanArgs...)
		if conn.Err != nil {
			return nil
		}

		fieldValues := make([]FieldValue, n)
		for i, val := range row {
			fieldValues[i] = FieldValue{value: val}
		}

		r.FieldValues = append(r.FieldValues, fieldValues)
		r.RowCount++
	}

	if conn.Err = conn.Rows.Err(); conn.Err != nil {
		return nil
	}

	// Normal completion: close Rows and mark success so defer skips.
	conn.Rows.Close()
	conn.Rows = nil
	success = true

	r.Result = nil
	return r
}

// Mysql_use_result streams results (reads row by row from server)
// Low memory usage, but requires maintaining connection and must be processed quickly
// (otherwise blocks server), suitable for large result sets.
// The caller MUST call Mysql_free_result when done to stop the producer goroutine
// and release conn.Rows.
func Mysql_use_result(conn *DBConnection) *MYSQL_RES {
	if conn == nil || conn.Rows == nil {
		return nil
	}

	var r *MYSQL_RES = init_result()
	r.Result = make(chan []FieldValue)
	r.conn = conn
	r.done = make(chan struct{})

	// Get column type information
	columnTypes, err := conn.Rows.ColumnTypes()
	if err != nil {
		conn.Err = err
		return nil
	}

	// Initialize Fields and Lengths
	r.Fields = columnTypes
	r.Lengths = int64(len(columnTypes))
	r.FieldValues = make([][]FieldValue, 0)
	go func() {
		// Pre-allocate scan buffer; scan into [][]byte so all columns come back as []byte (or nil for NULL).
		n := int(r.Lengths)
		row := make([]any, n)
		scanArgs := make([]any, n)
		for i := range row {
			scanArgs[i] = &row[i]
		}
		defer close(r.Result)
		defer func() {
			if conn.Rows != nil {
				conn.Rows.Close()
				conn.Rows = nil
			}
		}()
		for conn.Rows.Next() {
			conn.Err = conn.Rows.Scan(scanArgs...)
			if conn.Err != nil {
				return
			}
			fieldValues := make([]FieldValue, n)
			for i, val := range row {
				fieldValues[i] = FieldValue{value: val}
			}
			r.RowCount++
			select {
			case r.Result <- fieldValues:
			case <-r.done:
				return
			}
		}
	}()

	return r
}

// Mysql_warning_count returns the number of warnings from the last query.
// Closes any existing conn.Rows before running SHOW WARNINGS, and cleans up after.
func Mysql_warning_count(conn *DBConnection) int {
	if conn.Rows != nil {
		_ = conn.Rows.Close()
		conn.Rows = nil
	}
	conn.Rows, conn.Err = conn.Conn.Query("SHOW WARNINGS")
	if conn.Err != nil {
		return 0
	}
	count := 0
	for conn.Rows.Next() {
		count++
	}
	conn.Rows.Close()
	conn.Rows = nil
	return count
}

// MySQLQuery executes a query and returns true if there was an error
func (d *DBConnection) MySQLQuery(sql string) bool {
	if d.Rows != nil {
		err := d.Rows.Close()
		if err != nil {
			log.Warnf("MySQLQuery: Error closing connection: %s", err.Error())
		}
		d.Rows = nil
	}

	d.Rows, d.Err = d.Conn.Query(sql)
	if d.Err != nil {
		d.RealError()
		return true
	}
	return false
}

// Ping checks the database connection
func (d *DBConnection) Ping() error {
	if d.Rows != nil {
		_ = d.Rows.Close()
		d.Rows = nil
	}
	d.Err = d.Conn.Ping()
	if d.Err != nil {
		d.RealError()
		return d.Err
	}
	return d.Err
}

// Mysql_query_verbose executes a query with verbose logging
func Mysql_query_verbose(conn *DBConnection, q string) error {
	_, conn.Err = conn.Conn.Exec(q)
	if conn.Err == nil {
		log.Infof("%s: OK", q)
	} else {
		log.Errorf("%s: %v", q, conn.Err)
	}
	return conn.Err
}

// Mysql_num_rows returns the number of rows in the result set
func Mysql_num_rows(r *MYSQL_RES) uint64 {
	return r.RowCount
}

// RealError extracts MySQL error number and message from d.Err into d.Code and d.Message.
// Safe for non-MySQL errors (e.g. net.OpError) — uses CR_UNKNOWN_ERROR (2000).
func (d *DBConnection) RealError() {
	if d.Err == nil {
		d.Code = 0
		d.Message = ""
		return
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(d.Err, &mysqlErr) {
		d.Code = mysqlErr.Number
		d.Message = mysqlErr.Message
	} else {
		d.Code = 2000 // CR_UNKNOWN_ERROR
		d.Message = d.Err.Error()
	}
}

// UseDB executes USE dbName and returns false on error.
func (d *DBConnection) UseDB(dbName string) bool {
	if d.Rows != nil {
		_ = d.Rows.Close()
		d.Rows = nil
	}
	_, d.Err = d.Conn.Exec("USE " + dbName)
	d.RealError()
	if d.Err != nil {
		return false
	}
	return true
}

// GetConnectionID returns the connection ID stored when the connection was established.
func (d *DBConnection) GetConnectionID() uint32 {
	return d.connID
}

// Close closes Rows if open and then closes the database connection.
func (d *DBConnection) Close() error {
	if d.Rows != nil {
		d.Rows.Close()
	}
	return d.Conn.Close()
}

// Escape escapes special characters in a string for MySQL
func Escape(b string) string {
	var buf strings.Builder
	buf.Grow(len(b) * 2)
	for _, r := range b {
		switch r {
		case 0:
			buf.WriteString("\\0")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\\':
			buf.WriteString("\\\\")
		case '\'':
			buf.WriteString("\\'")
		case '"':
			buf.WriteString("\\\"")
		case '\032':
			buf.WriteString("\\Z")
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

// Mysql_get_server_version returns the MySQL server version as a uint
func Mysql_get_server_version() uint {
	return uint(Get_major()*10000 + Get_secondary()*100 + Get_revision())
}

// M_store_result_row_free frees the result set and clears the M_ROW.
// Matches C: mysql_free_result(mr->res); g_free(mr);
func M_store_result_row_free(mr *M_ROW) {
	Mysql_free_result(mr.Res)
	mr.Res = nil
	mr.Row = nil
}
