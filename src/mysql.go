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

var Number []string = []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"}

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
	value uint64 // Also for int64 and float64
	str   []byte
}

// Resultset represents a MySQL result set with rows, fields, and values
type Resultset struct {
	Rows       *sql.Rows
	Row        *sql.Row
	Fields     []*sql.ColumnType
	FieldNames map[string]int
	Values     [][]FieldValue
}

// Value returns the raw value, returns nil if the value is NULL (both fields are empty)
func (f *FieldValue) Value() any {
	if f == nil {
		return nil
	}
	// If str is not empty, return string
	if len(f.str) > 0 {
		return string(f.str)
	}
	// If value is not 0, return numeric value
	if f.value != 0 {
		return f.value
	}
	// Check if it's really NULL (both fields are empty)
	if len(f.str) == 0 && f.value == 0 {
		return nil
	}
	return f.value
}

// String returns string representation
func (f *FieldValue) String() string {
	if f == nil {
		return ""
	}
	if len(f.str) > 0 {
		return string(f.str)
	}
	if f.value != 0 {
		return fmt.Sprintf("%d", f.value)
	}
	return ""
}

// AsUint64 returns uint64 value
// If str is not empty, attempts to parse string as uint64; otherwise returns value
func (f *FieldValue) AsUint64() uint64 {
	if f == nil {
		return 0
	}
	if len(f.str) > 0 {
		// Attempt to parse string as uint64
		val, err := strconv.ParseUint(string(f.str), 10, 64)
		if err != nil {
			return 0
		}
		return val
	}
	return f.value
}

// AsString returns byte array representation
// If str is not empty, returns str; otherwise converts value to byte array of string
func (f *FieldValue) AsString() []byte {
	if f == nil {
		return nil
	}
	if len(f.str) > 0 {
		return f.str
	}
	return []byte(fmt.Sprintf("%d", f.value))
}

// Int64 returns int64 value
// If str is not empty, attempts to parse string as int64; otherwise converts value to int64 (note sign extension)
func (f *FieldValue) Int64() int64 {
	if f == nil {
		return 0
	}
	if len(f.str) > 0 {
		// Attempt to parse string as int64
		val, err := strconv.ParseInt(string(f.str), 10, 64)
		if err != nil {
			return 0
		}
		return val
	}
	// Convert uint64 to int64 (direct conversion, as value may store signed integer)
	return int64(f.value)
}

// AsInt64 returns int64 value (same as Int64)
func (f *FieldValue) AsInt64() int64 {
	return f.Int64()
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

// Mysql_errno returns the MySQL error number from the connection
func Mysql_errno(conn *DBConnection) uint16 {
	if conn.Code == 0 && conn.Err != nil {
		var myErr *mysql.MySQLError
		errors.As(conn.Err, &myErr)
		conn.Code = myErr.Number
		conn.Message = myErr.Message
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

// Mysql_free_result frees the result set
func Mysql_free_result(m *MYSQL_RES) {
	if m == nil {
		return
	}

}

// Mysql_real_query executes a query and stores the result
func Mysql_real_query(conn *DBConnection, query string) (result sql.Result) {
	if conn.Rows != nil {
		_ = conn.Rows.Close()
	}
	result, conn.Err = conn.Conn.Exec(query)
	return
}

// init_result initializes a new MYSQL_RES structure
func init_result(tp *sql.Rows) *MYSQL_RES {
	return &MYSQL_RES{
		Result: make(chan []FieldValue),
	}
}

// convertToFieldValue converts any type value to FieldValue
func convertToFieldValue(val any) FieldValue {
	if val == nil {
		return FieldValue{}
	}
	switch v := val.(type) {
	case []byte:
		return FieldValue{str: v}
	case string:
		return FieldValue{str: []byte(v)}
	case int64:
		return FieldValue{value: uint64(v)}
	case int32:
		return FieldValue{value: uint64(v)}
	case int16:
		return FieldValue{value: uint64(v)}
	case int8:
		return FieldValue{value: uint64(v)}
	case int:
		return FieldValue{value: uint64(v)}
	case uint64:
		return FieldValue{value: v}
	case uint32:
		return FieldValue{value: uint64(v)}
	case uint16:
		return FieldValue{value: uint64(v)}
	case uint8:
		return FieldValue{value: uint64(v)}
	case uint:
		return FieldValue{value: uint64(v)}
	case float64:
		return FieldValue{value: uint64(v)}
	case float32:
		return FieldValue{value: uint64(v)}
	default:
		// For other types, convert to string
		return FieldValue{str: []byte(fmt.Sprintf("%v", v))}
	}
}

// Mysql_store_result loads all results into client memory at once
// Suitable for small result sets, subsequent operations don't need server interaction,
// fast but may have higher memory usage
func Mysql_store_result(conn *DBConnection) *MYSQL_RES {
	// Check if conn and Rows are nil
	if conn == nil || conn.Rows == nil {
		return nil
	}

	var r *MYSQL_RES = init_result(conn.Rows)

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

	// Pre-allocate scan buffer
	row := make([]any, r.Lengths)
	scanArgs := make([]any, r.Lengths)
	for i := range row {
		scanArgs[i] = &row[i]
	}

	// Iterate through all rows
	for conn.Rows.Next() {
		// Scan current row
		conn.Err = conn.Rows.Scan(scanArgs...)
		if conn.Err != nil {
			return nil
		}

		// Convert scan results to FieldValue slice
		fieldValues := make([]FieldValue, r.Lengths)
		for i, val := range row {
			fieldValues[i] = convertToFieldValue(val)
		}

		// Add to result set
		r.FieldValues = append(r.FieldValues, fieldValues)
		r.RowCount++
	}

	// Check for errors during iteration
	if conn.Err = conn.Rows.Err(); conn.Err != nil {
		return nil
	}

	// Store result mode uses FieldValues only; nil the channel to avoid blocking in Mysql_fetch_row
	r.Result = nil
	return r
}

// Mysql_use_result streams results (reads row by row from server)
// Low memory usage, but requires maintaining connection and must be processed quickly
// (otherwise blocks server), suitable for large result sets
func Mysql_use_result(conn *DBConnection) *MYSQL_RES {
	if conn == nil || conn.Rows == nil {
		return nil
	}

	var r *MYSQL_RES = init_result(conn.Rows)
	r.Result = make(chan []FieldValue)

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
		// Pre-allocate scan buffer
		row := make([]any, r.Lengths)
		scanArgs := make([]any, r.Lengths)
		for i := range row {
			scanArgs[i] = &row[i]
		}
		// Iterate through all rows
		defer close(r.Result)
		for conn.Rows.Next() {
			conn.Err = conn.Rows.Scan(scanArgs...)
			if conn.Err != nil {
				return
			}
			// Convert scan results to FieldValue slice
			fieldValues := make([]FieldValue, r.Lengths)
			for i, val := range row {
				fieldValues[i] = convertToFieldValue(val)
			}
			r.RowCount++
			r.Result <- fieldValues
		}

	}()

	return r
}

// Mysql_warning_count returns the number of warnings from the last query
func Mysql_warning_count(conn *DBConnection) int {
	conn.Rows, conn.Err = conn.Conn.Query("SHOW WARNINGS")
	if conn.Err != nil {
		return 0
	}
	defer conn.Rows.Close()

	count := 0
	for conn.Rows.Next() {
		count++
	}
	return count
}

// MySQLQuery executes a query and returns true if there was an error
func (d *DBConnection) MySQLQuery(sql string) bool {
	if d.Rows != nil {
		err := d.Rows.Close()
		if err != nil {
			log.Warnf("MySQLQuery: Error closing connection: %s", err.Error())
		}
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
func (d *DBConnection) RealError() {
	if d.Err == nil {
		d.Code = 0
		d.Message = ""
		return
	}
	var mysqlErr *mysql.MySQLError
	errors.As(d.Err, &mysqlErr)
	d.Code = mysqlErr.Number
	d.Message = mysqlErr.Message
}

// UseDB executes USE dbName and returns false on error.
func (d *DBConnection) UseDB(dbName string) bool {
	_, d.Err = d.Conn.Exec("USE " + dbName)
	if d.Err != nil {
		d.RealError()
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

// M_store_result_row_free frees the memory associated with M_ROW
func M_store_result_row_free(mr *M_ROW) {
	mr.Res = nil
	mr.Row = nil
}
