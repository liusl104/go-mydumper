package mydumper

import (
	"strconv"
	"strings"
)

type ServerType int

const (
	SERVER_TYPE_UNKNOWN ServerType = iota
	SERVER_TYPE_MYSQL
	SERVER_TYPE_TIDB
	SERVER_TYPE_MARIADB
	SERVER_TYPE_PERCONA
	SERVER_TYPE_CLICKHOUSE
	SERVER_TYPE_RDS
	SERVER_TYPE_DOLT
)

var (
	product                   ServerType = SERVER_TYPE_UNKNOWN
	major                     int
	secondary                 int
	revision                  int
	Start_replica             string
	Stop_replica              string
	Start_replica_sql_thread  string
	Stop_replica_sql_thread   string
	Reset_replica             string
	Show_replica_status       string
	Show_all_replicas_status  string
	Show_binary_log_status    string
	Change_replication_source string
	Case_sensitive_prefix     string
	Case_sensitive_suffix     string
)

// Get_product_name returns the human-readable name of the detected server product (e.g. "MySQL", "MariaDB").
func Get_product_name() string {
	switch Get_product() {
	case SERVER_TYPE_PERCONA:
		return "Percona"
	case SERVER_TYPE_MYSQL:
		return "MySQL"
	case SERVER_TYPE_MARIADB:
		return "MariaDB"
	case SERVER_TYPE_TIDB:
		return "TiDB"
	case SERVER_TYPE_CLICKHOUSE:
		return "Clickhouse"
	case SERVER_TYPE_DOLT:
		return "Dolt"
	case SERVER_TYPE_UNKNOWN:
		return "unknown"
	default:
		return ""
	}
}

// Is_mysql_like returns true if the detected server is MySQL-like (Percona, MariaDB, MySQL, or unknown)
func Is_mysql_like() bool {
	return Get_product() == SERVER_TYPE_PERCONA || Get_product() == SERVER_TYPE_MARIADB || Get_product() == SERVER_TYPE_MYSQL ||
		Get_product() == SERVER_TYPE_DOLT || Get_product() == SERVER_TYPE_UNKNOWN
}

// Server_support_tablespaces returns true if the detected server supports tablespaces (Percona, MySQL, or unknown).
func Server_support_tablespaces() bool {
	return Get_product() == SERVER_TYPE_PERCONA || Get_product() == SERVER_TYPE_MYSQL || Get_product() == SERVER_TYPE_UNKNOWN
}

// Detect_product detects the server type and version
func Detect_product(_ascii_version_comment, _ascii_version string) error {
	var ascii_version, ascii_version_comment string
	if _ascii_version != "" {
		ascii_version = strings.ToLower(_ascii_version)
	}
	if _ascii_version_comment != "" {
		ascii_version_comment = strings.ToLower(_ascii_version_comment)
	}

	if strings.Contains(ascii_version, "percona") || strings.Contains(ascii_version_comment, "percona") {
		product = SERVER_TYPE_PERCONA
	} else if strings.Contains(ascii_version, "mariadb") || strings.Contains(ascii_version_comment, "mariadb") {
		product = SERVER_TYPE_MARIADB
	} else if strings.Contains(ascii_version, "tidb") || strings.Contains(ascii_version_comment, "tidb") {
		product = SERVER_TYPE_TIDB
	} else if strings.Contains(ascii_version, "dolt") || strings.Contains(ascii_version_comment, "dolt") {
		product = SERVER_TYPE_DOLT
	} else if strings.Contains(ascii_version, "mysql") || strings.Contains(ascii_version_comment, "mysql") ||
		strings.Contains(ascii_version, "source") || strings.Contains(ascii_version_comment, "source") {
		product = SERVER_TYPE_MYSQL
	}
	return nil
}

// Detect_version parses major.secondary.revision from sver and stores them in package variables.
func Detect_version(sver []string) {
	major, _ = strconv.Atoi(sver[0])
	secondary, _ = strconv.Atoi(sver[1])
	revision, _ = strconv.Atoi(sver[2])
}

// Detect_server_version queries @@version_comment and @@version (or ClickHouse build_options), then calls Detect_product and Detect_version.
func Detect_server_version(conn *DBConnection) {
	var mr = M_store_result_row(conn, "SELECT @@version_comment, @@version", M_warning, M_message, "Not able to determine database version")
	var ascii_version_comment string
	if mr.Row != nil {
		ascii_version_comment = strings.ToLower(string(mr.Row[0].AsString()))
		Detect_product(string(mr.Row[0].AsString()), string(mr.Row[1].AsString()))
	}
	var sver []string
	if product == SERVER_TYPE_UNKNOWN {
		M_store_result_row_free(mr)
		mr = M_store_result_row(conn, "SELECT value FROM system.build_options where name='VERSION_FULL' LIMIT 1", M_warning, M_message, "Not able to determine database version")
		if mr.Row != nil {
			var ascii_version = strings.ToLower(string(mr.Row[0].AsString()))
			var psver []string = strings.SplitN(ascii_version, " ", 2)
			if strings.Contains(ascii_version, "clickhouse") || strings.Contains(ascii_version_comment, "clickhouse") {
				product = SERVER_TYPE_CLICKHOUSE
				sver = strings.SplitN(psver[1], ".", 4)
			}
		} else {
			sver = strings.SplitN("0.0.0", ".", 3)
		}
	} else {
		sver = strings.SplitN(string(mr.Row[1].AsString()), ".", 3)
	}
	M_store_result_row_free(mr)
	Detect_version(sver)
}

// Detect_lower_case_table_names queries @@lower_case_table_names and sets Case_sensitive_prefix/Suffix (CAST/AS BINARY or empty).
func Detect_lower_case_table_names(conn *DBConnection) {
	var lower_case_table_names uint
	var mr *M_ROW = M_store_result_row(conn, "SELECT @@lower_case_table_names", M_warning, M_message, "Not able to determine lower_case_table_names")
	if mr.Row != nil {
		lower_case_table_names = uint(mr.Row[0].AsUint64())
	}
	if lower_case_table_names != 0 {
		Case_sensitive_prefix = CAST
		Case_sensitive_suffix = AS_BINARY
	} else {
		Case_sensitive_prefix = EMPTY_STRING
		Case_sensitive_suffix = EMPTY_STRING
	}
	M_store_result_row_free(mr)
}

// Detect_replica sets replica-related SQL command strings (START/STOP REPLICA, etc.) based on product and version.
func Detect_replica() {
	Show_replica_status = SHOW_SLAVE_STATUS
	Show_binary_log_status = SHOW_MASTER_STATUS

	if Source_control_command == TRADITIONAL {
		Start_replica = START_SLAVE
		Stop_replica = STOP_SLAVE
		Start_replica_sql_thread = START_SLAVE_SQL_THREAD
		Stop_replica_sql_thread = STOP_SLAVE_SQL_THREAD
		Reset_replica = RESET_SLAVE
		Change_replication_source = CHANGE_MASTER
		switch Get_product() {
		case SERVER_TYPE_MARIADB:
			if Get_major() < 10 {
				Show_all_replicas_status = SHOW_ALL_SLAVES_STATUS
				if Get_secondary() >= 5 {
					if Get_revision() >= 2 {
						Show_binary_log_status = SHOW_BINLOG_STATUS
					}
				}

			} else {
				if Get_secondary() <= 5 {
					Show_all_replicas_status = SHOW_ALL_SLAVES_STATUS
				} else {
					Start_replica = START_REPLICA
					Stop_replica = STOP_REPLICA
					Start_replica_sql_thread = START_REPLICA_SQL_THREAD
					Stop_replica_sql_thread = STOP_REPLICA_SQL_THREAD
					Reset_replica = RESET_REPLICA
					Show_replica_status = SHOW_REPLICA_STATUS
					Show_all_replicas_status = SHOW_ALL_REPLICAS_STATUS
				}
			}
			break
		case SERVER_TYPE_MYSQL:
		case SERVER_TYPE_PERCONA:
		case SERVER_TYPE_UNKNOWN:
			if Get_major() >= 8 && (Get_secondary() > 0 || (Get_secondary() == 0 && Get_revision() >= 22)) {
				Start_replica = START_REPLICA
				Stop_replica = STOP_REPLICA
				Start_replica_sql_thread = START_REPLICA_SQL_THREAD
				Stop_replica_sql_thread = STOP_REPLICA_SQL_THREAD
				Reset_replica = RESET_REPLICA
				Show_replica_status = SHOW_REPLICA_STATUS
				if Get_secondary() >= 2 {
					Show_binary_log_status = SHOW_BINARY_LOG_STATUS
				}
				Change_replication_source = CHANGE_REPLICATION_SOURCE
			}
			break
		case SERVER_TYPE_DOLT:
			if Get_major() >= 8 && Get_secondary() >= 0 {
				Start_replica = START_REPLICA
				Stop_replica = STOP_REPLICA
				Start_replica_sql_thread = START_REPLICA_SQL_THREAD
				Stop_replica_sql_thread = STOP_REPLICA_SQL_THREAD
				Reset_replica = RESET_REPLICA
				Show_replica_status = SHOW_REPLICA_STATUS
				if Get_secondary() >= 2 {
					Show_binary_log_status = SHOW_BINARY_LOG_STATUS
				}

				Change_replication_source = CHANGE_REPLICATION_SOURCE
			}
			break
		}
	} else {
		Start_replica = CALL_START_REPLICATION
		Start_replica_sql_thread = CALL_START_REPLICATION
		Stop_replica = CALL_STOP_REPLICATION
		Stop_replica_sql_thread = CALL_STOP_REPLICATION
		Reset_replica = CALL_RESET_EXTERNAL_MASTER
	}
}

// Server_detect runs full server detection: version (or ServerVersionArg), lower_case_table_names, and replica commands.
func Server_detect(conn *DBConnection) {
	if ServerVersionArg != "" {
		var _product []string = strings.SplitN(ServerVersionArg, "-", 2)
		Detect_product(_product[0], _product[1])
		if _product[1] != "" {
			var sver = strings.SplitN(_product[1], ".", 3)
			Detect_version(sver)
		}
	} else {
		Detect_server_version(conn)
	}
	Detect_lower_case_table_names(conn)
	Detect_replica()
}

// Get_product returns the detected server type
func Get_product() ServerType {
	return product
}

// Get_major returns the major number of the detected server
func Get_major() int {
	return major
}

// Get_secondary returns the secondary number of the detected server
func Get_secondary() int {
	return secondary
}

// Get_revision returns the revision number of the detected server
func Get_revision() int {
	return revision
}
