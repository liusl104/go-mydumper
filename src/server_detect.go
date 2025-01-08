package mydumper

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	log "go-mydumper/src/logrus"
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
)

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

// Is_mysql_like returns true if the detected server is MySQL-like (Percona, MariaDB, MySQL, or unknown)
func Is_mysql_like() bool {
	return Get_product() == SERVER_TYPE_PERCONA || Get_product() == SERVER_TYPE_MARIADB || Get_product() == SERVER_TYPE_MYSQL || Get_product() == SERVER_TYPE_UNKNOWN
}

// Detect_server_version detects the server type and version
func Detect_server_version(conn *DBConnection) error {
	var ascii_version_comment, ascii_version string
	var err error
	var res *mysql.Result
	res = conn.Execute("SELECT @@version_comment, @@version")
	if conn.Err != nil {
		log.Warnf("Not able to determine database version: %v", err)
		return err
	}
	if len(res.Values) == 0 {
		log.Warnf("Not able to determine database version")
		return nil
	}
	var ver []mysql.FieldValue
	for _, ver = range res.Values {
		ascii_version_comment = string(ver[0].AsString())
		ascii_version = string(ver[1].AsString())
	}

	if strings.HasPrefix(strings.ToLower(ascii_version), "percona") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "percona") {
		product = SERVER_TYPE_PERCONA
	} else if strings.HasPrefix(strings.ToLower(ascii_version), "mariadb") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "mariadb") {
		product = SERVER_TYPE_MARIADB
	} else if strings.HasPrefix(strings.ToLower(ascii_version), "tidb") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "tidb") {
		product = SERVER_TYPE_TIDB
	} else if strings.HasPrefix(strings.ToLower(ascii_version), "mysql") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "mysql") {
		product = SERVER_TYPE_MYSQL
	}
	var sver = strings.SplitN(string(ver[1].AsString()), ".", 3)
	if product == SERVER_TYPE_UNKNOWN {
		conn.Execute("SELECT value FROM system.build_options where name='VERSION_FULL'")
		for _, row := range res.Values {
			ascii_version = string(row[0].AsString())
			var psver = strings.SplitN(ascii_version, " ", 2)
			if strings.EqualFold(ascii_version, "clickhouse") || strings.EqualFold(ascii_version_comment, "clickhouse") {
				product = SERVER_TYPE_CLICKHOUSE
				sver = strings.SplitN(psver[1], ".", 4)
			}
			_ = psver
		}
	}

	major, err = strconv.Atoi(sver[0])
	secondary, err = strconv.Atoi(sver[1])
	revision, err = strconv.Atoi(sver[2])
	if err != nil {
		return err
	}
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
			Show_all_replicas_status = SHOW_ALL_SLAVES_STATUS
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
		case SERVER_TYPE_MYSQL, SERVER_TYPE_PERCONA, SERVER_TYPE_UNKNOWN:
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
		default:
			break
		}
	} else {
		Start_replica = CALL_START_REPLICATION
		Start_replica_sql_thread = CALL_START_REPLICATION
		Stop_replica = CALL_STOP_REPLICATION
		Stop_replica_sql_thread = CALL_STOP_REPLICATION
		Reset_replica = CALL_RESET_EXTERNAL_MASTER
	}
	return nil
}

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
		return "ClickHouse"
	case SERVER_TYPE_UNKNOWN:
		return "unknown"
	default:
		return ""
	}
}
