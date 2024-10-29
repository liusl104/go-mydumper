package mydumper

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	"strconv"
	"strings"
)

type server_type int

const (
	SERVER_TYPE_UNKNOWN server_type = iota
	SERVER_TYPE_MYSQL
	SERVER_TYPE_TIDB
	SERVER_TYPE_MARIADB
	SERVER_TYPE_PERCONA
	SERVER_TYPE_CLICKHOUSE
)

const (
	DETECT_MYSQL_REGEX   = "^([3-9]\\.[0-9]+\\.[0-9]+)"
	DETECT_MARIADB_REGEX = "^([0-9]{1,2}\\.[0-9]+\\.[0-9]+)"
	DETECT_TIDB_REGEX    = "TiDB"
)

func (o *OptionEntries) get_product() server_type {
	return o.global.product
}

func (o *OptionEntries) get_major() int {
	return o.global.major
}

func (o *OptionEntries) get_secondary() int {
	return o.global.secondary
}

func (o *OptionEntries) get_revision() int {
	return o.global.revision
}

func (o *OptionEntries) is_mysql_like() bool {
	return o.get_product() == SERVER_TYPE_PERCONA || o.get_product() == SERVER_TYPE_MARIADB || o.get_product() == SERVER_TYPE_MYSQL ||
		o.get_product() == SERVER_TYPE_UNKNOWN
}

func detect_server_version(o *OptionEntries, conn *client.Conn) error {
	var ascii_version_comment, ascii_version string
	var err error
	var res *mysql.Result
	res, err = conn.Execute("SELECT @@version_comment, @@version")
	if err != nil {
		log.Errorf("get server version fail:%v", err)
	}
	if len(res.Values) == 0 {
		log.Warnf("Not able to determine database version")
		return nil
	}
	for _, row := range res.Values {
		ascii_version_comment = string(row[0].AsString())
		ascii_version = string(row[1].AsString())
	}
	if err != nil {
		return err
	}
	if strings.HasPrefix(strings.ToLower(ascii_version), "percona") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "percona") {
		o.global.product = SERVER_TYPE_PERCONA
	} else if strings.HasPrefix(strings.ToLower(ascii_version), "mariadb") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "mariadb") {
		o.global.product = SERVER_TYPE_MARIADB
	} else if strings.HasPrefix(strings.ToLower(ascii_version), "mysql") || strings.HasPrefix(strings.ToLower(ascii_version_comment), "mysql") {
		o.global.product = SERVER_TYPE_MYSQL
	}
	var sver = strings.SplitN(ascii_version, ".", 3)
	if o.global.product == SERVER_TYPE_UNKNOWN {
		res, err = conn.Execute("SELECT value FROM system.build_options where name='VERSION_FULL'")
		if err != nil {
			return err
		}
		for _, ver := range res.Values {
			ascii_version = string(ver[0].AsString())
			psver := strings.SplitN(ascii_version, " ", 2)
			if ascii_version == "clickhouse" || ascii_version_comment == "clickhouse" {
				o.global.product = SERVER_TYPE_CLICKHOUSE
				sver = strings.SplitN(psver[1], ".", 4)
			}
		}
	}
	o.global.major, err = strconv.Atoi(sver[0])
	o.global.secondary, err = strconv.Atoi(sver[1])
	o.global.revision, err = strconv.Atoi(sver[2])
	if err != nil {
		return err
	}
	o.global.show_replica_status = SHOW_SLAVE_STATUS
	o.global.show_binary_log_status = SHOW_MASTER_STATUS
	if o.global.source_control_command == TRADITIONAL {
		o.global.start_replica = START_SLAVE
		o.global.stop_replica = STOP_SLAVE
		o.global.start_replica_sql_thread = START_SLAVE_SQL_THREAD
		o.global.stop_replica_sql_thread = STOP_SLAVE_SQL_THREAD
		o.global.reset_replica = RESET_SLAVE
		o.global.change_replication_source = CHANGE_MASTER
		switch o.get_product() {
		case SERVER_TYPE_MARIADB:
			if o.get_major() < 10 {
				o.global.show_all_replicas_status = SHOW_ALL_SLAVES_STATUS
				if o.get_secondary() >= 5 {
					if o.get_revision() >= 2 {
						o.global.show_binary_log_status = SHOW_BINLOG_STATUS
					}
				}

			} else {
				if o.get_secondary() <= 5 {
					o.global.show_all_replicas_status = SHOW_ALL_SLAVES_STATUS
				} else {
					o.global.start_replica = START_REPLICA
					o.global.stop_replica = STOP_REPLICA
					o.global.start_replica_sql_thread = START_REPLICA_SQL_THREAD
					o.global.stop_replica_sql_thread = STOP_REPLICA_SQL_THREAD
					o.global.reset_replica = RESET_REPLICA
					o.global.show_replica_status = SHOW_REPLICA_STATUS
					o.global.show_all_replicas_status = SHOW_ALL_REPLICAS_STATUS
				}
			}
			break
		case SERVER_TYPE_MYSQL, SERVER_TYPE_PERCONA, SERVER_TYPE_UNKNOWN:
			if o.get_major() >= 8 && (o.get_secondary() > 0 || (o.get_secondary() == 0 && o.get_revision() >= 22)) {
				o.global.start_replica = START_REPLICA
				o.global.stop_replica = STOP_REPLICA
				o.global.start_replica_sql_thread = START_REPLICA_SQL_THREAD
				o.global.stop_replica_sql_thread = STOP_REPLICA_SQL_THREAD
				o.global.reset_replica = RESET_REPLICA
				o.global.show_replica_status = SHOW_REPLICA_STATUS
				if o.get_secondary() >= 2 {
					o.global.show_binary_log_status = SHOW_BINARY_LOG_STATUS
				}
				o.global.change_replication_source = CHANGE_REPLICATION_SOURCE
			}
		}
	} else {
		o.global.start_replica = CALL_START_REPLICATION
		o.global.start_replica_sql_thread = CALL_START_REPLICATION
		o.global.stop_replica = CALL_STOP_REPLICATION
		o.global.stop_replica_sql_thread = CALL_STOP_REPLICATION
		o.global.reset_replica = CALL_RESET_EXTERNAL_MASTER
	}
	return nil
}

func (o *OptionEntries) get_product_name() string {
	switch o.get_product() {
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
	case SERVER_TYPE_UNKNOWN:
		return "unknown"
	default:
		return ""
	}
}
