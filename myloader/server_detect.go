package myloader

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

func detect_server_version(o *OptionEntries, conn *client.Conn) error {
	var ascii_version_comment, ascii_version string
	var err error
	var res *mysql.Result
	res, err = conn.Execute("SELECT @@version_comment, @@version")
	if err != nil {
		log.Errorf("get server version fail:%v", err)
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
	o.global.major, err = strconv.Atoi(sver[0])
	o.global.secondary, err = strconv.Atoi(sver[1])
	o.global.revision, err = strconv.Atoi(sver[2])
	if err != nil {
		return err
	}
	return nil
}

func (o *OptionEntries) get_product() server_type {
	return o.global.product
}

func (o *OptionEntries) is_mysql_like() bool {
	return o.get_product() == SERVER_TYPE_PERCONA || o.get_product() == SERVER_TYPE_MARIADB || o.get_product() == SERVER_TYPE_MYSQL || o.get_product() == SERVER_TYPE_UNKNOWN
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

func (o *OptionEntries) get_product_name() string {
	switch o.get_product() {
	case SERVER_TYPE_PERCONA:
		return "percona"
	case SERVER_TYPE_MYSQL:
		return "mysql"
	case SERVER_TYPE_MARIADB:
		return "mariadb"
	case SERVER_TYPE_TIDB:
		return "tidb"
	default:
		return ""
	}
}
