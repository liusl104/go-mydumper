package src

type server_type int

const (
	SERVER_TYPE_UNKNOWN server_type = iota
	SERVER_TYPE_MYSQL
	SERVER_TYPE_TIDB
	SERVER_TYPE_MARIADB
	SERVER_TYPE_PERCONA
)
