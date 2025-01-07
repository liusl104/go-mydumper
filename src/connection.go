package mydumper

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"golang.org/x/term"
)

type mysql_protocol_type int

const (
	MYSQL_PROTOCOL_DEFAULT mysql_protocol_type = iota
	MYSQL_PROTOCOL_TCP
	MYSQL_PROTOCOL_SOCKET
)

var (
	Protocol_str                  string
	protocol                      mysql_protocol_type = MYSQL_PROTOCOL_DEFAULT
	connection_defaults_file      string
	connection_default_file_group string
	program_name                  string
	print_connection_details      int64 = 1
)
var (
	Hostname          string // The host to connect to
	Username          string // Username with the necessary privileges
	Password          string // User password
	AskPassword       bool   // Prompt For User password
	Port              int    // TCP/IP port to connect to
	SocketPath        string // UNIX domain socket file to use for connection
	Protocol          string // The protocol to use for connection (tcp, socket)
	Compress_protocol bool   // Use compression on the MySQL connection
	Ssl               bool   // Connect using SSL
	Ssl_mode          string // Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
	Key               string // The path name to the key file
	Cert              string // The path name to the certificate file
	Ca                string // The path name to the certificate authority file
	Capath            string // The path name to a directory that contains trusted SSL CA certificates in PEM format
	Cipher            string // A list of permissible ciphers to use for SSL encryption
	Tls_version       string // Which protocols the server permits for encrypted connections
)

type DBConnection struct {
	Conn    *client.Conn
	Err     error
	Code    uint16
	Warning uint16
	Result  *mysql.Result
}

func connection_arguments_callback() bool {
	// var Err error
	if Protocol != "" {
		if strings.EqualFold(Protocol, "tcp") {
			protocol = MYSQL_PROTOCOL_TCP
			return true
		}
		if strings.EqualFold(Protocol, "socket") {
			protocol = MYSQL_PROTOCOL_SOCKET
			return true
		}
	}
	return false
}

func connection_entries() {
	pflag.StringVarP(&Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&Password, "password", "p", "", "User password")
	pflag.BoolVarP(&AskPassword, "ask-password", "a", false, "Prompt For User password")
	pflag.IntVarP(&Port, "port", "P", 3306, "TCP/IP port to connect to")
	pflag.StringVarP(&SocketPath, "socket", "S", "", "UNIX domain socket file to use for connection")
	pflag.StringVar(&Protocol, "protocol", "tcp", "The protocol to use for connection (tcp, socket)")
	pflag.BoolVar(&Ssl, "ssl", false, "Connect using SSL")
	pflag.StringVar(&Ssl_mode, "ssl-mode", "", "Desired security state of the connection to the server: DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY")
	pflag.StringVar(&Cert, "cert", "", "The path name to the certificate file")
	pflag.StringVar(&Ca, "ca", "", "The path name to the certificate authority file")
	pflag.StringVar(&Capath, "capath", "", "The path name to a directory that contains trusted SSL CA certificates in PEM format")
	pflag.StringVar(&Key, "key", "", "The path name to the key file")
	pflag.StringVar(&Cipher, "cipher", "", "A list of permissible ciphers to use for SSL encryption")
	pflag.StringVar(&Tls_version, "tls-version", "", "Which protocols the server permits for encrypted connections")

}

func set_connection_defaults_file_and_group(cdf string, group string) {
	connection_defaults_file = cdf
	connection_default_file_group = group
}

func Initialize_connection(app string) {
	program_name = app
}

func check_pem_exists(filename string, option string) {
	if filename == "" {
		log.Fatalf("SSL required option missing: %s", option)
	} else if !G_file_test(filename) {
		log.Fatalf("%s file does not exist: %s", option, filename)
	}
}

func check_capath(p string) {
	if !G_file_test(p) {
		log.Fatalf("capath is not directory: %s", p)
	}
}

func configure_connection(conn *DBConnection) {

}

func print_connection_details_once() {
	if !g_atomic_int_dec_and_test(&print_connection_details) {
		return
	}
	var print_head *GString = G_string_new("")
	G_string_append(print_head, "Connection")
	switch protocol {
	case MYSQL_PROTOCOL_DEFAULT:
		G_string_append_printf(print_head, " via default library settings")
		break
	case MYSQL_PROTOCOL_TCP:
		G_string_append_printf(print_head, " via TCP/IP")
		break
	case MYSQL_PROTOCOL_SOCKET:
		G_string_append_printf(print_head, " via UNIX socket")
		break
	default:
		break
	}
	if Password != "" || AskPassword {
		G_string_append(print_head, " using password")
	}
	var print_body *GString = G_string_new("")
	if Hostname != "" {
		G_string_append_printf(print_body, "\n\tHost: %s", Hostname)
	}
	if Port > 0 {
		G_string_append_printf(print_body, "\n\tPort: %d", Port)
	}
	if SocketPath != "" {
		G_string_append_printf(print_body, "\n\tSocket: %s", SocketPath)
	}
	if Username != "" {
		G_string_append_printf(print_body, "\n\tUser: %s", Username)
	}
	if print_body.Len > 1 {
		G_string_append(print_head, ":")
		G_string_append(print_head, print_body.Str.String())
	}
	log.Info(print_head.Str.String())
	G_string_free(print_head, true)
	G_string_free(print_body, true)
}

func mysql_real_connect(conn *DBConnection, hostname string, username string, password string, db string, port int, unix_socket string) bool {
	if Ssl {
		tlsConfig := client.NewClientTLSConfig([]byte(Ca), []byte(Cert), []byte(Key),
			false, program_name)
		conn.Conn, conn.Err = client.Connect(hostname, username, password, db, func(c *client.Conn) {
			c.SetTLSConfig(tlsConfig)
		})
		if conn.Err != nil {
			return false
		}
	} else {
		conn.Conn, conn.Err = client.Connect(hostname, username, password, db, func(c *client.Conn) {
		})
		if conn.Err != nil {
			return false
		}
	}
	return true
}
func Mysql_thread_id(dc *DBConnection) uint64 {
	res, err := dc.Conn.Execute("SELECT connection_id()")
	if err != nil {
		log.Fatalf("Error getting connection_id: %v", err)
		return 0
	}
	for _, v := range res.Values {
		for _, v2 := range v {
			return v2.AsUint64()
		}
	}
	return 0
}
func M_connect(conn *DBConnection) {
	configure_connection(conn)
	if !mysql_real_connect(conn, Hostname, Username, Password, "", Port, SocketPath) {
		log.Fatalf("Error connection to database: %v", conn.Err)
	}

	conn.Err = conn.Ping()
	if conn.Err != nil {
		log.Fatalf("Error connection to database: %v", conn.Err)
	}
	print_connection_details_once()

	if Set_names_statement != "" {
		conn.Execute(Set_names_statement)
	}
	return
}

func (d *DBConnection) Ping() error {
	d.Err = d.Conn.Ping()
	if d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		d.Code = myError.Code
	} else {
		d.Code = 0
	}
	return d.Err
}

func (d *DBConnection) UseDB(dbName string) bool {
	if d.Err = d.Conn.UseDB(dbName); d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		d.Code = myError.Code
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
	d.Result, d.Err = d.Conn.Execute(command, args...)
	if d.Err != nil {
		var myError *mysql.MyError
		errors.As(d.Err, &myError)
		d.Code = myError.Code
	} else {
		d.Code = 0
	}
	d.Warning = result.Warnings
	return
}

func Hide_password() {
	if Password != "" {
		var tmpPasswd []byte = []byte(Password)
		for index := 1; index <= len(os.Args)-1; index++ {
			if os.Args[index] == string(tmpPasswd) {
				p := *(*unsafe.Pointer)(unsafe.Pointer(&os.Args[index]))
				for i := 0; i < len(os.Args[index]); i++ {
					*(*uint8)(unsafe.Pointer(uintptr(p) + uintptr(i))) = 'X'
				}
			}
		}
		Password = string(tmpPasswd)
	}
}

func passwordPrompt() string {
	fmt.Printf("Enter MySQL Password: ")
	return terminalInput()
}

func terminalInput() string {
	// 将标准输入的文件描述符传给 term.MakeRaw，它会返回一个恢复终端状态的函数
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		log.Fatal(err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState) // 恢复终端状态

	// 创建一个新的终端，用于读取密码
	terminal := term.NewTerminal(os.Stdin, "")

	password, err := terminal.ReadPassword("")
	if err != nil {
		log.Fatal(err)
	}
	return password
}

func Ask_password() {
	if Password == "" && AskPassword {
		Password = passwordPrompt()
	}
}

func Mysql_get_server_version() uint {
	return uint(Get_major()*10000 + Get_revision()*100 + Get_secondary())
}
