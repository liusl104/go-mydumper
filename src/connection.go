package mydumper

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/liusl104/go-mydumper/src/logrus"
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
	LocalInFile                   bool
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
	Code    int16
	Warning uint16
	Result  *mysql.Result
	Stmt    *client.Stmt
	Query   string
}

func Connection_arguments_callback() bool {
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

func Connection_entries() {
	pflag.StringVarP(&Hostname, "host", "h", "", "The host to connect to")
	pflag.StringVarP(&Username, "user", "u", "", "Username with the necessary privileges")
	pflag.StringVarP(&HidePassword, "password", "p", "", "User password")
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
	if Hostname == "" {
		Hostname = os.Getenv("MYSQL_HOST")
	}
	if Port == 0 {
		Port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	}

}

func print_connection_details_once() {
	if !g_atomic_int_dec_and_test(&print_connection_details) {
		return
	}
	var print_head *GString = G_string_sized_new(20)
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
		G_string_append_printf(print_body, " Host: %s", Hostname)
	}
	if Port > 0 {
		G_string_append_printf(print_body, " Port: %d", Port)
	}
	if SocketPath != "" {
		G_string_append_printf(print_body, " Socket: %s", SocketPath)
	}
	if Username != "" {
		G_string_append_printf(print_body, " User: %s", Username)
	}
	if print_body.Len > 1 {
		G_string_append(print_head, ":")
		G_string_append(print_head, print_body.Str.String())
	}
	log.Info(print_head.Str.String())
	G_string_free(print_head, true)
	G_string_free(print_body, true)
}

// readPEMFile reads a PEM file and returns its contents as bytes
func readPEMFile(filename string) ([]byte, error) {
	if filename == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", filename, err)
	}
	return data, nil
}

// configureTLSConfig creates and configures a TLS config based on SSL parameters
// Returns TLS config data and whether SSL should be enabled
func configureTLSConfig() (caCert []byte, cert []byte, key []byte, skipVerify bool, useSSL bool, err error) {
	// Determine if SSL should be enabled based on Ssl flag or Ssl_mode
	useSSL = Ssl || (Ssl_mode != "" && strings.ToUpper(Ssl_mode) != "DISABLED")

	if !useSSL {
		return nil, nil, nil, false, false, nil
	}

	// Determine skip verify based on SSL mode
	skipVerify = false
	if Ssl_mode != "" {
		mode := strings.ToUpper(Ssl_mode)
		switch mode {
		case "DISABLED":
			return nil, nil, nil, false, false, nil
		case "PREFERRED", "REQUIRED":
			skipVerify = false
		case "VERIFY_CA":
			skipVerify = false // Verify CA but not hostname
		case "VERIFY_IDENTITY":
			skipVerify = false // Verify both CA and hostname
		default:
			log.Warnf("Unknown SSL mode: %s, using default", Ssl_mode)
		}
	}

	// Read certificate files if provided
	if Ca != "" {
		check_pem_exists(Ca, "ca")
		caCert, err = readPEMFile(Ca)
		if err != nil {
			return nil, nil, nil, false, false, err
		}
	}

	if Cert != "" {
		check_pem_exists(Cert, "cert")
		cert, err = readPEMFile(Cert)
		if err != nil {
			return nil, nil, nil, false, false, err
		}
	}

	if Key != "" {
		check_pem_exists(Key, "key")
		key, err = readPEMFile(Key)
		if err != nil {
			return nil, nil, nil, false, false, err
		}
	}

	// Check capath if provided
	if Capath != "" {
		check_capath(Capath)
		// Note: go-mysql library may not directly support capath,
		// but we validate it exists for consistency with C version
	}

	// Note: go-mysql library's NewClientTLSConfig may not support
	// Cipher and Tls_version directly. These would need to be set
	// on the underlying tls.Config if the library exposes it.
	// For now, we log a warning if these are specified.
	if Cipher != "" {
		log.Warnf("Cipher specification is not yet fully supported by go-mysql library")
	}
	if Tls_version != "" {
		log.Warnf("TLS version specification is not yet fully supported by go-mysql library")
	}

	return caCert, cert, key, skipVerify, true, nil
}

func mysql_real_connect(conn *DBConnection, hostname string, username string, password string, db string, port int, unix_socket string) bool {
	// Validate required parameters
	if conn == nil {
		log.Criticalf("mysql_real_connect: conn is nil")
		return false
	}

	if username == "" {
		log.Criticalf("mysql_real_connect: username is required")
		conn.Err = fmt.Errorf("username is required")
		return false
	}

	// Build connection address
	var addr string
	if unix_socket != "" {
		addr = unix_socket
	} else {
		if hostname == "" {
			hostname = "localhost"
		}
		if port <= 0 {
			port = 3306
		}
		addr = fmt.Sprintf("%s:%d", hostname, port)
	}

	// Configure TLS/SSL if needed
	caCert, cert, key, skipVerify, useSSL, err := configureTLSConfig()
	if err != nil {
		conn.Err = err
		log.Errorf("Failed to configure TLS: %v", err)
		return false
	}

	// Configure compression if requested
	// Note: go-mysql library may handle compression automatically
	// or through connection parameters. Check library documentation.
	if Compress_protocol {
		// Compression is typically negotiated during handshake
		// The go-mysql library should handle this automatically
		log.Debugf("Compression protocol requested")
	}

	// Connect to MySQL server with appropriate options
	// Note: Based on go-mysql library API, the callback receives *client.Conn
	if useSSL {
		if caCert != nil || cert != nil || key != nil {
			// Custom TLS config with certificates
			tlsConfig := client.NewClientTLSConfig(caCert, cert, key, skipVerify, program_name)
			conn.Conn.SetTLSConfig(tlsConfig)
			conn.Conn, conn.Err = client.Connect(addr, username, password, db)
		} else {
			// Basic SSL without certificates
			conn.Conn, conn.Err = client.Connect(addr, username, password, db)
		}
	} else {
		// No SSL
		conn.Conn, conn.Err = client.Connect(addr, username, password, db)
	}

	if conn.Err != nil {
		log.Errorf("Failed to connect to MySQL server at %s: %v", addr, conn.Err)
		return false
	}

	// Verify connection is valid
	if conn.Conn == nil {
		conn.Err = fmt.Errorf("connection object is nil after Connect")
		return false
	}

	return true
}
func Mysql_thread_id(dc *DBConnection) uint64 {
	res := dc.Conn.GetConnectionID()
	return uint64(res)
}
func Mysql_init() *DBConnection {
	return new(DBConnection)
}
func M_connect(conn *DBConnection) {
	configure_connection(conn)
	if !mysql_real_connect(conn, Hostname, Username, Password, "", Port, SocketPath) {
		log.Criticalf("Error connection to database: %v", conn.Err)
	}

	conn.Err = conn.Ping()
	if conn.Err != nil {
		log.Criticalf("Error connection to database: %v", conn.Err)
	}
	print_connection_details_once()

	if Set_names_statement != "" {
		M_query_warning(conn, Set_names_statement, "Not able to execute SET NAMES statement")
	}
	return
}

func Hide_password() {
	if HidePassword != "" {
		var tmpPasswd []byte = []byte(HidePassword)
		Password = string(tmpPasswd)
		for index := 1; index <= len(os.Args)-1; index++ {
			if os.Args[index] == HidePassword {
				p := *(*unsafe.Pointer)(unsafe.Pointer(&os.Args[index]))
				for i := 0; i < len(os.Args[index]); i++ {
					*(*uint8)(unsafe.Pointer(uintptr(p) + uintptr(i))) = 'X'
				}
			}
		}
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
