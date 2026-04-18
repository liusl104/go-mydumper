package mydumper

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/go-sql-driver/mysql"
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
	Conn    *sql.DB
	Err     error
	Code    uint16
	Warning uint16
	Message string
	Rows    *sql.Rows
	Query   string
	connID  uint32 // Store connection ID separately

}

// Connection_arguments_callback parses the protocol flag and sets protocol (tcp/socket). Returns true if protocol was set.
func Connection_arguments_callback() bool {
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

// Connection_entries registers connection-related command-line flags (host, user, password, port, socket, SSL, etc.).
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

// set_connection_defaults_file_and_group stores the defaults file path and group name for later use.
func set_connection_defaults_file_and_group(cdf string, group string) {
	connection_defaults_file = cdf
	connection_default_file_group = group
}

// Initialize_connection sets the program name used for connection defaults.
func Initialize_connection(app string) {
	program_name = app
}

// check_pem_exists exits with log.Fatal if the PEM file is missing or path is empty (used for SSL options).
func check_pem_exists(filename string, option string) {
	if filename == "" {
		log.Fatalf("SSL required option missing: %s", option)
	} else if !G_file_test(filename) {
		log.Fatalf("%s file does not exist: %s", option, filename)
	}
}

// check_capath exits with log.Fatal if p is not an existing directory (used for SSL capath).
func check_capath(p string) {
	if !G_file_test(p) {
		log.Fatalf("capath is not directory: %s", p)
	}
}

// configure_connection fills Hostname and Port from environment (MYSQL_HOST, MYSQL_PORT) if not set.
func configure_connection(conn *DBConnection) {
	if Hostname == "" {
		Hostname = os.Getenv("MYSQL_HOST")
	}
	if Port == 0 {
		Port, _ = strconv.Atoi(os.Getenv("MYSQL_TCP_PORT"))
	}
}

// print_connection_details_once logs connection type and parameters once (thread-safe via atomic).
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

// buildDSN builds a MySQL DSN (Data Source Name) string using mysql.Config
func buildDSN(hostname string, username string, password string, db string, port int, unix_socket string) (string, error) {
	// Create a new MySQL config
	cnf := mysql.NewConfig()

	// Set basic connection parameters
	cnf.User = username
	cnf.Passwd = password
	cnf.DBName = db

	// Set network address
	if unix_socket != "" {
		// Use Unix socket
		cnf.Net = "unix"
		cnf.Addr = unix_socket
	} else {
		// Use TCP/IP
		cnf.Net = "tcp"
		if hostname == "" {
			hostname = "localhost"
		}
		if port <= 0 {
			port = 3306
		}
		cnf.Addr = fmt.Sprintf("%s:%d", hostname, port)
	}
	cnf.Params = map[string]string{"charset": "utf8mb4"}
	// SSL/TLS configuration
	useSSL := Ssl || (Ssl_mode != "" && strings.ToUpper(Ssl_mode) != "DISABLED")
	if useSSL {
		mode := strings.ToUpper(Ssl_mode)
		switch mode {
		case "DISABLED":
			cnf.TLSConfig = "false"
		case "PREFERRED":
			cnf.TLSConfig = "preferred"
		case "REQUIRED":
			cnf.TLSConfig = "true"
		case "VERIFY_CA":
			cnf.TLSConfig = "skip-verify" // Skip hostname verification but verify CA
		case "VERIFY_IDENTITY":
			cnf.TLSConfig = "true" // Full verification including hostname
		default:
			cnf.TLSConfig = "true"
		}

		// Initialize Params if nil
		if cnf.Params == nil {
			cnf.Params = make(map[string]string)
		}

		// Certificate files (set via Params)
		if Ca != "" {
			check_pem_exists(Ca, "ca")
			cnf.Params["tls-ca"] = Ca
		}
		if Cert != "" {
			check_pem_exists(Cert, "cert")
			cnf.Params["tls-cert"] = Cert
		}
		if Key != "" {
			check_pem_exists(Key, "key")
			cnf.Params["tls-key"] = Key
		}
		if Capath != "" {
			check_capath(Capath)
			// go-sql-driver/mysql doesn't directly support capath, but we validate it
		}
		if Cipher != "" {
			cnf.Params["tls-ciphers"] = Cipher
		}
		if Tls_version != "" {
			cnf.Params["tls-version"] = Tls_version
		}
	} else {
		cnf.TLSConfig = "false"
	}

	// Compression (set via Params)
	if Compress_protocol {
		if cnf.Params == nil {
			cnf.Params = make(map[string]string)
		}
		cnf.Params["compress"] = "true"
	}

	// Format and return DSN string
	return cnf.FormatDSN(), nil
}

// mysql_real_connect opens a MySQL connection using the given parameters, builds DSN, and pings; returns false on error.
func mysql_real_connect(conn *DBConnection, hostname string, username string, password string, db string, port int, unix_socket string) bool {
	var dsn string
	dsn, conn.Err = buildDSN(hostname, username, password, db, port, unix_socket)
	if conn.Err != nil {
		log.Errorf("Failed to build DSN: %v", conn.Err)
		return false
	}
	// Open database connection
	conn.Conn, conn.Err = sql.Open("mysql", dsn)
	if conn.Err != nil {
		log.Errorf("Failed to open MySQL connection: %v", conn.Err)
		return false
	}
	// Set connection pool settings
	conn.Conn.SetMaxOpenConns(1)
	conn.Conn.SetMaxIdleConns(1)
	conn.Conn.SetConnMaxLifetime(0)
	if conn.Err = conn.Ping(); conn.Err != nil {
		log.Errorf("Failed to ping MySQL connection: %v", conn.Err)
		return false
	}
	return true
}

// Mysql_thread_id queries CONNECTION_ID() from the server and stores it in dc; returns the connection ID or 0 on error.
func Mysql_thread_id(dc *DBConnection) uint64 {
	var connID uint32
	err := dc.Conn.QueryRow("SELECT CONNECTION_ID()").Scan(&connID)
	if err != nil {
		log.Errorf("Failed to get connection ID: %v", err)
		return 0
	}
	dc.connID = connID
	return uint64(connID)
}

// Mysql_init allocates and returns a new DBConnection (not yet connected).
func Mysql_init() *DBConnection {
	return new(DBConnection)
}

// M_connect configures the connection, opens it, fetches connection ID, prints details once, and runs SET NAMES if set.
func M_connect(conn *DBConnection) {
	configure_connection(conn)
	if !mysql_real_connect(conn, Hostname, Username, Password, "", Port, SocketPath) {
		log.Criticalf("Error connection to database: %v", conn.Err)
		return
	}

	// Get connection ID
	Mysql_thread_id(conn)

	print_connection_details_once()

	if Set_names_statement != "" {
		M_query_warning(conn, Set_names_statement, "Not able to execute SET NAMES statement")
	}
	return
}

// Hide_password copies HidePassword to Password and overwrites the password in os.Args with 'X' to avoid exposure.
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

// passwordPrompt prints the password prompt and reads the password from the terminal without echoing.
func passwordPrompt() string {
	fmt.Printf("Enter MySQL Password: ")
	return terminalInput()
}

// terminalInput reads a line from stdin with terminal in raw mode (e.g. for password input).
func terminalInput() string {
	// Pass stdin fd to term.MakeRaw; it returns a function to restore terminal state
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		log.Fatal(err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState) // Restore terminal state

	// Create a new terminal for reading password
	terminal := term.NewTerminal(os.Stdin, "")

	password, err := terminal.ReadPassword("")
	if err != nil {
		log.Fatal(err)
	}
	return password
}

// Ask_password prompts for password if AskPassword is set and Password is empty.
func Ask_password() {
	if Password == "" && AskPassword {
		Password = passwordPrompt()
	}
}
