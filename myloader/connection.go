package myloader

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	"os"
	"strings"
	"unsafe"
)

func set_connection_defaults_file_and_group(o *OptionEntries, cdf string, group string) {
	o.global.connection_defaults_file = cdf
	o.global.connection_default_file_group = group
}

func (o *OptionEntries) initialize_connection(app string) {
	o.global.program_name = app
	o.global.print_connection_details = 1
}

func connection_arguments_callback(o *OptionEntries) error {
	if o.Protocol == "" {
		o.Protocol = "tcp"
	}
	if o.Protocol != "" {
		if strings.ToLower(o.Protocol) == "tcp" {
			o.Protocol = strings.ToLower(o.Protocol)
			return nil
		}
		if strings.ToLower(o.Protocol) == "socket" {
			o.Protocol = strings.ToLower(o.Protocol)
			return nil
		}
		log.Errorf("option --protocol value error")
		return fmt.Errorf("option --protocol value error")
	}

	return nil

}

func (o *OptionEntries) configure_connection() *db_connection {
	var conn = new(db_connection)
	var host string
	if o.Protocol == "tcp" {
		host = fmt.Sprintf("%s:%d", o.Hostname, o.Port)
	}
	if o.Protocol == "socket" {
		host = o.SocketPath
	}
	if o.Ssl {
		tlsConfig := client.NewClientTLSConfig([]byte(o.Ca), []byte(o.Cert), []byte(o.Key),
			false, o.global.program_name)
		conn.conn, conn.err = client.Connect(host, o.Username, o.Password, "", func(c *client.Conn) {
			c.SetTLSConfig(tlsConfig)

		})
		if conn.err != nil {
			return conn
		}
	} else {
		conn.conn, conn.err = client.Connect(host, o.Username, o.Password, "", func(c *client.Conn) {
			return
		})
		if conn.err != nil {
			return conn
		}
	}
	conn.err = conn.conn.Ping()
	if conn.err != nil {
		return conn
	}
	return conn
}

func print_connection_details_once(o *OptionEntries) {
	if !g_atomic_int_dec_and_test(&o.global.print_connection_details) {
		return
	}
	var print_head, print_body string
	print_head += "Connection"
	switch o.Protocol {
	case "tcp":
		print_head += " via TCP/IP"
	case "socker":
		print_head += " via UNIX socket"
	}
	if o.Password != "" || o.AskPassword {
		print_head += " using password"
	}
	if o.Hostname != "" {
		print_body += fmt.Sprintf(" Host: %s", o.Hostname)
	}
	if o.Port > 0 {
		print_body += fmt.Sprintf(" Port: %d", o.Port)
	}
	if o.SocketPath != "" {
		print_body += fmt.Sprintf(" SocketPath: %s", o.SocketPath)
	}
	if o.Username != "" {
		print_body += fmt.Sprintf(" User: %s", o.Username)
	}
	if len(print_body) > 1 {
		print_head += ":"
		print_head += print_body
	}
	log.Infof(print_head)
}

func (o *OptionEntries) m_connect() *db_connection {
	var conn = o.configure_connection()
	if conn.conn == nil {
		log.Fatalf("Error connection to database server: %v", conn.err)
		return conn
	}
	conn.err = conn.conn.Ping()
	if conn.err != nil {
		log.Fatalf("Error connection to database: %v", conn.err)
	}
	print_connection_details_once(o)

	if o.global.set_names_statement != "" {
		_, conn.err = conn.conn.Execute(o.global.set_names_statement)
	}
	return conn
}

func hide_password(o *OptionEntries) {
	if o.Password != "" {
		var tmpPasswd []byte = []byte(o.Password)
		for index := 1; index <= len(os.Args)-1; index++ {
			if os.Args[index] == string(tmpPasswd) {
				p := *(*unsafe.Pointer)(unsafe.Pointer(&os.Args[index]))
				for i := 0; i < len(os.Args[index]); i++ {
					*(*uint8)(unsafe.Pointer(uintptr(p) + uintptr(i))) = 'X'
				}
			}
		}
		o.Password = string(tmpPasswd)
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

func ask_password(o *OptionEntries) {
	if o.Password == "" && o.AskPassword {
		o.Password = passwordPrompt()
	}
}

func (d *db_connection) Execute(command string, args ...any) (result *mysql.Result) {
	result, d.err = d.conn.Execute(command, args...)
	if d.err != nil {
		var myError *mysql.MyError
		errors.As(d.err, &myError)
		d.code = myError.Code
	} else {
		d.code = 0
	}
	d.warning = result.Warnings
	return
}
func (d *db_connection) ping() error {
	d.err = d.conn.Ping()
	if d.err != nil {
		var myError *mysql.MyError
		errors.As(d.err, &myError)
		d.code = myError.Code
	} else {
		d.code = 0
	}
	return d.err
}
func (d *db_connection) close() error {
	return d.conn.Close()
}

func execute_gstring(conn *db_connection, ss *GString) {
	if ss != nil {
		lines := strings.Split(ss.str, ";\n")
		for _, line := range lines {
			if len(line) <= 3 {
				continue
			}
			_ = conn.Execute(line)
			if conn.err != nil {
				log.Warnf("Set session failed: %s", line)
			}
		}
	}
}
