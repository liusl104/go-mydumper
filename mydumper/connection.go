package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"
)

func set_connection_defaults_file_and_group(o *OptionEntries, cdf string, group string) {
	o.global.connection_defaults_file = cdf
	o.global.connection_default_file_group = group
}

func initialize_connection(o *OptionEntries, app string) {
	o.global.program_name = app
	o.global.print_connection_details = 1
}

func configure_connection(o *OptionEntries) (conn *client.Conn, err error) {
	var host string
	if o.Connection.Protocol == "tcp" {
		if o.Connection.Hostname != "" && o.Connection.Port == 0 {
			p := os.Getenv("MYSQL_TCP_PORT")
			if p == "" {
				o.Connection.Port = 3306
			} else {
				i, _ := strconv.ParseInt(p, 10, 64)
				o.Connection.Port = int(i)
			}
		}
		if o.Connection.Hostname == "" {
			o.Connection.Hostname = os.Getenv("MYSQL_HOST")
		}
		host = fmt.Sprintf("%s:%d", o.Connection.Hostname, o.Connection.Port)
	}
	if o.Connection.Protocol == "socket" {
		host = o.Connection.SocketPath
		if host == "" {
			host = "/var/lib/mysql/mysql.sock"
		}
	}
	if o.CommonConnection.Ssl {
		tlsConfig := client.NewClientTLSConfig([]byte(o.CommonConnection.Ca), []byte(o.CommonConnection.Cert), []byte(o.CommonConnection.Key),
			false, o.global.program_name)
		conn, err = client.Connect(host, o.Connection.Username, o.Connection.Password, "", func(c *client.Conn) {
			c.SetTLSConfig(tlsConfig)

		})
		if err != nil {
			return
		}
	} else {
		conn, err = client.Connect(host, o.Connection.Username, o.Connection.Password, "", func(c *client.Conn) {
			return
		})
		if err != nil {
			return
		}
	}
	err = conn.Ping()
	if err != nil {
		return
	}
	return
}

func print_connection_details_once(o *OptionEntries) {
	if !g_atomic_int_dec_and_test(&o.global.print_connection_details) {
		return
	}
	var print_head, print_body string
	print_head += "Connection"
	switch o.Connection.Protocol {
	case "tcp":
		print_head += " via TCP/IP"
	case "socker":
		print_head += " via UNIX socket"
	}
	if o.Connection.Password != "" || o.Connection.AskPassword {
		print_head += " using password"
	}
	if o.Connection.Hostname != "" {
		print_body += fmt.Sprintf(" Host: %s", o.Connection.Hostname)
	}
	if o.Connection.Port > 0 {
		print_body += fmt.Sprintf(" Port: %d", o.Connection.Port)
	}
	if o.Connection.SocketPath != "" {
		print_body += fmt.Sprintf(" SocketPath: %s", o.Connection.SocketPath)
	}
	if o.Connection.Username != "" {
		print_body += fmt.Sprintf(" User: %s", o.Connection.Username)
	}
	if len(print_body) > 1 {
		print_head += ":"
		print_head += print_body
	}
	log.Infof(print_head)
}

func m_connect(o *OptionEntries) (*client.Conn, error) {
	conn, err := configure_connection(o)
	if err != nil {
		return conn, err
	}
	err = conn.Ping()
	if err != nil {
		log.Fatalf("Error connection to database: %v", err)
	}
	print_connection_details_once(o)

	if o.global.set_names_statement != "" {
		_, err = conn.Execute(o.global.set_names_statement)
	}
	return conn, err
}

func hide_password(o *OptionEntries) {
	if o.Connection.HidePassword != "" {
		for index := 1; index <= len(os.Args)-1; index++ {
			if os.Args[index] == string(o.Connection.HidePassword) {
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

func ask_password(o *OptionEntries) {
	if o.Connection.Password == "" && o.Connection.AskPassword {
		o.Connection.Password = passwordPrompt()
	}
}

func g_atomic_int_dec_and_test(a *int64) bool {
	atomic.AddInt64(a, -1)
	if *a == 0 {
		return true
	}
	return false

}
