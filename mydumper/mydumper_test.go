package mydumper

import (
	"testing"
)

func TestNewDefaultEntries(t *testing.T) {
	o := NewDefaultEntries()
	o.Common.Debug = true
	o.Connection.Port = 5000
	o.Connection.Hostname = "10.23.14.50"
	o.Connection.Password = "admin123"
	o.Connection.Username = "root"
	o.CommonOptionEntries.Output_directory_param = "outout"
	o.Filter.DB = "test2"
	o.CommonFilter.TablesList = "test2.t2"
	o.StartDump()

}
