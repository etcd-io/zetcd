// +build !zkdocker,!xchk

package integration

import (
	"net"
	"testing"
)

func TestRUOK(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	conn, err := net.Dial("tcp", zkclus.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("ruok")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := conn.Read(buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "imok" {
		t.Fatalf(`expected "imok", got %q`, string(buf))
	}
}
