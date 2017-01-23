// Copyright 2016 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zetcd

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	acl = zk.WorldACL(zk.PermAll)
)

func TestDirStat(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}

		keys1, stat1, cerr1 := c.Children("/abc")
		if cerr1 != nil {
			t.Fatal(cerr1)
		}
		if len(keys1) != 0 {
			t.Fatalf("expected 0 keys, got %v", keys1)
		}

		if _, err := c.Create("/abc/def", []byte("data"), 0, acl); err != nil {
			t.Fatal(err)
		}

		keys2, stat2, cerr2 := c.Children("/abc")
		if cerr2 != nil {
			t.Fatal(cerr2)
		}
		if len(keys2) != 1 {
			t.Fatalf("expected {\"/abc/def\"} key, got %v", keys2)
		}
		if stat2.Mzxid != stat1.Mzxid {
			t.Fatalf("expected mzxid=%d, got %d", stat1.Mzxid, stat2.Mzxid)
		}
		if stat2.Pzxid <= stat1.Pzxid {
			t.Fatalf("stat2.Pzxid=%d <= stat1.Pzxid=%d", stat2.Pzxid, stat1.Pzxid)
		}

		if err := c.Delete("/abc/def", -1); err != nil {
			t.Fatal(err)
		}

		keys3, stat3, cerr3 := c.Children("/abc")
		if cerr3 != nil {
			t.Fatal(cerr3)
		}
		if len(keys3) != 0 {
			t.Fatalf("expected no keys, got %v", keys2)
		}
		if stat3.Mzxid != stat1.Mzxid {
			t.Fatalf("expected mzxid=%d, got %d", stat1.Mzxid, stat3.Mzxid)
		}
		if stat3.Pzxid <= stat2.Pzxid {
			t.Fatalf("stat3.Pzxid=%d <= stat2.Pzxid=%d", stat3.Pzxid, stat2.Pzxid)
		}
	})
}

func TestCreateGet(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, _, err := c.Get("/abc"); err == nil {
			t.Fatalf("expected error on getting absent /abc")
		}
		if _, err := c.Create("/foo/bar", []byte("x"), 0, acl); err == nil {
			t.Fatalf("expected error on creating /foo/bar without /foo")
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err == nil {
			t.Fatalf("don't allow double create")
		}
		if _, _, err := c.Get("/abc"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err == nil {
			t.Fatalf("expected error on getting /abc/def")
		}
		if _, err := c.Create("/abc/def", []byte("data2"), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.Get("/abc/def"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCreateSequential(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte("x"), 0, acl); err != nil {
			t.Fatal(err)
		}
		s, err := c.Create("/abc/def", []byte("x"), zk.FlagSequence, acl)
		if err != nil {
			t.Fatal(err)
		}
		if s != "/abc/def0000000000" {
			t.Fatalf("got %s, expected /abc/def%010d", s, 0)
		}
		s, err = c.Create("/abc/def", []byte("x"), zk.FlagSequence, acl)
		if err != nil {
			t.Fatal(err)
		}
		if s != "/abc/def0000000001" {
			t.Fatalf("got %s, expected /abc/def%010d", s, 1)
		}
		if _, _, err = c.Get("/abc/def0000000000"); err != nil {
			t.Fatal(err)
		}
		if _, _, err = c.Get("/abc/def0000000001"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestGetDataW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err != nil {
			t.Fatal(err)
		}
		_, _, ch, werr := c.GetW("/abc")
		if werr != nil {
			t.Fatal(werr)
		}
		select {
		case resp := <-ch:
			t.Fatalf("should block on get channel, got %+v", resp)
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Set("/abc", []byte("a"), -1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("took too long to get data update")
		}
	})
}

func TestSync(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Sync("/abc"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExists(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if ok, _, err := c.Exists("/abc"); err != nil || !ok {
			t.Fatalf("expected it to exist %v %v", err, ok)
		}
		if ok, _, err := c.Exists("/ab"); ok {
			t.Fatalf("expected it to not exist %v %v", err, ok)
		}
	})
}

func TestExistsW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		// test create
		ok, _, ch, err := c.ExistsW("/abc")
		if ok || err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("took too long to get creation exists event")
		}

		// test (multi) set
		for i := 0; i < 2; i++ {
			ok, _, ch, err = c.ExistsW("/abc")
			if !ok || err != nil {
				t.Fatal(err)
			}
			if _, err := c.Set("/abc", []byte("a"), -1); err != nil {
				t.Fatal(err)
			}
			select {
			case <-ch:
				t.Fatalf("set data shouldn't trigger watcher")
			case <-time.After(time.Second):
			}
		}

		// test delete
		ok, _, ch, err = c.ExistsW("/abc")
		if !ok || err != nil {
			t.Fatal(err)
		}
		if err = c.Delete("/abc", -1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("took too long to get deletion exists event")
		}
	})
}

func TestChildren(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/123", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		children, _, err := c.Children("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 {
			t.Fatalf("expected one child, got %v", children)
		}
		children, _, err = c.Children("/abc")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 2 {
			t.Fatalf("expected two children, got %v", children)
		}
	})
}

func TestGetChildrenW(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}

		// watch for /abc/def
		_, _, ch, err := c.ChildrenW("/abc")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
			t.Fatalf("should block")
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("waited to long for new child")
		}

		// watch for /abc/123
		_, _, ch, err = c.ChildrenW("/abc")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
			t.Fatalf("should block")
		case <-time.After(10 * time.Millisecond):
		}
		if _, err := c.Create("/abc/123", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("waited to long for new child")
		}
	})
}

func TestCreateInvalidACL(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		werr := ErrInvalidACL
		resp, err := c.Create("/foo", []byte("x"), 0, nil)
		if err == nil {
			t.Fatalf("created with invalid acl %v, wanted %v", resp, werr)
		}
		if err.Error() != werr.Error() {
			t.Fatalf("got err %v, wanted %v", err, werr)
		}
	})
}

func runTest(t *testing.T, f func(*testing.T, *zk.Conn)) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	c, _, err := zk.Connect([]string{zkclus.zkClientAddr}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	f(t, c)
}

type zkCluster struct {
	etcdClus *integration.ClusterV3

	zkClientAddr string
	cancel       func()
	donec        <-chan struct{}
}

func newZKCluster(t *testing.T) *zkCluster {
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	donec := make(chan struct{})

	// TODO use unix socket
	ln, err := net.Listen("tcp", ":30000")
	if err != nil {
		os.Exit(-1)
	}

	go func() {
		defer close(donec)
		c := clus.RandClient()
		Serve(c.Ctx(), ln, NewAuth(c), NewZK(c))
	}()
	return &zkCluster{
		etcdClus:     clus,
		zkClientAddr: "127.0.0.1:30000",
		cancel:       func() { ln.Close() },
		donec:        donec,
	}
}

func (zkclus *zkCluster) Close(t *testing.T) {
	zkclus.etcdClus.Terminate(t)
	zkclus.cancel()
	<-zkclus.donec
}
