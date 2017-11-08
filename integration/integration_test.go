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

package integration

import (
	"net"
	"testing"
	"time"

	"github.com/coreos/zetcd"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	acl = zk.WorldACL(zk.PermAll)
)

func TestGetRoot(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, _, err := c.Get("/"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEphemeral(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	c, _, err := zk.Connect([]string{zkclus.Addr()}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Use closure to capture c because it's overwritten with a new connection later.
	defer func() { c.Close() }()

	if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create("/abc/def", []byte(""), zk.FlagEphemeral, acl); err != nil {
		t.Fatal(err)
	}
	// Confirm there's an ephemeral owner after updating an ephemeral node.
	if _, err := c.Set("/abc/def", []byte("123"), -1); err != nil {
		t.Fatal(err)
	}
	_, s, err := c.Get("/abc/def")
	if err != nil {
		t.Fatal(err)
	}
	if s.EphemeralOwner == 0 {
		t.Fatal("expected ephemeral owner")
	}

	// Confirm parent directory can be deleted after session expiration.
	c.Close()
	time.Sleep(3 * time.Second)
	c, _, err = zk.Connect([]string{zkclus.Addr()}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	keys, _, cerr := c.Children("/abc")
	if cerr != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected no keys in /abc, got %v", keys)
	}
	if err := c.Delete("/abc", -1); err != nil {
		t.Fatal(err)
	}
}

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

func TestWatchData(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte("data1"), 0, acl); err != nil {
			t.Fatal(err)
		}
		dat, _, evc, werr := c.GetW("/abc")
		if werr != nil {
			t.Fatal(werr)
		}
		if string(dat) != "data1" {
			t.Fatalf("expected data %q, got %q", "data1", string(dat))
		}
		if _, err := c.Create("/def", []byte("x"), 0, acl); err != nil {
			t.Fatal(err)
		}
		// wrong rev on watch waits were causing this to stall
		if _, _, err := c.Get("/abc"); err != nil {
			t.Fatal(err)
		}
		select {
		case ev := <-evc:
			t.Errorf("expected no event, got %+v", ev)
		default:
		}
		if _, err := c.Set("/abc", []byte("a"), -1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-evc:
		case <-time.After(time.Second):
			t.Errorf("no event after 1s")
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
		// Test node creation with a trailing / character. This should produce
		// a subdirectory, with the parent node acting as the counter.
		// This will create node#2 in /abc/ (but not sequentially named)
		if _, err := c.Create("/abc/defg", []byte("x"), 0, acl); err != nil {
			t.Fatal(err)
		}
		s, err = c.Create("/abc/defg/", []byte("x"), zk.FlagSequence, acl)
		if err != nil {
			t.Fatal(err)
		}
		if s != "/abc/defg/0000000000" {
			t.Fatalf("got %s, expected /abc/defg/%010d", s, 0)
		}
		if _, _, err = c.Get("/abc/defg/0000000000"); err != nil {
			t.Fatal(err)
		}
		// Create the next node and check the increement is +1. It should not be
		// getting stored under /abc 's counter.
		_, err = c.Create("/abc/defg/", []byte("x"), zk.FlagSequence, acl)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = c.Get("/abc/defg/0000000001"); err != nil {
			t.Fatal(err)
		}
		// Check that creating /abc/def increments to 3 (i.e. the above did not
		// get stored under the parent node somehow.
		// This will create node#3 in /abc/
		s, err = c.Create("/abc/def", []byte("x"), zk.FlagSequence, acl)
		if err != nil {
			t.Fatal(err)
		}
		if s != "/abc/def0000000003" {
			t.Fatalf("got %s, expected /abc/def%010d", s, 2)
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

		_, _, ch, werr = c.GetW("/abc")
		if werr != nil {
			t.Fatal(werr)
		}
		if err := c.Delete("/abc", -1); err != nil {
			t.Fatal(err)
		}
		select {
		case ev := <-ch:
			if ev.Path != "/abc" || ev.Type != zk.EventNodeDeleted {
				t.Fatalf("expected /abc, got %+v", ev)
			}
			// {Type:EventNodeDeleted State:Unknown Path:/abc Err:<nil> Server:}
		case <-time.After(5 * time.Second):
			t.Fatalf("took too long to get data update")
		}

	})
}

func TestRejectDeleteWithChildren(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/def", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete("/abc", -1); err != zk.ErrNotEmpty {
			t.Fatalf("expected error %q, got %q", zk.ErrNotEmpty, err)
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
		if ok, _, err := c.Exists("/"); err != nil || !ok {
			t.Errorf("expected /, got err=%v, ok=%v", err, ok)
		}
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if ok, _, err := c.Exists("/"); err != nil || !ok {
			t.Errorf("expected /, got err=%v, ok=%v", err, ok)
		}
		if ok, _, err := c.Exists("/abc"); err != nil || !ok {
			t.Errorf("expected it to exist %v %v", err, ok)
		}
		if ok, _, err := c.Exists("/ab"); ok {
			t.Errorf("expected it to not exist %v %v", err, ok)
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
			case ev := <-ch:
				if ev.Path != "/abc" || ev.Type != zk.EventNodeDataChanged {
					t.Fatalf("expected /abc, got %+v", ev)
				}
				// {EventNodeDataChanged Unknown /abc <nil> }
			case <-time.After(time.Second):
				t.Fatalf("set data event timed out")
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
		if _, err := c.Create("/abc/def/123", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Create("/abc/def/456", []byte(""), 0, acl); err != nil {
			t.Fatal(err)
		}
		children, _, err := c.Children("/abc/def")
		if err != nil {
			t.Error(err)
		}
		if len(children) != 2 || children[0] != "123" || children[1] != "456" {
			t.Errorf("expected [123 456], got %v", children)
		}
		children, _, err = c.Children("/abc")
		if err != nil {
			t.Error(err)
		}
		if len(children) != 1 {
			t.Errorf("expected /abc/def, got %v", children)
		}
	})
}

func TestChildrenQuota(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		if _, err := c.Create("/abc", []byte(""), 0, acl); err != nil {
			t.Error(err)
		}
		children, _, err := c.Children("/")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) == 1 && children[0] == "abc" {
			t.Skipf("quota not supported yet, got %v", children)
		}
		if len(children) != 2 {
			t.Errorf("expected [abc zookeeper], got %v", children)
		}
		children, _, err = c.Children("/zookeeper")
		if err != nil {
			t.Error(err)
		}
		if len(children) != 1 || children[0] != "quota" {
			t.Skipf("expected [quota], got %v, but no quota support yet", children)
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
		werr := zetcd.ErrInvalidACL
		resp, err := c.Create("/foo", []byte("x"), 0, nil)
		if err == nil {
			t.Fatalf("created with invalid acl %v, wanted %v", resp, werr)
		}
		if err.Error() != werr.Error() {
			t.Fatalf("got err %v, wanted %v", err, werr)
		}
	})
}

func TestCreateInvalidPath(t *testing.T) {
	runTest(t, func(t *testing.T, c *zk.Conn) {
		// werr := zetcd.ErrInvalidACL
		werr := "zk: invalid path"
		resp, err := c.Create("/.", []byte("x"), 0, nil)
		if err == nil {
			t.Errorf("created with invalid path %v, wanted %v", resp, werr)
		} else if err.Error() != werr {
			t.Errorf("got err %v, wanted %v", err, werr)
		}

		// this is ErrBadArguments under the hood but the zk library
		// doesn't recognize it so it goes to ErrUnknown
		// werr = zetcd.ErrUnknown
		sresp, err := c.Set("/.", []byte("x"), -1)
		if err == nil {
			t.Errorf("created with invalid path %v, wanted %v", sresp, werr)
		} else if err.Error() != werr {
			t.Errorf("got err %v, wanted %v", err, werr)
		}

		// werr = zetcd.ErrNoNode
		err = c.Delete("/.", -1)
		if err == nil {
			t.Errorf("created with invalid path %v, wanted %v", sresp, werr)
		} else if err.Error() != werr {
			t.Errorf("got err %v, wanted %v", err, werr)
		}

		// werr = zetcd.ErrNoNode
		_, _, err = c.Get("/.")
		if err == nil {
			t.Errorf("got err %v, wanted %v", err, werr)
		} else if err.Error() != werr {
			t.Errorf("got err %v, wanted %v", err, werr)
		}
	})
}

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

func TestMultiOp(t *testing.T) { runTest(t, testMultiOp) }

func testMultiOp(t *testing.T, c *zk.Conn) {
	// test create+create => same zxid
	ops := []interface{}{
		&zk.CreateRequest{Path: "/abc", Data: []byte("foo"), Acl: acl},
		&zk.CreateRequest{Path: "/def", Data: []byte("bar"), Acl: acl},
	}
	if _, err := c.Multi(ops...); err != nil {
		t.Fatal(err)
	}
	_, s1, err1 := c.Get("/abc")
	if err1 != nil {
		t.Fatal(err1)
	}
	_, s2, err2 := c.Get("/def")
	if err2 != nil {
		t.Fatal(err2)
	}
	if s1.Czxid != s2.Czxid || s1.Mzxid != s2.Mzxid {
		t.Fatalf("expected zxids in %+v to match %+v", *s1, *s2)
	}
	// test 2 create, 1 delete
	ops = []interface{}{
		&zk.CreateRequest{Path: "/foo", Data: []byte("foo"), Acl: acl},
		&zk.DeleteRequest{Path: "/def"},
		&zk.CreateRequest{Path: "/bar", Data: []byte("foo"), Acl: acl},
	}
	if _, err := c.Multi(ops...); err != nil {
		t.Fatal(err)
	}
	_, s1, err1 = c.Get("/foo")
	if err1 != nil {
		t.Fatal(err1)
	}
	if _, _, err := c.Get("/def"); err == nil || err.Error() != zetcd.ErrNoNode.Error() {
		t.Fatalf("expected %v, got %v", zetcd.ErrNoNode, err)
	}
	_, s2, err2 = c.Get("/bar")
	if err2 != nil {
		t.Fatal(err1)
	}
	if s1.Czxid != s2.Czxid || s1.Mzxid != s2.Mzxid {
		t.Fatalf("expected zxids in %+v to match %+v", *s1, *s2)
	}
	// test create on key that already exists
	ops = []interface{}{
		&zk.CreateRequest{Path: "/foo", Data: []byte("foo"), Acl: acl},
	}
	if _, err := c.Multi(ops...); err == nil || err.Error() != zetcd.ErrAPIError.Error() {
		t.Fatalf("expected %v, got %v", zetcd.ErrAPIError, err)
	}
	// test create+delete on same key == no key
	ops = []interface{}{
		&zk.CreateRequest{Path: "/create-del", Data: []byte("foo"), Acl: acl},
		&zk.DeleteRequest{Path: "/create-del"},
		// update foo to get version=1
		&zk.SetDataRequest{Path: "/foo", Data: []byte("bar")},
	}
	resp, err := c.Multi(ops...)
	if err != nil {
		t.Fatal(err)
	}
	if resp[0].String != "/create-del" || resp[1].String != "" {
		t.Fatalf("expected /create-del, ''; got %+v", resp)
	}
	_, _, err = c.Get("/create-del")
	if err == nil || err.Error() != zetcd.ErrNoNode.Error() {
		t.Fatalf("expected %v, got %v", zetcd.ErrNoNode, err)
	}
	// test version check mismatch
	ops = []interface{}{
		&zk.CreateRequest{Path: "/test1", Data: []byte("foo"), Acl: acl},
		&zk.CheckVersionRequest{Path: "/foo", Version: 2},
	}
	_, err = c.Multi(ops...)
	if err == nil || err.Error() != zetcd.ErrAPIError.Error() {
		t.Fatalf("expected %v, got %v", zetcd.ErrAPIError, err)
	}
	if _, s1, err = c.Get("/test1"); err == nil || err.Error() != zetcd.ErrNoNode.Error() {
		t.Fatalf("expected %v, got (%v,%v)", zetcd.ErrNoNode, s1, err)
	}
	// test version check match
	ops = []interface{}{
		&zk.CheckVersionRequest{Path: "/foo", Version: 1},
		&zk.CreateRequest{Path: "/test1", Data: []byte("foo"), Acl: acl},
		&zk.CreateRequest{Path: "/test2", Data: []byte("foo"), Acl: acl},
	}
	if _, err = c.Multi(ops...); err != nil {
		t.Fatal(err)
	}
	if _, s1, err1 = c.Get("/test1"); err1 != nil {
		t.Fatal(err1)
	}
	if _, s2, err2 = c.Get("/test2"); err2 != nil {
		t.Fatal(err2)
	}
	if s1.Czxid != s2.Czxid || s1.Mzxid != s2.Mzxid {
		t.Fatalf("expected zxids in %+v to match %+v", *s1, *s2)
	}
	// test version missing key
	ops = []interface{}{
		&zk.CheckVersionRequest{Path: "/missing-key", Version: 0},
	}
	if _, err = c.Multi(ops...); err == nil || err.Error() != zetcd.ErrAPIError.Error() {
		t.Fatalf("expected %v, got %v", zetcd.ErrAPIError, err)
	}
	// test empty operation list
	if resp, err = c.Multi(); err != nil || len(resp) != 0 {
		t.Fatalf("expected empty resp, got (%+v,%v)", resp, err)
	}
	// test setdata if path exists
	ops = []interface{}{
		&zk.SetDataRequest{Path: "/foo", Data: []byte("foo"), Version: -1},
		&zk.CreateRequest{Path: "/set-txn", Data: []byte("foo"), Acl: acl},
	}
	if _, err = c.Multi(ops...); err != nil {
		t.Fatal(err)
	}
	if _, s1, err1 = c.Get("/foo"); err1 != nil {
		t.Fatal(err1)
	}
	if _, s2, err2 = c.Get("/set-txn"); err2 != nil {
		t.Fatal(err2)
	}
	if s1.Mzxid != s2.Mzxid {
		t.Fatalf("expected zxids in %+v to match %+v", *s1, *s2)
	}
}

func runTest(t *testing.T, f func(*testing.T, *zk.Conn)) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	c, _, err := zk.Connect([]string{zkclus.Addr()}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	f(t, c)
}
