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
	"sync"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type acceptHandler func(conn net.Conn, auth AuthFunc, zk ZKFunc)

// Serve will serve multiple sessions in concurrently.
func Serve(ctx context.Context, ln net.Listener, auth AuthFunc, zk ZKFunc) {
	serveByHandler(handleSessionSerialRequests, ctx, ln, auth, zk)
}

// ServeSerial has at most one inflight request at a time so two servers can be
// reliably cross checked.
func ServeSerial(ctx context.Context, ln net.Listener, auth AuthFunc, zk ZKFunc) {
	serveByHandler(newHandleGlobalSerialRequests(), ctx, ln, auth, zk)
}

func newHandleGlobalSerialRequests() acceptHandler {
	var mu sync.Mutex
	return func(conn net.Conn, auth AuthFunc, zk ZKFunc) {
		s, zke, serr := openSession(conn, auth, zk)
		if serr != nil {
			return
		}
		defer s.Close()
		glog.V(9).Infof("serving global serial session requests on s=%+v", s)
		for zkreq := range s.Read() {
			mu.Lock()
			err := serveRequest(s, zke, zkreq)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func handleSessionSerialRequests(conn net.Conn, auth AuthFunc, zk ZKFunc) {
	s, zke, serr := openSession(conn, auth, zk)
	if serr != nil {
		return
	}
	defer s.Close()
	glog.V(9).Infof("serving serial session requests on id=%x", s.Sid())
	for zkreq := range s.Read() {
		if err := serveRequest(s, zke, zkreq); err != nil {
			return
		}
	}
}

func serveRequest(s Session, zke ZK, zkreq ZKRequest) error {
	glog.V(9).Infof("zkreq=%v", &zkreq)
	if zkreq.err != nil {
		return zkreq.err
	}
	zkresp := DispatchZK(zke, zkreq.xid, zkreq.req)
	if zkresp.Err != nil {
		glog.V(9).Infof("dispatch error %v", zkresp.Err)
		return zkresp.Err
	}
	if zkresp.Hdr.Err == 0 {
		s.Send(zkresp.Hdr.Xid, zkresp.Hdr.Zxid, zkresp.Resp)
	} else {
		s.Send(zkresp.Hdr.Xid, zkresp.Hdr.Zxid, &zkresp.Hdr.Err)
	}
	return nil
}

func openSession(conn net.Conn, auth AuthFunc, zk ZKFunc) (Session, ZK, error) {
	glog.V(6).Infof("accepted remote connection %q", conn.RemoteAddr())
	s, serr := auth(NewAuthConn(conn))
	if serr != nil {
		return nil, nil, serr
	}
	zke, zkerr := zk(s)
	if zkerr != nil {
		s.Close()
		return nil, nil, zkerr
	}
	return s, zke, nil
}

func serveByHandler(h acceptHandler, ctx context.Context, ln net.Listener, auth AuthFunc, zk ZKFunc) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			glog.V(5).Infof("Accept()=%v", err)
		} else {
			go h(conn, auth, zk)
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

/*

Special session handlers for debugging and future reference:

func newHandleGlobalSerialSessions(ch acceptHandler) acceptHandler {
	var mu sync.Mutex
	return func(conn net.Conn, auth AuthFunc, zk ZKFunc) {
		mu.Lock()
		defer mu.Unlock()
		ch(conn, auth, zk)
	}
}

func handleSessionConcurrentRequests(conn net.Conn, auth AuthFunc, zk ZKFunc) {
	s, zke, serr := openSession(conn, auth, zk)
	if serr != nil {
		return
	}
	defer s.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	glog.V(9).Infof("serving concurrent session requests on id=%x", s.Sid())
	for zkreq := range s.Read() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := serveRequest(s, zke, zkreq); err != nil {
				return
			}
		}()
	}
	wg.Wait()
}

func newHandleLogClose(ch acceptHandler) acceptHandler {
	return func(conn net.Conn, auth AuthFunc, zk ZKFunc) {
		glog.V(6).Infof("closing remote connection %q", conn.RemoteAddr())
		ch(conn, auth, zk)
	}
}
*/
