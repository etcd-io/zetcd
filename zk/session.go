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

package zk

import (
	"fmt"
	"net"
	"sync"

	"github.com/coreos/zetcd"
	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type session struct {
	zetcd.Conn
	zetcd.Watches
	zkc     zetcd.Client
	connReq zetcd.ConnectRequest
	sid     zetcd.Sid

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	futures map[zetcd.Xid]chan zetcd.ZKResponse
}

func (s *session) Sid() zetcd.Sid   { return s.sid }
func (s *session) ZXid() zetcd.ZXid { return 111111 }

func (s *session) ConnReq() zetcd.ConnectRequest { return s.connReq }
func (s *session) Backing() interface{}          { return s }

func (s *session) Close() {
	s.Conn.Close()
	s.zkc.Close()
}

func newSession(servers []string, zka zetcd.AuthConn) (*session, error) {
	defer zka.Close()
	glog.V(6).Infof("newSession(%s)", servers)
	req := zetcd.ConnectRequest{}
	areq, err := zka.Read()
	if err != nil {
		return nil, err
	}
	if req.ProtocolVersion != 0 {
		panic("unhandled req stuff!")
	}
	// create connection to zk server based on 'servers'
	zkconn, err := net.Dial("tcp", servers[0])
	if err != nil {
		glog.V(6).Infof("failed to dial (%v)", err)
		return nil, err
	}
	// send connection request
	if err = zetcd.WritePacket(zkconn, areq.Req); err != nil {
		glog.V(6).Infof("failed to write connection request (%v)", err)
		zkconn.Close()
		return nil, err
	}
	// pipe back connectino result
	resp := zetcd.ConnectResponse{}
	if err := zetcd.ReadPacket(zkconn, &resp); err != nil {
		glog.V(6).Infof("failed to read connection response (%v)", err)
		return nil, err
	}
	// pass response back to proxy
	zkc, aerr := zka.Write(zetcd.AuthResponse{Resp: &resp})
	if zkc == nil || aerr != nil {
		zkconn.Close()
		return nil, aerr
	}

	ctx, cancel := context.WithCancel(context.Background())
	glog.V(6).Infof("auth resp OK (%+v)", resp)

	s := &session{
		Conn:    zkc,
		zkc:     zetcd.NewClient(ctx, zkconn),
		connReq: req,
		sid:     resp.SessionID,
		ctx:     ctx,
		cancel:  cancel,
		futures: make(map[zetcd.Xid]chan zetcd.ZKResponse),
	}
	go s.recvLoop()
	return s, nil
}

func (s *session) future(xid zetcd.Xid, op interface{}) <-chan zetcd.ZKResponse {
	ch := make(chan zetcd.ZKResponse, 1)
	if s.futures == nil {
		glog.V(6).Infof("futuresClosed=%+v", op)
		ch <- zetcd.ZKResponse{Err: fmt.Errorf("closed")}
		return ch
	}
	s.mu.Lock()
	s.futures[xid] = ch
	s.mu.Unlock()
	if err := s.zkc.Send(xid, op); err != nil {
		ch <- zetcd.ZKResponse{Err: err}
		s.mu.Lock()
		delete(s.futures, xid)
		s.mu.Unlock()
		return ch
	}
	glog.V(6).Infof("waitFutureSendResp=%+v", op)
	return ch
}

// recvLoop forwards responses from the real zk server to the zetcd connection.
func (s *session) recvLoop() {
	defer func() {
		s.mu.Lock()
		for _, ch := range s.futures {
			close(ch)
		}
		s.futures = nil
		s.mu.Unlock()
		s.cancel()
	}()
	for resp := range s.zkc.Read() {
		if resp.Err != nil {
			glog.V(6).Infof("zk/zkresp=Err(%v)", resp.Err)
			return
		}
		glog.V(6).Infof("zk/zkresp=(%+v,%+v)", *resp.Hdr, resp.Resp)
		s.mu.Lock()
		if ch := s.futures[resp.Hdr.Xid]; ch != nil {
			ch <- resp
			delete(s.futures, resp.Hdr.Xid)
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		// out of band requests (i.e., watches)
		var r interface{}
		if resp.Hdr.Err != 0 {
			r = &resp.Hdr.Err
		} else {
			r = resp.Resp
		}

		glog.V(6).Infof("zk/zkSessOOB=%+v %+v", resp.Hdr, r)
		s.Send(resp.Hdr.Xid, resp.Hdr.Zxid, r)
	}
}
