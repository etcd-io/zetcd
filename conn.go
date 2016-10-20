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
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/golang/glog"
)

type Conn interface {
	Send(xid Xid, zxid ZXid, resp interface{}) error
	Read() <-chan ZKRequest
	StopNotify() <-chan struct{}
	Close()
}

type conn struct {
	zkc   net.Conn
	outc  chan []byte
	readc chan ZKRequest
	mu    sync.RWMutex

	// stopc is closed to shutdown session
	stopc chan struct{}
	// donec is closed to signal session is torn down
	donec chan struct{}
}

type ZKRequest struct {
	xid Xid
	req interface{}
	err error
}

func (zk *ZKRequest) String() string {
	if zk.req != nil {
		return fmt.Sprintf("{xid:%v req:%T:%+v}", zk.xid, zk.req, zk.req)
	}
	if zk.err != nil {
		return fmt.Sprintf("{xid:%v err:%q}", zk.xid, zk.err)
	}
	return fmt.Sprintf("{xid:%v err:%q}", zk.xid, zk.err)
}

func NewConn(zk net.Conn) Conn {
	outc := make(chan []byte, 16)
	c := &conn{
		zkc:   zk,
		outc:  outc,
		readc: make(chan ZKRequest),
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}

	go func() {
		defer close(c.readc)
		for {
			xid, req, err := readReqOp(c.zkc)
			select {
			case c.readc <- ZKRequest{xid, req, err}:
				if err != nil {
					return
				}
			case <-c.stopc:
				return
			case <-c.donec:
				return
			}
		}
	}()

	go func() {
		defer close(c.donec)
		for msg := range outc {
			if _, err := c.zkc.Write(msg); err != nil {
				return
			}
		}
	}()

	return c
}

func (c *conn) Read() <-chan ZKRequest { return c.readc }

func (c *conn) Send(xid Xid, zxid ZXid, resp interface{}) error {
	buf := make([]byte, 2*1024*1024)
	hdr := &ResponseHeader{Xid: xid, Zxid: zxid, Err: errOk}

	_, isEv := resp.(*WatcherEvent)
	if isEv {
		hdr.Xid = -1
	}

	ec, hasErr := resp.(*ErrCode)
	if hasErr {
		hdr.Err = *ec
	}
	n1, err1 := encodePacket(buf[4:], hdr)
	if err1 != nil {
		return err1
	}
	pktlen := n1
	if !hasErr {
		n2, err2 := encodePacket(buf[4+n1:], resp)
		if err2 != nil {
			return err2
		}
		pktlen += n2
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(pktlen))
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case c.outc <- buf[:4+pktlen]:
		glog.V(9).Infof("conn.Send(xid=%v, zxid=%v, %+v)", xid, zxid, resp)
	case <-c.donec:
	}
	return nil
}

func (c *conn) Close() {
	c.mu.Lock()
	if c.outc != nil {
		close(c.stopc)
		close(c.outc)
		c.outc = nil
		c.zkc.Close()
	}
	c.mu.Unlock()
	<-c.donec
}

func (c *conn) StopNotify() <-chan struct{} { return c.donec }
