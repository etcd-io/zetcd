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

	"golang.org/x/net/context"
)

// client represents a client that connects to a zk server.
type client struct {
	ctx   context.Context
	zkc   net.Conn
	outc  chan []byte
	readc chan ZKResponse
	xids  map[Xid]Op
	mu    sync.RWMutex

	// stopc is closed to shutdown session
	stopc chan struct{}
	// donec is closed to signal session is torn down
	donec chan struct{}
}

type Client interface {
	Send(xid Xid, req interface{}) error
	Read() <-chan ZKResponse
	StopNotify() <-chan struct{}
	Close()
}

type ZKResponse struct {
	// data to be send back to the proxy's client

	Hdr  *ResponseHeader
	Resp interface{}

	// Err is from transmission errors, etc
	Err error
}

func NewClient(ctx context.Context, zk net.Conn) Client {
	outc := make(chan []byte, 16)
	c := &client{
		ctx:   ctx,
		zkc:   zk,
		outc:  outc,
		readc: make(chan ZKResponse),
		xids:  make(map[Xid]Op),
		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}

	go func() {
		defer close(c.readc)
		xid2op := func(xid Xid) interface{} { return c.xid2resp(xid) }
		for {
			hdr, resp, err := readRespOp(c.zkc, xid2op)
			if hdr != nil {
				c.ackXid(hdr.Xid)
			}
			select {
			case c.readc <- ZKResponse{hdr, resp, err}:
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

func (c *client) ackXid(xid Xid) {
	if xid == -1 {
		return
	}
	c.mu.Lock()
	delete(c.xids, xid)
	c.mu.Unlock()
}

func (c *client) xid2resp(xid Xid) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	op, ok := c.xids[xid]
	if !ok {
		return fmt.Sprintf("unexpected xid %x", xid)
	}
	return op2resp(op)
}

// Read receives zookeeper responses.
func (c *client) Read() <-chan ZKResponse { return c.readc }

// Send sends a zookeeper request.
func (c *client) Send(xid Xid, req interface{}) error {
	hdr := &requestHeader{Xid: xid, Opcode: req2op(req)}
	if hdr.Opcode == opInvalid {
		return ErrAPIError
	}

	buf := make([]byte, 2*1024*1024)

	n1, err1 := encodePacket(buf[4:], hdr)
	if err1 != nil {
		return err1
	}
	pktlen := n1
	n2, err2 := encodePacket(buf[4+n1:], req)
	if err2 != nil {
		return err2
	}
	pktlen += n2

	// record the xid
	c.mu.Lock()
	c.xids[xid] = hdr.Opcode
	c.mu.Unlock()

	binary.BigEndian.PutUint32(buf[:4], uint32(pktlen))
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case c.outc <- buf[:4+pktlen]:
	case <-c.donec:
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
	return nil
}

func (c *client) Close() {
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

func (c *client) StopNotify() <-chan struct{} { return c.donec }
