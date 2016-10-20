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

	"github.com/golang/glog"
)

// AuthConn transfers zookeeper handshaking for establishing a session
type AuthConn interface {
	Read() (*AuthRequest, error)
	Write(AuthResponse) (Conn, error)
	Close()
}

type AuthResponse struct {
	Resp *ConnectResponse
	// TODO: add four letter response
}

type AuthRequest struct {
	Req *ConnectRequest
	// TODO: add four letter commands
}

type authConn struct {
	c net.Conn
}

func NewAuthConn(c net.Conn) AuthConn { return &authConn{c} }

func (ac *authConn) Read() (*AuthRequest, error) {
	req := &ConnectRequest{}
	if err := ReadPacket(ac.c, req); err != nil {
		glog.V(6).Infof("error reading connection request (%v)", err)
		return nil, err
	}
	glog.V(6).Infof("auth(%+v)", req)
	return &AuthRequest{req}, nil
}

func (ac *authConn) Write(ar AuthResponse) (Conn, error) {
	if err := WritePacket(ac.c, ar.Resp); err != nil {
		return nil, err
	}
	zkc := NewConn(ac.c)
	ac.c = nil
	return zkc, nil
}

func (ac *authConn) Close() {
	if ac.c != nil {
		ac.c.Close()
	}
}
