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

package xchk

import (
	"fmt"

	"github.com/etcd-io/zetcd"
)

// authConn implements an AuthConn that can fork off xchked AuthConns
type authConn struct {
	zka     zetcd.AuthConn
	workers []*authConnWorker
}

func newAuthConn(zka zetcd.AuthConn) *authConn {
	return &authConn{zka: zka}
}

func (ac *authConn) Read() (*zetcd.AuthRequest, error) { return ac.zka.Read() }

// Write waits for the worker writes and returns a new conn if matching.
func (ac *authConn) Write(ar zetcd.AuthResponse) (zetcd.Conn, error) {
	zkc, cerr := ac.zka.Write(ar)
	if cerr != nil {
		return nil, cerr
	}
	conn, workers := newConn(zkc, len(ac.workers))
	for i, w := range ac.workers {
		w.connc <- workers[i]
	}
	return conn, nil
}

func (ac *authConn) Close() {
	ac.zka.Close()
	for _, w := range ac.workers {
		w.Close()
	}
}

// authConnWorker implements an AuthConn that is xchked by an authConn
type authConnWorker struct {
	reqc  chan *zetcd.AuthRequest
	respc chan *zetcd.AuthResponse
	connc chan zetcd.Conn
}

// worker creates a clone of the auth conn
func (ac *authConn) worker() *authConnWorker {
	acw := &authConnWorker{
		reqc:  make(chan *zetcd.AuthRequest, 1),
		respc: make(chan *zetcd.AuthResponse, 1),
		connc: make(chan zetcd.Conn),
	}
	ac.workers = append(ac.workers, acw)
	return acw
}

func (acw *authConnWorker) Read() (*zetcd.AuthRequest, error) {
	if req := <-acw.reqc; req != nil {
		return req, nil
	}
	return nil, fmt.Errorf("lost request")
}

func (acw *authConnWorker) Write(ar zetcd.AuthResponse) (zetcd.Conn, error) {
	acw.respc <- &ar
	c := <-acw.connc
	if c == nil {
		return nil, fmt.Errorf("xchk error")
	}
	return c, nil
}

func (acw *authConnWorker) Close() {
	if acw.respc != nil {
		close(acw.respc)
		close(acw.reqc)
		acw.respc = nil
	}
}
