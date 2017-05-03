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
	"sync"

	"github.com/coreos/zetcd"
)

type sessCreds struct {
	sid  zetcd.Sid
	pass []byte
}

type sessionPool struct {
	mu sync.Mutex
	// o2cSid maps oracle sids to candidate sids
	o2cSid map[zetcd.Sid]sessCreds
}

func (sp *sessionPool) get(sid zetcd.Sid) (zetcd.Sid, []byte) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sc, ok := sp.o2cSid[sid]; ok {
		return sc.sid, sc.pass
	}
	return 0, nil
}

func (sp *sessionPool) put(osid, csid zetcd.Sid, pwd []byte) {
	sp.mu.Lock()
	sp.o2cSid[osid] = sessCreds{csid, pwd}
	sp.mu.Unlock()
}

func newSessionPool() *sessionPool {
	return &sessionPool{o2cSid: make(map[zetcd.Sid]sessCreds)}
}

// session intercepts Sends so responses can be xchked
type session struct {
	zetcd.Conn
	oracle    zetcd.Session
	candidate zetcd.Session
	req       zetcd.ConnectRequest

	sp *sessionPool
}

func (s *session) Close() {
	s.Conn.Close()
	s.oracle.Close()
	s.candidate.Close()
}

func Auth(sp *sessionPool, zka zetcd.AuthConn, cAuth, oAuth zetcd.AuthFunc) (zetcd.Session, error) {
	xzka := newAuthConn(zka)
	defer xzka.Close()

	var oSession, cSession zetcd.Session
	ozka, czka := xzka.worker(), xzka.worker()
	oerrc, cerrc := make(chan error, 1), make(chan error, 1)

	go func() {
		s, oerr := oAuth(ozka)
		oSession = s
		oerrc <- oerr
	}()
	go func() {
		s, cerr := cAuth(czka)
		cSession = s
		cerrc <- cerr
	}()

	// get client request
	ar, arerr := xzka.Read()
	if arerr != nil {
		return nil, arerr
	}

	// send to oracle as usual
	ozka.reqc <- ar

	// send to candidate patched with right session info
	creq := *ar.Req
	if creq.SessionID != 0 {
		creq.SessionID, creq.Passwd = sp.get(creq.SessionID)
	}
	czka.reqc <- &zetcd.AuthRequest{Req: &creq}

	oresp, cresp := <-ozka.respc, <-czka.respc
	vNil := oresp == nil && cresp == nil
	vVal := oresp != nil && cresp != nil
	if !vNil && !vVal {
		return nil, fmt.Errorf("mismatch %+v vs %+v", oresp, cresp)
	}
	if oresp == nil {
		return nil, fmt.Errorf("bad oracle response")
	}

	// save session info in case of resume
	sp.put(oresp.Resp.SessionID, cresp.Resp.SessionID, cresp.Resp.Passwd)

	xzkc, xerr := xzka.Write(zetcd.AuthResponse{Resp: oresp.Resp})
	oerr, cerr := <-oerrc, <-cerrc
	if xerr != nil || cerr != nil || oerr != nil {
		return nil, fmt.Errorf("err: xchk: %v. oracle: %v. candidate: %v", oerr, cerr)
	}

	return &session{
		Conn:      xzkc,
		oracle:    oSession,
		candidate: cSession,
		req:       *ar.Req,
		sp:        sp,
	}, nil
}

func (s *session) ConnReq() zetcd.ConnectRequest { return s.req }

func (s *session) Backing() interface{} { return s }

func (s *session) Sid() zetcd.Sid { return s.oracle.Sid() }

func (s *session) Wait(rev zetcd.ZXid, path string, evtype zetcd.EventType) { panic("stub") }

func (s *session) Watch(rev zetcd.ZXid, xid zetcd.Xid, path string, evtype zetcd.EventType, cb func(zetcd.ZXid)) {
	panic("stuB")
}

func (s *session) ZXid() zetcd.ZXid { panic("uh") }
