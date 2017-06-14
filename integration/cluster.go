// Copyright 2017 CoreOS, Inc.
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
	"context"
	"net"
	"testing"

	"github.com/coreos/zetcd"
	"github.com/coreos/zetcd/xchk"
	"github.com/coreos/zetcd/zk"
)

type zkCluster interface {
	Addr() string
	Close(t *testing.T)
}

type zkClusterXchk struct {
	zkClientAddr string

	ln        net.Listener
	xerrc     chan error
	oracle    zkCluster
	candidate zkCluster
	cancel    context.CancelFunc
	donec     chan struct{}
}

func NewClusterXChk(oracle, candidate zkCluster) *zkClusterXchk {
	oAuth, oZK := zk.NewAuth([]string{oracle.Addr()}), zk.NewZK()
	cAuth, cZK := zk.NewAuth([]string{candidate.Addr()}), zk.NewZK()
	xerrc := make(chan error, 16)
	xAuth, xZK := xchk.NewAuth(cAuth, oAuth, xerrc), xchk.NewZK(cZK, oZK, xerrc)

	// TODO use unix socket
	ln, err := net.Listen("tcp", ":30001")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	donec := make(chan struct{})
	go func() {
		defer close(donec)
		zetcd.Serve(ctx, ln, xAuth, xZK)
	}()

	return &zkClusterXchk{
		zkClientAddr: "127.0.0.1:30001",
		ln:           ln,
		xerrc:        xerrc,
		oracle:       oracle,
		candidate:    candidate,
		cancel:       cancel,
		donec:        donec,
	}
}

func (zc *zkClusterXchk) Addr() string { return zc.zkClientAddr }

func (zc *zkClusterXchk) Close(t *testing.T) {
	zc.ln.Close()
	zc.cancel()
	<-zc.donec
	zc.candidate.Close(t)
	zc.oracle.Close(t)
	close(zc.xerrc)
}
