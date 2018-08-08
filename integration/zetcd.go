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
	"net"
	"testing"

	"github.com/etcd-io/zetcd"

	"github.com/coreos/etcd/integration"
)

type zetcdCluster struct {
	zkClientAddr string

	etcdClus *integration.ClusterV3
	cancel   func()
	donec    <-chan struct{}
}

func NewZetcdCluster(t *testing.T) *zetcdCluster {
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	donec := make(chan struct{})

	// TODO use unix socket
	ln, err := net.Listen("tcp", ":30000")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		defer close(donec)
		c := clus.RandClient()
		zetcd.Serve(c.Ctx(), ln, zetcd.NewAuth(c), zetcd.NewZK(c))
	}()
	return &zetcdCluster{
		zkClientAddr: "127.0.0.1:30000",

		etcdClus: clus,
		cancel:   func() { ln.Close() },
		donec:    donec,
	}
}

func (zc *zetcdCluster) Addr() string { return zc.zkClientAddr }

func (zc *zetcdCluster) Close(t *testing.T) {
	zc.etcdClus.Terminate(t)
	zc.cancel()
	<-zc.donec
}
