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

// +build zkdocker

package integration

import (
	"net"
	"testing"
	"time"
)

var zkContainerName = "zetcd-zk-test"
var zkDockerFile = "../docker/zk/Dockerfile"

type zkDockerCluster struct {
	zkClientAddr string
	c            *Container
}

func NewZKDockerCluster() (*zkDockerCluster, error) {
	c, err := NewContainer(zkContainerName, zkDockerFile, []string{"2181/tcp"})
	if err != nil {
		return nil, err
	}
	// poll until zk server is available
	for {
		time.Sleep(200 * time.Millisecond)
		conn, cerr := net.Dial("tcp", "127.0.0.1:2181")
		if cerr != nil {
			return nil, cerr
		}
		if _, werr := conn.Write([]byte("ruok")); werr != nil {
			conn.Close()
			continue
		}
		imok := make([]byte, 4)
		if _, rerr := conn.Read(imok); rerr != nil {
			conn.Close()
			continue
		}
		conn.Close()
		if string(imok) == "imok" {
			break
		}
	}
	return &zkDockerCluster{zkClientAddr: "127.0.0.1:2181", c: c}, nil
}

func (zc *zkDockerCluster) Addr() string { return zc.zkClientAddr }

func (zc *zkDockerCluster) Close(t *testing.T) {
	if err := zc.c.Close(); err != nil {
		t.Fatal(err)
	}
}
