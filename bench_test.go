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
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	zkAddr    = "127.0.0.1:2181"
	zetcdAddr = "127.0.0.1:2182"
)

var acl = zk.WorldACL(zk.PermAll)

func init() { zk.DefaultLogger = log.New(io.Discard, "", 0) }

func benchGet(b *testing.B, addr string) {
	c, _, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	c.Create("/abc", []byte("abc"), 0, acl)
	for i := 0; i < b.N; i++ {
		if _, _, gerr := c.Get("/abc"); gerr != nil {
			b.Fatal(err)
		}
	}
}

func benchConnGet(b *testing.B, addr string) {
	for i := 0; i < b.N; i++ {
		c, _, err := zk.Connect([]string{addr}, time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, gerr := c.Get("/abc"); gerr != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func benchCreateSet(b *testing.B, addr string) {
	c, _, err := zk.Connect([]string{addr}, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("/%d", i)
		v := fmt.Sprintf("%v", time.Now())
		c.Create(s, []byte(v), 0, acl)
		c.Set("/", []byte(v), -1)
	}
}

func BenchmarkZetcdGet(b *testing.B) { benchGet(b, zetcdAddr) }
func BenchmarkZKGet(b *testing.B)    { benchGet(b, zkAddr) }

func BenchmarkZetcdConnGet(b *testing.B) { benchConnGet(b, zetcdAddr) }
func BenchmarkZKConnGet(b *testing.B)    { benchConnGet(b, zkAddr) }

func BenchmarkZetcdCreateSet(b *testing.B) { benchCreateSet(b, zetcdAddr) }
func BenchmarkZKCreateSet(b *testing.B)    { benchCreateSet(b, zkAddr) }
