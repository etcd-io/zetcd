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

package main

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	flags = int32(0)
	acl   = zk.WorldACL(zk.PermAll)
)

func main() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second)
	if err != nil {
		panic(err)
	}

	//	benchMem(conn)
	benchPut(conn)
}

func benchMem(conn *zk.Conn) {
	val := make([]byte, 128)

	for i := 0; i < 1000000; i++ {
		_, err := conn.Create("/foo"+fmt.Sprint(i), val, flags, acl)
		if err != nil {
			panic(err)
		}
		if i%1000 == 0 {
			fmt.Println(i)
		}
	}
}

func benchPut(conn *zk.Conn) {
	_, err := conn.Create("/foo", []byte("bar"), flags, acl)
	if err != nil {
		fmt.Println(err)
	}

	donec := make(chan struct{})
	start := time.Now()
	for n := 0; n < 100; n++ {
		go func() {
			for i := 0; i < 1000; i++ {
				_, err = conn.Set("/foo", []byte("bar"), -1)
				if err != nil {
					panic(err)
				}
			}
			donec <- struct{}{}
		}()
	}

	for n := 0; n < 100; n++ {
		<-donec
	}
	fmt.Println(time.Since(start))
}
