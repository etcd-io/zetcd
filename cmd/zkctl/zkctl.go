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
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	acl = zk.WorldACL(zk.PermAll)
)

func main() {
	s := flag.String("zkaddr", "127.0.0.1", "address of zookeeper server")
	flag.Parse()
	c, _, err := zk.Connect([]string{*s}, time.Second)
	if err != nil {
		panic(err)
	}

	switch flag.Args()[0] {
	case "watch":
		dir := "/"
		if len(flag.Args()) > 1 {
			dir = flag.Args()[1]
		}
		watch(c, dir)
	case "ls":
		dir := "/"
		if len(flag.Args()) > 1 {
			dir = flag.Args()[1]
		}
		err = ls(c, dir)
	case "rm":
		err = rm(c, flag.Args()[1])
	case "set":
		err = set(c, flag.Args()[1], flag.Args()[2])
	case "get":
		err = get(c, flag.Args()[1])
	case "put":
		err = put(c, flag.Args()[1], flag.Args()[2], 0)
	case "eput":
		err = put(c, flag.Args()[1], flag.Args()[2], zk.FlagEphemeral)
	case "sput":
		err = put(c, flag.Args()[1], flag.Args()[2], zk.FlagSequence)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func watch(c *zk.Conn, dir string) {
	fmt.Println("watch dir", dir)
	children, stat, ch, err := c.ChildrenW(dir)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
	e := <-ch
	fmt.Printf("%+v\n", e)
}

func ls(c *zk.Conn, dir string) error {
	fmt.Println("ls dir", dir)
	children, stat, err := c.Children(dir)
	if err != nil {
		return err
	}
	sort.Sort(sort.StringSlice(children))
	fmt.Println("Children:")
	for _, c := range children {
		fmt.Printf("%s (%s)\n", path.Clean(dir+"/"+c), c)
	}
	fmt.Printf("Stat: %+v\n", stat)
	return nil
}

func put(c *zk.Conn, path, data string, fl int32) error {
	// TODO: descriptive acls
	_, err := c.Create(path, []byte(data), fl, acl)
	return err
}

func set(c *zk.Conn, path, data string) error {
	_, err := c.Set(path, []byte(data), -1)
	return err
}

func rm(c *zk.Conn, path string) error {
	return c.Delete(path, -1)
}

func get(c *zk.Conn, path string) error {
	dat, st, err := c.Get(path)
	if err == nil {
		fmt.Println(dat)
		fmt.Printf("Stat:\n%+v\n", st)
	}
	return err
}
