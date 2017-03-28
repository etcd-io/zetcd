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
	"os"
	"path"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/cobra"
)

var (
	acl = zk.WorldACL(zk.PermAll)

	rootCmd = &cobra.Command{
		Use:   "zkctl",
		Short: "A simple command line client for Zookeeper.",
	}

	zkCtlEndpoints []string
	zkCtlEphemeral bool
	zkCtlSequence  bool
)

func init() {
	cobra.EnablePrefixMatching = true
	rootCmd.PersistentFlags().StringSliceVar(&zkCtlEndpoints, "endpoints", []string{"127.0.0.1:2181"}, "Zookeeper client endpoints")

	createCmd := &cobra.Command{
		Use:   "create <path> <value>",
		Short: "creates a key with a given value",
		Run:   createCommandFunc,
	}
	createCmd.Flags().BoolVar(&zkCtlSequence, "sequence", false, "name key using the sequence policy")
	createCmd.Flags().BoolVar(&zkCtlEphemeral, "ephemeral", false, "set key as ephemeral")

	rootCmd.AddCommand(
		createCmd,
		&cobra.Command{
			Use:   "delete <path>",
			Short: "deletes a znode",
			Run:   deleteCommandFunc,
		},
		&cobra.Command{
			Use:   "get <path>",
			Short: "gets the value for a given path",
			Run:   getCommandFunc,
		},
		&cobra.Command{
			Use:   "ls [path]",
			Short: "lists a directory (default /)",
			Run:   lsCommandFunc,
		},
		&cobra.Command{
			Use:   "set <path> <value>",
			Short: "sets the value for an existing znode",
			Run:   setCommandFunc,
		},
		&cobra.Command{
			Use:   "watch [path]",
			Short: "watches a znode (default /)",
			Run:   watchCommandFunc,
		},
	)
}

func main() {
	rootCmd.SetUsageFunc(usageFunc)
	rootCmd.SetHelpTemplate(`{{.UsageString}}`)
	if err := rootCmd.Execute(); err != nil {
		exitOn(err)
	}
}

func lsCommandFunc(cmd *cobra.Command, args []string) {
	dir := "/"
	if len(args) > 1 {
		dir = args[0]
	}
	fmt.Println("ls dir", dir)
	children, stat, err := mustZKClient().Children(dir)
	if err != nil {
		exitOn(err)
	}
	sort.Sort(sort.StringSlice(children))
	fmt.Println("Children:")
	for _, c := range children {
		fmt.Printf("%s (%s)\n", path.Clean(dir+"/"+c), c)
	}
	fmt.Printf("Stat: %+v\n", stat)
}

func watchCommandFunc(cmd *cobra.Command, args []string) {
	dir := "/"
	if len(args) > 1 {
		dir = args[0]
	}
	fmt.Println("watch dir", dir)
	children, stat, ch, err := mustZKClient().ChildrenW(dir)
	if err != nil {
		exitOn(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
	e := <-ch
	fmt.Printf("%+v\n", e)
}

func createCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		exitOn(fmt.Errorf("create expected path and data arguments, got %q", args))
	}
	path, data := args[0], args[1]
	var flags int32
	if zkCtlEphemeral {
		flags |= zk.FlagEphemeral
	}
	if zkCtlSequence {
		flags |= zk.FlagSequence
	}
	if _, err := mustZKClient().Create(path, []byte(data), flags, acl); err != nil {
		exitOn(err)
	}
}

func setCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		exitOn(fmt.Errorf("set expected path and data arguments, got %q", args))
	}
	path, data := args[0], args[1]
	if _, err := mustZKClient().Set(path, []byte(data), -1); err != nil {
		exitOn(err)
	}
}

func deleteCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		exitOn(fmt.Errorf("delete expected path argument, got %q", args))
	}
	if err := mustZKClient().Delete(args[0], -1); err != nil {
		exitOn(err)
	}
}

func getCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		exitOn(fmt.Errorf("get expected path argument, got %q", args))
	}
	dat, st, err := mustZKClient().Get(args[0])
	if err != nil {
		exitOn(err)
	}
	fmt.Println(dat)
	fmt.Printf("Stat:\n%+v\n", st)
}

func mustZKClient() *zk.Conn {
	c, _, err := zk.Connect(zkCtlEndpoints, time.Second)
	if err != nil {
		exitOn(err)
	}
	return c
}

func exitOn(err error) {
	fmt.Fprintf(os.Stderr, "%+v\n", err)
	os.Exit(1)
}
