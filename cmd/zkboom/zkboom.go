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
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/report"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

var (
	acl = zk.WorldACL(zk.PermAll)

	rootCmd = &cobra.Command{
		Use:   "zkctl",
		Short: "A simple command line client for Zookeeper.",
	}

	zkBoomConns          int
	zkBoomEndpoints      []string
	zkBoomRate           int
	zkBoomSample         bool
	zkBoomTimeoutSeconds int
	zkBoomTotal          int

	zkBoomCreateEphemeral bool
	zkBoomCreateSequence  bool

	zkBoomCreateKeySize int
	zkBoomCreateValSize int

	zkBoomPrecise bool
)

func init() {
	cobra.EnablePrefixMatching = true
	rootCmd.PersistentFlags().IntVar(&zkBoomConns, "conns", 1, "Total number of connections")
	rootCmd.PersistentFlags().StringSliceVar(&zkBoomEndpoints, "endpoints", []string{"127.0.0.1:2181"}, "Zookeeper client endpoints")
	rootCmd.PersistentFlags().IntVar(&zkBoomRate, "rate", 0, "Maximum requests per second (0 is no limit)")
	rootCmd.PersistentFlags().BoolVar(&zkBoomSample, "samples", false, "Report time-series sample results")
	rootCmd.PersistentFlags().BoolVar(&zkBoomPrecise, "precise", false, "Print high precision results")
	rootCmd.PersistentFlags().IntVar(&zkBoomTimeoutSeconds, "timeout", 5, "Timeout for ZooKeeper client in seconds")
	rootCmd.PersistentFlags().IntVar(&zkBoomTotal, "total", 10000, "Total number of requests")

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "creates new keys",
		Run:   createCommandFunc,
	}
	createCmd.Flags().IntVar(&zkBoomCreateKeySize, "key-size", 8, "Key size of create request")
	createCmd.Flags().IntVar(&zkBoomCreateValSize, "val-size", 8, "Value size of create request")
	createCmd.Flags().BoolVar(&zkBoomCreateSequence, "sequential", false, "Use sequential keys")
	createCmd.Flags().BoolVar(&zkBoomCreateEphemeral, "ephemeral", false, "Use ephemeral keys")

	setCmd := &cobra.Command{
		Use:   "set <path>",
		Short: "sets a key by path",
		Run:   setCommandFunc,
	}
	setCmd.Flags().IntVar(&zkBoomCreateValSize, "val-size", 8, "Value size of set request")

	rootCmd.AddCommand(
		createCmd,
		setCmd,
		&cobra.Command{
			Use:   "get <path>",
			Short: "gets a key by path",
			Run:   getCommandFunc,
		},
	)
}

func mustZKs() []*zk.Conn {
	conns := make([]*zk.Conn, zkBoomConns)
	timeout := time.Duration(zkBoomTimeoutSeconds) * time.Second
	d := func(n, addr string, to time.Duration) (net.Conn, error) {
		return net.DialTimeout(n, addr, timeout)
	}
	for i := range conns {
		conn, _, err := zk.ConnectWithDialer(zkBoomEndpoints, timeout, d)
		if err != nil {
			panic(err)
		}
		conns[i] = conn
	}
	return conns
}

func main() {
	rootCmd.SetHelpTemplate(`{{.UsageString}}`)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}
}

func createCommandFunc(cmd *cobra.Command, args []string) {
	var flags int32
	if zkBoomCreateEphemeral {
		flags |= zk.FlagEphemeral
	}
	if zkBoomCreateSequence {
		flags |= zk.FlagSequence
	}
	requests := make(chan func(*zk.Conn) error)
	go func() {
		defer close(requests)
		val := make([]byte, zkBoomCreateValSize)
		for i := 0; i < zkBoomTotal; i++ {
			n := i
			requests <- func(c *zk.Conn) error {
				key := fmt.Sprintf("/k-%d", n)
				for j := zkBoomCreateKeySize - len(key); j > 0; j-- {
					key += " "
				}
				_, err := c.Create(key, val, flags, acl)
				return err
			}
		}
	}()
	doReport(requests)
}

func setCommandFunc(cmd *cobra.Command, args []string) {
	requests := make(chan func(*zk.Conn) error)
	go func() {
		defer close(requests)
		val := make([]byte, zkBoomCreateValSize)
		for i := 0; i < zkBoomTotal; i++ {
			requests <- func(c *zk.Conn) error {
				_, err := c.Set(args[0], val, -1)
				return err
			}
		}
	}()
	doReport(requests)
}

func getCommandFunc(cmd *cobra.Command, args []string) {
	requests := make(chan func(*zk.Conn) error)
	go func() {
		defer close(requests)
		for i := 0; i < zkBoomTotal; i++ {
			requests <- func(c *zk.Conn) error {
				_, _, err := c.Get(args[0])
				return err
			}
		}
	}()
	doReport(requests)
}

func doReport(requests <-chan func(*zk.Conn) error) {
	var wg sync.WaitGroup

	r := newReport()

	limit := rate.NewLimiter(rate.Limit(zkBoomRate), 1)
	for _, conn := range mustZKs() {
		wg.Add(1)
		go func(c *zk.Conn) {
			defer wg.Done()
			for op := range requests {
				limit.Wait(context.Background())
				st := time.Now()
				err := op(c)
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
			}
		}(conn)
	}
	rc := r.Run()

	wg.Wait()
	close(r.Results())
	fmt.Println(<-rc)
}

func newReport() report.Report {
	p := "%4.4f"
	if zkBoomPrecise {
		p = "%g"
	}
	if zkBoomSample {
		return report.NewReportSample(p)
	}
	return report.NewReport(p)
}
