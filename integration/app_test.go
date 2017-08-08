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

// +build docker,xchk

package integration

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"
)

func TestKafka(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	envs := [][]string{
		{"KAFKA_CONFIG=/kafka/config/kafka.server.properties"},
		{"KAFKA_CONFIG=/kafka/config/kafka.chroot.properties"},
	}

	cfg := ContainerConfig{
		name: "zetcd-test-kafka",
		files: []string{
			"kafka/Dockerfile",
			"kafka/run.sh",
			"kafka/kafka.server.properties",
			"kafka/kafka.chroot.properties",
		},
		ports: []string{"9092/tcp"},
	}
	for i, env := range envs {
		writer, errc := expect("started (kafka.server.KafkaServer")
		cfg.env = env
		cfg.writer = writer
		c, err := newContainerFiles(cfg)
		if err != nil {
			t.Errorf("#%d: failed with env %q (%v)", i, env, err)
			continue
		}
		select {
		case err = <-errc:
			if err != nil {
				t.Errorf("#%d: failed (%v)", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("#%d: took too long to start", i)
		}
		writer.Close()
		<-errc
		c.Close()
	}
}

func TestDrill(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	writer, errc := expect("Initiating Jersey application")
	cfg := ContainerConfig{
		name:   "zetcd-test-drill",
		files:  []string{"drill/Dockerfile", "drill/drill-override.conf"},
		ports:  []string{"8047/tcp"},
		env:    []string{"DRILL_HOST_NAME=172.17.0.4"},
		writer: writer,
	}
	c, err := newContainerFiles(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-errc:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Error("drill took too long")
	}
	writer.Close()
	<-errc
	c.Close()
}

// TestZKCli checks kafka chroot directories can be recursively deleted.
func TestZKCli(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	// have kafka create a chrooted dir
	writer, errc := expect("started (kafka.server.KafkaServer")
	cfg := ContainerConfig{
		name:   "zetcd-test-kafka",
		files:  []string{"kafka/Dockerfile", "kafka/run.sh", "kafka/kafka.chroot.properties"},
		ports:  []string{"9092/tcp"},
		env:    []string{"KAFKA_CONFIG=/kafka/config/kafka.chroot.properties"},
		writer: writer,
	}
	kafka, err := newContainerFiles(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("kafka took too long")
	}
	writer.Close()
	<-errc
	kafka.Close()

	// delete chrooted directory with zkcli rmr
	writer, errc = expect("OK")
	cliCfg := ContainerConfig{
		name:   "zetcd-test-zkcli",
		files:  []string{"zk/Dockerfile.cli", "zk/runCLI"},
		env:    []string{"ZKCLI_INPUT=-server 172.17.0.1:30001 rmr /kafka-chroo"},
		writer: writer,
	}
	zkCli, err := newContainerFiles(cliCfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("zkcli took too long")
	}
	writer.Close()
	<-errc
	zkCli.Close()
}

func TestMesos(t *testing.T) {
	zkclus := newZKCluster(t)
	defer zkclus.Close(t)

	writer, errc := expect("Recovered 0 agents from the registry")
	cfg := ContainerConfig{
		name:  "zetcd-test-mesos",
		files: []string{"mesos/Dockerfile.master"},
		env: []string{
			"MESOS_PORT=5050",
			"MESOS_ZK=zk://172.17.0.1:30001/mesos",
			"MESOS_QUORUM=1",
			"MESOS_REGISTRY=in_memory",
			"MESOS_LOG_DIR=/var/log/mesos",
			"MESOS_WORK_DIR=/var/tmp/mesos",
		},
		writer: writer,
	}
	c, err := newContainerFiles(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-errc:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Error("mesos took too long")
	}
	writer.Close()
	<-errc
	c.Close()
}

func expect(s string) (io.WriteCloser, <-chan error) {
	r, w := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		rr := bufio.NewReader(r)
		for {
			l, err := rr.ReadString('\n')
			if err != nil {
				errc <- err
			}
			if strings.Contains(l, s) {
				return
			}
		}
	}()
	return w, errc
}
