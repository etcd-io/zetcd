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
	"archive/tar"
	"bytes"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/fsouza/go-dockerclient"
)

var containerName = "zetcd-zk-test"

type zkCluster struct {
	zkClientAddr string

	// don't use these in tests since they may change with build tags
	cancel func()
	donec  <-chan struct{}
}

func newZKCluster(t *testing.T) *zkCluster {
	c, err := docker.NewClient("unix://var/run/docker.sock")
	if err != nil {
		t.Fatal(err)
	}
	// Kill off anything still hanging around
	id, ierr := containerIDFromName(c, containerName)
	if ierr == nil && id != "" {
		c.StopContainer(id, 1000)
		c.WaitContainer(id)
		c.RemoveContainer(docker.RemoveContainerOptions{ID: id})
	}
	// Grab fresh container
	if id, err = containerBuild(c); err != nil {
		t.Fatal(err)
	}
	if err := c.StartContainer(id, nil); err != nil {
		t.Fatal(err)
	}
	// attach to get zk server output for debugging
	attachc := make(chan struct{})
	go func() {
		defer close(attachc)
		aot := docker.AttachToContainerOptions{
			Container:    id,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Logs:         true,
			Stream:       true,
			Stdout:       true,
			Stderr:       true,
		}
		if err := c.AttachToContainer(aot); err != nil {
			t.Fatal(err)
		}
	}()
	// poll until zk server is available
	for {
		time.Sleep(200 * time.Millisecond)
		conn, cerr := net.Dial("tcp", "127.0.0.1:2181")
		if cerr != nil {
			t.Fatal(cerr)
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

	donec := make(chan struct{})
	return &zkCluster{
		zkClientAddr: "127.0.0.1:2181",

		cancel: func() {
			defer close(donec)
			if err := c.StopContainer(id, 1000); err != nil {
				t.Fatal(err)
			}
			c.WaitContainer(id)
			c.RemoveContainer(docker.RemoveContainerOptions{ID: id})
			<-attachc
		},
		donec: donec,
	}
}

func containerBuild(c *docker.Client) (string, error) {
	// build
	// this is ridiculous, but client expects a remote if passed a Dockerfile;
	// instead, gavage it with a tar over an input stream
	inputbuf, outputbuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	tr := tar.NewWriter(inputbuf)
	dat, derr := ioutil.ReadFile("../docker/zk/Dockerfile")
	if derr != nil {
		return "", derr
	}
	now := time.Now()
	tr.WriteHeader(&tar.Header{
		Name:       "Dockerfile",
		Size:       int64(len(dat)),
		ModTime:    now,
		AccessTime: now,
		ChangeTime: now,
	})
	tr.Write(dat)
	tr.Close()

	opts := docker.BuildImageOptions{
		Name:         containerName,
		InputStream:  inputbuf,
		OutputStream: outputbuf,
	}
	if err := c.BuildImage(opts); err != nil {
		return "", err
	}
	output := string(outputbuf.Bytes())
	lines := strings.Split(output, "\n")
	img := strings.Split(lines[len(lines)-2], " ")[2]

	// create
	cco := docker.CreateContainerOptions{
		Name: containerName,
		Config: &docker.Config{
			Image:        img,
			ExposedPorts: map[docker.Port]struct{}{"2181/tcp": struct{}{}},
			AttachStderr: true,
			AttachStdout: true,
		},
		HostConfig: &docker.HostConfig{
			PortBindings: map[docker.Port][]docker.PortBinding{
				"2181/tcp": []docker.PortBinding{
					{HostIP: "127.0.0.1", HostPort: "2181/tcp"},
				},
			},
		},
	}
	con, cerr := c.CreateContainer(cco)
	if cerr != nil {
		return "", cerr
	}
	return con.ID, nil
}

func (zkclus *zkCluster) Close(t *testing.T) {
	zkclus.cancel()
	<-zkclus.donec
}

func containerIDFromName(c *docker.Client, name string) (string, error) {
	apic, err := c.ListContainers(docker.ListContainersOptions{All: true})
	if err != nil {
		return "", err
	}
	for i := range apic {
		for _, n := range apic[i].Names {
			if n == "/"+name {
				return apic[i].ID, nil
			}
		}
	}
	return "", nil
}
