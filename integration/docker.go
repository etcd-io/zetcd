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

// +build docker zkdocker

package integration

import (
	"archive/tar"
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/fsouza/go-dockerclient"
)

type Container struct {
	dc      *docker.Client
	name    string
	id      string
	attachc chan error
}

func NewContainer(containerName, dockerFile string, ports []string) (c *Container, err error) {
	dc, err := docker.NewClient("unix://var/run/docker.sock")
	if err != nil {
		return nil, err
	}
	c = &Container{
		dc:      dc,
		name:    containerName,
		attachc: make(chan error, 1),
	}
	defer func() {
		if err != nil && c != nil {
			c.Close()
		}
	}()

	// Kill off anything still hanging around
	c.id, err = c.idByName()
	if err == nil && c.id != "" {
		c.stop()
	}
	// Grab fresh container
	if c.id, err = c.build(dockerFile, ports); err != nil {
		close(c.attachc)
		return nil, err
	}
	if err = c.dc.StartContainer(c.id, nil); err != nil {
		close(c.attachc)
		return nil, err
	}
	// attach to get output for debugging
	go func() {
		defer close(c.attachc)
		aot := docker.AttachToContainerOptions{
			Container:    c.id,
			OutputStream: os.Stdout,
			ErrorStream:  os.Stderr,
			Logs:         true,
			Stream:       true,
			Stdout:       true,
			Stderr:       true,
		}
		c.attachc <- c.dc.AttachToContainer(aot)
	}()
	return c, nil
}

func (c *Container) stop() error {
	err := c.dc.StopContainer(c.id, 1000)
	c.dc.WaitContainer(c.id)
	c.dc.RemoveContainer(docker.RemoveContainerOptions{ID: c.id})
	return err
}

func (c *Container) Close() error {
	if err := c.stop(); err != nil {
		return err
	}
	return <-c.attachc
}

func (c *Container) build(dockerFile string, ports []string) (string, error) {
	// build
	// this is ridiculous, but client expects a remote if passed a Dockerfile;
	// instead, gavage it with a tar over an input stream
	inputbuf, outputbuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	tr := tar.NewWriter(inputbuf)
	dat, derr := ioutil.ReadFile(dockerFile)
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
		Name:         c.name,
		InputStream:  inputbuf,
		OutputStream: outputbuf,
	}
	if err := c.dc.BuildImage(opts); err != nil {
		return "", err
	}
	output := string(outputbuf.Bytes())
	lines := strings.Split(output, "\n")
	img := strings.Split(lines[len(lines)-2], " ")[2]

	exposedPorts := make(map[docker.Port]struct{})
	portBindings := make(map[docker.Port][]docker.PortBinding)
	for _, port := range ports {
		p := docker.Port(port)
		exposedPorts[p] = struct{}{}
		portBindings[p] = []docker.PortBinding{
			{
				HostIP:   "127.0.0.1",
				HostPort: port,
			},
		}
	}

	cco := docker.CreateContainerOptions{
		Name: c.name,
		Config: &docker.Config{
			Image:        img,
			ExposedPorts: exposedPorts,
			AttachStderr: true,
			AttachStdout: true,
		},
		HostConfig: &docker.HostConfig{PortBindings: portBindings},
	}
	con, cerr := c.dc.CreateContainer(cco)
	if cerr != nil {
		return "", cerr
	}
	return con.ID, nil
}

func (c *Container) idByName() (string, error) {
	apic, err := c.dc.ListContainers(docker.ListContainersOptions{All: true})
	if err != nil {
		return "", err
	}
	for i := range apic {
		for _, n := range apic[i].Names {
			if n == "/"+c.name {
				return apic[i].ID, nil
			}
		}
	}
	return "", nil
}
