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
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/fsouza/go-dockerclient"
)

type Container struct {
	dc      *docker.Client
	id      string
	attachc chan error
	ContainerConfig
}

type ContainerConfig struct {
	name   string
	files  []string
	ports  []string
	env    []string
	writer io.Writer
}

func NewContainer(containerName, dockerFile string, ports []string) (*Container, error) {
	cfg := ContainerConfig{
		name:  containerName,
		files: []string{dockerFile},
		ports: ports,
	}
	return newContainerFiles(cfg)
}

func newContainerFiles(cfg ContainerConfig) (c *Container, err error) {
	dc, err := docker.NewClient("unix://var/run/docker.sock")
	if err != nil {
		return nil, err
	}

	if cfg.writer == nil {
		cfg.writer = os.Stdout
	}
	c = &Container{
		dc:              dc,
		attachc:         make(chan error, 1),
		ContainerConfig: cfg,
	}
	defer func() {
		if err != nil && c != nil {
			c.Close()
		}
	}()

	hc := &docker.HostConfig{
		NetworkMode:     "host",
		Privileged:      true,
		PublishAllPorts: true,
	}

	// Kill off anything still hanging around
	c.id, err = c.idByName()
	if err == nil && c.id != "" {
		c.stop()
	}
	// Grab fresh container
	if c.id, err = c.build(cfg.files, cfg.ports, cfg.env); err != nil {
		close(c.attachc)
		return nil, err
	}
	if err = c.dc.StartContainer(c.id, hc); err != nil {
		close(c.attachc)
		return nil, err
	}
	// attach to get output for debugging
	go func() {
		defer close(c.attachc)
		aot := docker.AttachToContainerOptions{
			Container:    c.id,
			OutputStream: c.writer,
			ErrorStream:  c.writer,
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
	err := c.dc.StopContainer(c.id, 1)
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

func buildTar(files ...string) (io.Reader, error) {
	inputbuf := bytes.NewBuffer(nil)
	tr := tar.NewWriter(inputbuf)
	defer tr.Close()
	now := time.Now()
	for _, f := range files {
		dat, derr := ioutil.ReadFile(f)
		if derr != nil {
			return nil, derr
		}
		tr.WriteHeader(&tar.Header{
			Name:       f,
			Size:       int64(len(dat)),
			ModTime:    now,
			AccessTime: now,
			ChangeTime: now,
		})
		tr.Write(dat)
	}
	return inputbuf, nil
}

// build takes a list of files and ports, bundles the files up for docker,
// and exposes the given ports.
func (c *Container) build(files []string, ports []string, env []string) (string, error) {
	inputbuf, ierr := buildTar(files...)
	if ierr != nil {
		return "", ierr
	}

	outputbuf := bytes.NewBuffer(nil)
	opts := docker.BuildImageOptions{
		Name:         c.name,
		Dockerfile:   files[0],
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
				HostIP:   "0.0.0.0",
				HostPort: port,
			},
		}
	}

	cco := docker.CreateContainerOptions{
		Name: c.name,
		Config: &docker.Config{
			Image:        img,
			ExposedPorts: exposedPorts,
			Env:          env,
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
