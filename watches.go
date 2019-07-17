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
	"sync"

	etcd "github.com/etcd-io/etcd/clientv3"
	"github.com/golang/glog"
	"golang.org/x/net/context"
)

type Watches interface {
	// Watch creates a watch request on a given path and evtype.
	Watch(rev ZXid, xid Xid, path string, evtype EventType, cb WatchHandler)

	// Wait blocks until all watches that rely on the given rev are dispatched.
	Wait(rev ZXid, path string, evtype EventType)
}

type WatchHandler func(ZXid, EventType)

type watches struct {
	mu sync.Mutex
	c  *etcd.Client

	path2watch [5]map[string]*watch

	ctx    context.Context
	cancel context.CancelFunc
}

type watch struct {
	c *etcd.Client

	xid    Xid
	evtype EventType
	path   string

	wch    etcd.WatchChan
	ctx    context.Context
	cancel context.CancelFunc

	// startRev is the etcd store revision when this watch began
	startRev ZXid
	donec    chan struct{}
}

func ev2evtype(ev *etcd.Event) EventType {
	switch {
	case ev.IsCreate():
		return EventNodeCreated
	case ev.Type == etcd.EventTypeDelete:
		return EventNodeDeleted
	case ev.IsModify():
		return EventNodeDataChanged
	default:
		return EventNotWatching
	}
}

func newWatches(c *etcd.Client) *watches {
	ctx, cancel := context.WithCancel(context.TODO())
	ws := &watches{
		c:      c,
		ctx:    ctx,
		cancel: cancel,
	}
	for i := 0; i < len(ws.path2watch); i++ {
		ws.path2watch[i] = make(map[string]*watch)
	}
	return ws
}

func (ws *watches) Watch(rev ZXid, xid Xid, path string, evtype EventType, cb WatchHandler) {
	ws.mu.Lock()
	curw := ws.path2watch[evtype][path]
	ws.mu.Unlock()
	if curw != nil {
		return
	}

	ctx, cancel := context.WithCancel(ws.ctx)
	var wch etcd.WatchChan
	switch evtype {
	case EventNodeDataChanged:
		fallthrough
	case EventNodeCreated:
		fallthrough
	// use rev+1 watch begins AFTER the requested zxid
	case EventNodeDeleted:
		wch = ws.c.Watch(ctx, mkPathKey(path), etcd.WithRev(int64(rev+1)))
	case EventNodeChildrenChanged:
		wch = ws.c.Watch(
			ctx,
			getListPfx(path),
			etcd.WithPrefix(),
			etcd.WithRev(int64(rev+1)))
	default:
		panic("unsupported watch op")
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	curw = ws.path2watch[evtype][path]
	if curw != nil {
		glog.V(7).Infof("ELIDING WATCH on xid=%d evtype=%d, already have %s evtype=%d", xid, evtype, path, curw.evtype)
		cancel()
		return
	}
	w := &watch{ws.c, xid, evtype, path, wch, ctx, cancel, rev, make(chan struct{})}
	ws.path2watch[evtype][path] = w
	go ws.runWatch(w, cb)
}

func (ws *watches) runWatch(w *watch, cb WatchHandler) {
	defer func() {
		close(w.donec)
		<-w.wch
	}()
	for {
		select {
		case resp, ok := <-w.wch:
			if !ok {
				return
			}
			if len(resp.Events) == 0 {
				continue
			}
			evtype := ev2evtype(resp.Events[0])
			ws.mu.Lock()
			delete(ws.path2watch[w.evtype], w.path)
			ws.mu.Unlock()
			cb(ZXid(resp.Header.Revision), evtype)
			w.cancel()
		case <-w.ctx.Done():
		}
	}
}

func (ws *watches) close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.cancel()
	for i := range ws.path2watch {
		for _, w := range ws.path2watch[i] {
			for range w.wch {
			}
		}
	}
}

// Wait until watcher depending on the given rev completes.
// NOTE: path is internal zkpath representation
// TODO: watch waiting may need to be proxy-wide to be correct
// TODO: better algorithm
func (ws *watches) Wait(rev ZXid, path string, evtype EventType) {
	ch := []<-chan struct{}{}
	ws.mu.Lock()
	for k, w := range ws.path2watch[evtype] {
		if k != path {
			continue
		}
		if w.startRev <= rev && w.evtype == evtype {
			ch = append(ch, w.donec)
		}
	}
	ws.mu.Unlock()
	for _, c := range ch {
		<-c
	}
}
