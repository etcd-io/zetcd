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

package zetcd

import (
	"sync"
	"sync/atomic"
)

// bufPool holds a pool of buffers to be consumed by conn when sending
// messages back to the zk client.
type bufPool struct {
	total int32
	sync.Pool
}

const maxBufs = 128

var bufpool bufPool

func init() {
	bufpool.New = func() interface{} { return make([]byte, 2*1024*1024) }
}

func (bp *bufPool) Get() interface{} {
	atomic.AddInt32(&bp.total, 1)
	return bp.Pool.Get()
}

func (bp *bufPool) Put(v interface{}) {
	if atomic.AddInt32(&bp.total, -1) > maxBufs {
		return
	}
	bp.Pool.Put(v)
}
