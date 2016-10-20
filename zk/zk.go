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

package zk

import (
	"fmt"

	"github.com/coreos/zetcd"
)

// zkZK takes incoming ZK requests and forwards them to a remote ZK server
type zkZK struct{ s *session }

func newZK(s zetcd.Session) (*zkZK, error) {
	ss, ok := s.Backing().(*session)
	if !ok {
		return nil, fmt.Errorf("unexpected session type %t", s)
	}
	return &zkZK{ss}, nil
}

func (zz *zkZK) Create(xid zetcd.Xid, op *zetcd.CreateRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}

func (zz *zkZK) Delete(xid zetcd.Xid, op *zetcd.DeleteRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) Exists(xid zetcd.Xid, op *zetcd.ExistsRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) GetData(xid zetcd.Xid, op *zetcd.GetDataRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) SetData(xid zetcd.Xid, op *zetcd.SetDataRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) GetAcl(xid zetcd.Xid, op *zetcd.GetAclRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) SetAcl(xid zetcd.Xid, op *zetcd.SetAclRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) GetChildren(xid zetcd.Xid, op *zetcd.GetChildrenRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) Sync(xid zetcd.Xid, op *zetcd.SyncRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) Ping(xid zetcd.Xid, op *zetcd.PingRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}

func (zz *zkZK) GetChildren2(xid zetcd.Xid, op *zetcd.GetChildren2Request) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) Multi(xid zetcd.Xid, op *zetcd.MultiRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) Close(xid zetcd.Xid, op *zetcd.CloseRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) SetAuth(xid zetcd.Xid, op *zetcd.SetAuthRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
func (zz *zkZK) SetWatches(xid zetcd.Xid, op *zetcd.SetWatchesRequest) zetcd.ZKResponse {
	return <-zz.s.future(xid, op)
}
