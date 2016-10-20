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
	"github.com/golang/glog"
)

type zkLog struct{ zk ZK }

func NewZKLog(zk ZK) ZK {
	return &zkLog{zk}
}

func (zl *zkLog) Create(xid Xid, op *CreateRequest) ZKResponse {
	glog.V(7).Infof("Create(%v,%+v)", xid, *op)
	return zl.zk.Create(xid, op)
}

func (zl *zkLog) Delete(xid Xid, op *DeleteRequest) ZKResponse {
	glog.V(7).Infof("Delete(%v,%+v)", xid, *op)
	return zl.zk.Delete(xid, op)
}

func (zl *zkLog) Exists(xid Xid, op *ExistsRequest) ZKResponse {
	glog.V(7).Infof("Exists(%v,%+v)", xid, *op)
	return zl.zk.Exists(xid, op)
}

func (zl *zkLog) GetData(xid Xid, op *GetDataRequest) ZKResponse {
	glog.V(7).Infof("GetData(%v,%+v)", xid, *op)
	return zl.zk.GetData(xid, op)
}

func (zl *zkLog) SetData(xid Xid, op *SetDataRequest) ZKResponse {
	glog.V(7).Infof("SetData(%v,%+v)", xid, *op)
	return zl.zk.SetData(xid, op)
}

func (zl *zkLog) GetAcl(xid Xid, op *GetAclRequest) ZKResponse {
	glog.V(7).Infof("GetAcl(%v,%+v)", xid, *op)
	return zl.zk.GetAcl(xid, op)
}

func (zl *zkLog) SetAcl(xid Xid, op *SetAclRequest) ZKResponse {
	glog.V(7).Infof("SetAcl(%v,%+v)", xid, *op)
	return zl.zk.SetAcl(xid, op)
}

func (zl *zkLog) GetChildren(xid Xid, op *GetChildrenRequest) ZKResponse {
	glog.V(7).Infof("GetChildren(%v,%+v)", xid, *op)
	return zl.zk.GetChildren(xid, op)
}

func (zl *zkLog) Sync(xid Xid, op *SyncRequest) ZKResponse {
	glog.V(7).Infof("Sync(%v,%+v)", xid, *op)
	return zl.zk.Sync(xid, op)
}

func (zl *zkLog) Ping(xid Xid, op *PingRequest) ZKResponse {
	glog.V(7).Infof("Ping(%v,%+v)", xid, *op)
	return zl.zk.Ping(xid, op)
}

func (zl *zkLog) GetChildren2(xid Xid, op *GetChildren2Request) ZKResponse {
	glog.V(7).Infof("GetChildren2(%v,%+v)", xid, *op)
	return zl.zk.GetChildren2(xid, op)
}

func (zl *zkLog) Multi(xid Xid, op *MultiRequest) ZKResponse {
	glog.V(7).Infof("Multi(%v,%+v)", xid, *op)
	return zl.zk.Multi(xid, op)
}

func (zl *zkLog) Close(xid Xid, op *CloseRequest) ZKResponse {
	glog.V(7).Infof("Close(%v,%+v)", xid, *op)
	return zl.zk.Close(xid, op)
}

func (zl *zkLog) SetAuth(xid Xid, op *SetAuthRequest) ZKResponse {
	glog.V(7).Infof("SetAuth(%v,%+v)", xid, *op)
	return zl.zk.SetAuth(xid, op)
}

func (zl *zkLog) SetWatches(xid Xid, op *SetWatchesRequest) ZKResponse {
	glog.V(7).Infof("SetWatches(%v,%+v)", xid, *op)
	return zl.zk.SetWatches(xid, op)
}
