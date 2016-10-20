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

import "fmt"

// ZK is a synchronous interface
type ZK interface {
	Create(xid Xid, op *CreateRequest) ZKResponse
	Delete(xid Xid, op *DeleteRequest) ZKResponse
	Exists(xid Xid, op *ExistsRequest) ZKResponse
	GetData(xid Xid, op *GetDataRequest) ZKResponse
	SetData(xid Xid, op *SetDataRequest) ZKResponse
	GetAcl(xid Xid, op *GetAclRequest) ZKResponse
	SetAcl(xid Xid, op *SetAclRequest) ZKResponse
	GetChildren(xid Xid, op *GetChildrenRequest) ZKResponse
	Sync(xid Xid, op *SyncRequest) ZKResponse
	Ping(xid Xid, op *PingRequest) ZKResponse
	GetChildren2(xid Xid, op *GetChildren2Request) ZKResponse
	// opCheck		= 13
	Multi(xid Xid, op *MultiRequest) ZKResponse
	Close(xid Xid, op *CloseRequest) ZKResponse
	SetAuth(xid Xid, op *SetAuthRequest) ZKResponse
	SetWatches(xid Xid, op *SetWatchesRequest) ZKResponse
}

func DispatchZK(zk ZK, xid Xid, op interface{}) ZKResponse {
	switch op := op.(type) {
	case *CreateRequest:
		return zk.Create(xid, op)
	case *DeleteRequest:
		return zk.Delete(xid, op)
	case *GetChildrenRequest:
		return zk.GetChildren(xid, op)
	case *GetChildren2Request:
		return zk.GetChildren2(xid, op)
	case *PingRequest:
		return zk.Ping(xid, op)
	case *GetDataRequest:
		return zk.GetData(xid, op)
	case *SetDataRequest:
		return zk.SetData(xid, op)
	case *ExistsRequest:
		return zk.Exists(xid, op)
	case *SyncRequest:
		return zk.Sync(xid, op)
	case *CloseRequest:
		return zk.Close(xid, op)
	case *SetWatchesRequest:
		return zk.SetWatches(xid, op)
	default:
		fmt.Printf("unexpected type %d %T\n", xid, op)
	}
	return mkZKErr(xid, 0, errAPIError)
}
