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

// This file describes the on-wire format.

type Xid int32
type Op int32
type ZXid int64
type Sid int64
type Ver int32 // version

type ACL struct {
	Perms  int32
	Scheme string
	ID     string
}

type CheckVersionRequest pathVersionRequest

type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    ZXid
	TimeOut         int32
	SessionID       Sid
	Passwd          []byte
}

type ConnectResponse struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionID       Sid
	Passwd          []byte
}

type CreateRequest struct {
	Path  string
	Data  []byte
	Acl   []ACL
	Flags int32
}

type CreateResponse pathResponse

type CloseRequest struct{}
type CloseResponse struct{}

type auth struct {
	Type   int32
	Scheme string
	Auth   []byte
}

type SetAuthRequest auth
type SetAuthResponse struct{}

type SetWatchesRequest struct {
	RelativeZxid ZXid
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type SetWatchesResponse struct{}

type MultiHeader struct {
	Type Op
	Done bool
	Err  ErrCode
}

type MultiRequestOp struct {
	Header MultiHeader
	Op     interface{}
}
type MultiRequest struct {
	Ops        []MultiRequestOp
	DoneHeader MultiHeader
}
type MultiResponseOp struct {
	Header MultiHeader
	String string
	Stat   *Stat
}
type MultiResponse struct {
	Ops        []MultiResponseOp
	DoneHeader MultiHeader
}

type GetChildren2Request pathWatchRequest

type GetChildren2Response struct {
	Children []string
	Stat     Stat
}

type GetDataRequest pathWatchRequest

type GetDataResponse struct {
	Data []byte
	Stat Stat
}

type DeleteRequest pathVersionRequest
type DeleteResponse struct{}

type ExistsRequest pathWatchRequest
type ExistsResponse statResponse

type GetAclRequest pathRequest

type GetAclResponse struct {
	Acl  []ACL
	Stat Stat
}
type SetAclRequest struct {
	Path    string
	Acl     []ACL
	Version Ver
}

type SetAclResponse statResponse

type GetChildrenRequest pathWatchRequest

type GetChildrenResponse struct {
	Children []string
}

type SyncRequest pathRequest
type SyncResponse pathResponse

type PingRequest struct{}
type PingResponse struct{}

type SetDataRequest struct {
	Path    string
	Data    []byte
	Version Ver
}

type SetDataResponse statResponse

type Stat struct {
	// Czxid is the zxid change that caused this znode to be created.
	Czxid ZXid
	// Mzxid is The zxid change that last modified this znode.
	Mzxid ZXid
	// Ctime is milliseconds from epoch when this znode was created.
	Ctime int64
	// Mtime is The time in milliseconds from epoch when this znode was last modified.
	Mtime          int64
	Version        Ver   // The number of changes to the data of this znode.
	Cversion       Ver   // The number of changes to the children of this znode.
	Aversion       Ver   // The number of changes to the ACL of this znode.
	EphemeralOwner Sid   // The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
	DataLength     int32 // The length of the data field of this znode.
	NumChildren    int32 // The number of children of this znode.
	Pzxid          ZXid  // last modified children
}

type WatcherEvent struct {
	Type  EventType
	State State
	Path  string
}

type pathWatchRequest struct {
	Path  string
	Watch bool
}

type pathResponse struct {
	Path string
}

type pathVersionRequest struct {
	Path    string
	Version Ver
}

type statResponse struct {
	Stat Stat
}

type requestHeader struct {
	Xid    Xid
	Opcode Op
}

type ResponseHeader struct {
	Xid  Xid
	Zxid ZXid
	Err  ErrCode
}

type pathRequest struct {
	Path string
}
