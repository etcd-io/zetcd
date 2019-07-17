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

// use a map

func op2req(op Op) interface{} {
	switch op {
	case opGetChildren2:
		return &GetChildren2Request{}
	case opPing:
		return &PingRequest{}
	case opCreate:
		return &CreateRequest{}
	case opCheck:
		return &CheckVersionRequest{}
	case opSetWatches:
		return &SetWatchesRequest{}
	case opSetData:
		return &SetDataRequest{}
	case opGetData:
		return &GetDataRequest{}
	case opDelete:
		return &DeleteRequest{}
	case opExists:
		return &ExistsRequest{}
	case opGetAcl:
		return &GetAclRequest{}
	case opSetAcl:
		return &SetAclRequest{}
	case opGetChildren:
		return &GetChildrenRequest{}
	case opSync:
		return &SyncRequest{}
	case opMulti:
		return &MultiRequest{}
	case opClose:
		return &CloseRequest{}
	case opSetAuth:
		return &SetAuthRequest{}

	default:
		fmt.Println("unknown opcode ", op)
	}
	return nil
}

func op2resp(op Op) interface{} {
	switch op {
	case opGetChildren2:
		return &GetChildren2Response{}
	case opPing:
		return &PingResponse{}
	case opCreate:
		return &CreateResponse{}
	case opSetWatches:
		return &SetWatchesResponse{}
	case opSetData:
		return &SetDataResponse{}
	case opGetData:
		return &GetDataResponse{}
	case opDelete:
		return &DeleteResponse{}
	case opExists:
		return &ExistsResponse{}
	case opGetAcl:
		return &GetAclResponse{}
	case opSetAcl:
		return &SetAclResponse{}
	case opGetChildren:
		return &GetChildrenResponse{}
	case opSync:
		return &SyncResponse{}
	case opMulti:
		return &MultiResponse{}
	case opClose:
		return &CloseResponse{}
	case opSetAuth:
		return &SetAuthResponse{}
	default:
		fmt.Println("unknown opcode ", op)
	}
	return nil
}

func req2op(req interface{}) Op {
	switch req.(type) {
	case *GetChildren2Request:
		return opGetChildren2
	case *PingRequest:
		return opPing
	case *CreateRequest:
		return opCreate
	case *SetWatchesRequest:
		return opSetWatches
	case *SetDataRequest:
		return opSetData
	case *GetDataRequest:
		return opGetData
	case *DeleteRequest:
		return opDelete
	case *ExistsRequest:
		return opExists
	case *GetAclRequest:
		return opGetAcl
	case *SetAclRequest:
		return opSetAcl
	case *GetChildrenRequest:
		return opGetChildren
	case *SyncRequest:
		return opSync
	case *MultiRequest:
		return opMulti
	case *CloseRequest:
		return opClose
	case *SetAuthRequest:
		return opSetAuth
	default:
		fmt.Println("unknown request", req)
	}
	return opInvalid
}
