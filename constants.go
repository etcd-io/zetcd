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
	"errors"
)

const (
	protocolVersion = 0

	DefaultPort = 2181
)

const (
	opNotify       Op = 0
	opCreate          = 1
	opDelete          = 2
	opExists          = 3
	opGetData         = 4
	opSetData         = 5
	opGetAcl          = 6
	opSetAcl          = 7
	opGetChildren     = 8
	opSync            = 9
	opPing            = 11
	opGetChildren2    = 12
	opCheck           = 13
	opMulti           = 14

	opClose      = -11
	opSetAuth    = 100
	opSetWatches = 101

	// Not in protocol, used internally
	opWatcherEvent = -2
	opInvalid      = -100000
)

type EventType int32

const (
	EventNodeCreated EventType = iota + 1
	EventNodeDeleted
	EventNodeDataChanged
	EventNodeChildrenChanged

	EventSession     = EventType(-1)
	EventNotWatching = EventType(-2)
)

const (
	FlagEphemeral = 1
	FlagSequence  = 2
)

const (
	StateUnknown           = State(-1)
	StateDisconnected      = State(0)
	StateConnecting        = State(1)
	StateSyncConnected     = State(3)
	StateAuthFailed        = State(4)
	StateConnectedReadOnly = State(5)
	StateSaslAuthenticated = State(6)
	StateExpired           = State(-112)
	// StateAuthFailed        = State(-113)

	StateConnected  = State(100)
	StateHasSession = State(101)
)

var (
	stateNames = map[State]string{
		StateUnknown:           "StateUnknown",
		StateDisconnected:      "StateDisconnected",
		StateConnectedReadOnly: "StateConnectedReadOnly",
		StateSaslAuthenticated: "StateSaslAuthenticated",
		StateExpired:           "StateExpired",
		StateAuthFailed:        "StateAuthFailed",
		StateConnecting:        "StateConnecting",
		StateConnected:         "StateConnected",
		StateHasSession:        "StateHasSession",
	}
)

type State int32

var (
	ErrConnectionClosed        = errors.New("zk: connection closed")
	ErrUnknown                 = errors.New("zk: unknown error")
	ErrAPIError                = errors.New("zk: api error")
	ErrNoNode                  = errors.New("zk: node does not exist")
	ErrNoAuth                  = errors.New("zk: not authenticated")
	ErrBadVersion              = errors.New("zk: version conflict")
	ErrNoChildrenForEphemerals = errors.New("zk: ephemeral nodes may not have children")
	ErrNodeExists              = errors.New("zk: node already exists")
	ErrNotEmpty                = errors.New("zk: node has children")
	ErrSessionExpired          = errors.New("zk: session has been expired by the server")
	ErrInvalidACL              = errors.New("zk: invalid ACL specified")
	ErrAuthFailed              = errors.New("zk: client authentication failed")
	ErrClosing                 = errors.New("zk: zookeeper is closing")
	ErrNothing                 = errors.New("zk: no server responsees to process")
	ErrSessionMoved            = errors.New("zk: session moved to another server, so operation is ignored")

	ErrBadArguments = errors.New("zk: bad arguments")

	// ErrInvalidCallback         = errors.New("zk: invalid callback specified")
	errCodeToError = map[ErrCode]error{
		0:                          nil,
		errAPIError:                ErrAPIError,
		errNoNode:                  ErrNoNode,
		errNoAuth:                  ErrNoAuth,
		errBadVersion:              ErrBadVersion,
		errNoChildrenForEphemerals: ErrNoChildrenForEphemerals,
		errNodeExists:              ErrNodeExists,
		errNotEmpty:                ErrNotEmpty,
		errSessionExpired:          ErrSessionExpired,
		// errInvalidCallback:         ErrInvalidCallback,
		errInvalidAcl:   ErrInvalidACL,
		errAuthFailed:   ErrAuthFailed,
		errClosing:      ErrClosing,
		errNothing:      ErrNothing,
		errSessionMoved: ErrSessionMoved,

		errBadArguments: ErrBadArguments,
	}

	errorToErrCode = map[error]ErrCode{
		ErrBadArguments: errBadArguments,
		ErrNoNode:       errNoNode,
		ErrNodeExists:   errNodeExists,
		ErrInvalidACL:   errInvalidAcl,
		ErrBadVersion:   errBadVersion,
		ErrAPIError:     errAPIError,
		ErrNotEmpty:     errNotEmpty,
	}
)

func (e ErrCode) toError() error {
	if err, ok := errCodeToError[e]; ok {
		return err
	}
	return ErrUnknown
}

type ErrCode int32

const (
	errOk ErrCode = 0
	// System and server-side errors
	errSystemError          = ErrCode(-1)
	errRuntimeInconsistency = ErrCode(-2)
	errDataInconsistency    = ErrCode(-3)
	errConnectionLoss       = ErrCode(-4)
	errMarshallingError     = ErrCode(-5)
	errUnimplemented        = ErrCode(-6)
	errOperationTimeout     = ErrCode(-7)
	errBadArguments         = ErrCode(-8)
	errInvalidState         = ErrCode(-9)
	// API errors
	errAPIError                = ErrCode(-100)
	errNoNode                  = ErrCode(-101) // *
	errNoAuth                  = ErrCode(-102)
	errBadVersion              = ErrCode(-103) // *
	errNoChildrenForEphemerals = ErrCode(-108)
	errNodeExists              = ErrCode(-110) // *
	errNotEmpty                = ErrCode(-111)
	errSessionExpired          = ErrCode(-112)
	errInvalidCallback         = ErrCode(-113)
	errInvalidAcl              = ErrCode(-114)
	errAuthFailed              = ErrCode(-115)
	errClosing                 = ErrCode(-116)
	errNothing                 = ErrCode(-117)
	errSessionMoved            = ErrCode(-118)
)

// four letter word commands / responses

const (
	flwRUOK = "ruok"
	flwIMOK = "imok"
)
