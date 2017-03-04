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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"path"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	v3sync "github.com/coreos/etcd/clientv3/concurrency"
	"github.com/golang/glog"
)

type zkEtcd struct {
	c *etcd.Client
	s Session
}

// PerfectZXid is enabled to insert err writes to match zookeeper's zxids
var PerfectZXidMode bool = true

func NewZKEtcd(c *etcd.Client, s Session) ZK { return &zkEtcd{c, s} }

func (z *zkEtcd) Create(xid Xid, op *CreateRequest) ZKResponse {
	// validate the path is a correct zookeeper path
	zkPath := op.Path
	// zookeeper sequence keys must be checked presuming a number is added,
	// ZK upstream implements it by transforming the proposed path as below.
	if (op.Flags & FlagSequence) != 0 {
		zkPath = fmt.Sprintf("%s1", zkPath)
	}
	if err := validatePath(zkPath); err != nil {
		zxid, err := z.incrementAndGetZxid()
		if err != nil {
			return mkErr(err)
		}
		return mkZKErr(xid, zxid, errBadArguments)
	}

	opts := []etcd.OpOption{}
	if (op.Flags & FlagEphemeral) != 0 {
		opts = append(opts, etcd.WithLease(etcd.LeaseID(z.s.Sid())))
	}
	if (op.Flags & ^(FlagSequence | FlagEphemeral)) != 0 {
		// support seq flag
		panic("unsupported create flags")
	}

	var p, respPath string // path of new node, passed back from txn
	pp := mkPath(path.Dir(op.Path))
	pkey := "/zk/cver/" + pp
	applyf := func(s v3sync.STM) (err error) {
		defer func() {
			if PerfectZXidMode && err != nil {
				s.Put("/zk/err-node", "1")
			}
		}()

		if len(op.Acl) == 0 {
			return ErrInvalidACL
		}
		if len(pp) != 2 && s.Rev("/zk/ctime/"+pp) == 0 {
			// no parent
			return ErrNoNode
		}

		p, respPath = mkPath(op.Path), op.Path
		if op.Flags&FlagSequence != 0 {
			count := int32(decodeInt64([]byte(s.Get("/zk/count/" + pp))))
			// force as int32 to get integer overflow as per zk docs
			cstr := fmt.Sprintf("%010d", count)
			p += cstr
			respPath += cstr
			count++
			s.Put("/zk/count/"+pp, encodeInt64(int64(count)))
		} else if s.Rev("/zk/ctime/"+p) != 0 {
			return ErrNodeExists
		}

		t := encodeTime()

		// update parent key's version by blindly writing an empty value
		s.Put(pkey, "")

		// creating a znode will NOT update its parent mtime
		// s.Put("/zk/mtime/"+pp, t)

		s.Put("/zk/key/"+p, string(op.Data), opts...)
		s.Put("/zk/ctime/"+p, t, opts...)
		s.Put("/zk/mtime/"+p, t, opts...)
		s.Put("/zk/ver/"+p, encodeInt64(0), opts...)
		s.Put("/zk/cver/"+p, "", opts...)
		s.Put("/zk/aver/"+p, encodeInt64(0), opts...)
		s.Put("/zk/acl/"+p, encodeACLs(op.Acl), opts...)
		s.Put("/zk/count/"+p, encodeInt64(0), opts...)

		return nil
	}

	var apiErr error
	resp, err := z.doSTM(wrapErr(&apiErr, applyf))
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(resp.Header.Revision)
	switch apiErr {
	case nil:
	case ErrNoNode:
		// parent missing
		return mkZKErr(xid, zxid, errNoNode)
	case ErrNodeExists:
		// this key already exists
		return mkZKErr(xid, zxid, errNodeExists)
	case ErrInvalidACL:
		return mkZKErr(xid, zxid, errInvalidAcl)
	default:
		return mkZKErr(xid, zxid, errAPIError)
	}

	z.s.Wait(zxid, p, EventNodeCreated)
	crResp := &CreateResponse{respPath}

	glog.V(7).Infof("Create(%v) = (zxid=%v, resp=%+v); txnresp.Header: %+v", zxid, xid, *crResp, resp.Header)
	return mkZKResp(xid, zxid, crResp)
}

func (z *zkEtcd) GetChildren2(xid Xid, op *GetChildren2Request) ZKResponse {
	resp := &GetChildren2Response{}
	p := mkPath(op.Path)

	txnresp, err := z.c.Txn(z.c.Ctx()).Then(statGets(p)...).Commit()
	if err != nil {
		return mkErr(err)
	}

	resp.Stat = statTxn(txnresp)
	if len(p) != 2 && resp.Stat.Ctime == 0 {
		return mkZKErr(xid, ZXid(txnresp.Header.Revision), errNoNode)
	}

	children := txnresp.Responses[5].GetResponseRange()
	for _, kv := range children.Kvs {
		zkkey := strings.Replace(string(kv.Key), getListPfx(p), "", 1)
		resp.Children = append(resp.Children, zkkey)
	}

	zxid := ZXid(children.Header.Revision)
	z.s.Wait(zxid, p, EventNodeChildrenChanged)

	if op.Watch {
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeChildrenChanged,
				State: StateSyncConnected,
				Path:  op.Path,
			}
			glog.V(7).Infof("WatchChild (%v,%v,%+v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(zxid, xid, p, EventNodeChildrenChanged, f)
	}

	glog.V(7).Infof("GetChildren2(%v) = (zxid=%v, resp=%+v)", zxid, xid, *resp)
	return mkZKResp(xid, zxid, resp)
}

func (z *zkEtcd) Ping(xid Xid, op *PingRequest) ZKResponse {
	return mkZKResp(xid, z.s.ZXid(), &PingResponse{})
}

func (z *zkEtcd) Delete(xid Xid, op *DeleteRequest) ZKResponse {
	p := mkPath(op.Path)
	pp := mkPath(path.Dir(op.Path))
	key := "/zk/key/" + p

	applyf := func(s v3sync.STM) error {
		if len(pp) != 2 && s.Rev("/zk/ctime/"+pp) == 0 {
			// no parent
			if PerfectZXidMode {
				s.Put("/zk/err-node", "1")
			}
			return ErrNoNode
		}
		if s.Rev("/zk/ctime/"+p) == 0 {
			if PerfectZXidMode {
				s.Put("/zk/err-node", "1")
			}
			return ErrNoNode
		}
		ver := Ver(decodeInt64([]byte(s.Get("/zk/ver/" + p))))
		if op.Version != Ver(-1) && op.Version != ver {
			return ErrBadVersion
		}

		s.Put("/zk/cver/"+pp, "")

		// deleting a znode will NOT update its parent mtime
		// s.Put("/zk/mtime/"+pp, encodeTime())

		s.Del(key)
		s.Del("/zk/ctime/" + p)
		s.Del("/zk/mtime/" + p)
		s.Del("/zk/ver/" + p)
		s.Del("/zk/cver/" + p)
		s.Del("/zk/aver/" + p)
		s.Del("/zk/acl/" + p)
		s.Del("/zk/count/" + p)

		return nil
	}

	var apiErr error
	resp, err := z.doSTM(wrapErr(&apiErr, applyf))
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(resp.Header.Revision)
	switch apiErr {
	case nil:
	case ErrNoNode:
		return mkZKErr(xid, zxid, errNoNode)
	case ErrBadVersion:
		return mkZKErr(xid, zxid, errBadVersion)
	default:
		return mkZKErr(xid, zxid, errAPIError)
	}

	delResp := &DeleteResponse{}
	z.s.Wait(zxid, p, EventNodeDeleted)

	glog.V(7).Infof("Delete(%v) = (zxid=%v, resp=%+v)", xid, zxid, *delResp)
	return mkZKResp(xid, zxid, delResp)
}

func (z *zkEtcd) Exists(xid Xid, op *ExistsRequest) ZKResponse {
	p := mkPath(op.Path)
	gets := statGets(p)
	txnresp, err := z.c.Txn(z.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return mkErr(err)
	}

	exResp := &ExistsResponse{}
	exResp.Stat = statTxn(txnresp)
	zxid := ZXid(txnresp.Header.Revision)
	z.s.Wait(zxid, p, EventNodeCreated)

	if op.Watch {
		ev := EventNodeDeleted
		if exResp.Stat.Mtime == 0 {
			ev = EventNodeCreated
		}
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  ev,
				State: StateSyncConnected,
				Path:  op.Path,
			}
			glog.V(7).Infof("WatchExists (%v,%v,%+v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(zxid, xid, p, ev, f)
	}

	if exResp.Stat.Mtime == 0 {
		return mkZKErr(xid, zxid, errNoNode)
	}

	glog.V(7).Infof("Exists(%v) = (zxid=%v, resp=%+v)", xid, zxid, *exResp)
	return mkZKResp(xid, zxid, exResp)
}

func (z *zkEtcd) GetData(xid Xid, op *GetDataRequest) ZKResponse {
	p := mkPath(op.Path)
	gets := statGets(p)
	txnresp, err := z.c.Txn(z.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(txnresp.Header.Revision)

	datResp := &GetDataResponse{}
	datResp.Stat = statTxn(txnresp)
	if datResp.Stat.Mtime == 0 {
		return mkZKErr(xid, zxid, errNoNode)
	}

	z.s.Wait(zxid, p, EventNodeDataChanged)

	if op.Watch {
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeDataChanged,
				State: StateSyncConnected,
				Path:  op.Path,
			}
			glog.V(7).Infof("WatchData (%v,%v,%+v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(zxid, xid, p, EventNodeDataChanged, f)
	}
	datResp.Data = []byte(txnresp.Responses[2].GetResponseRange().Kvs[0].Value)

	glog.V(7).Infof("GetData(%v) = (zxid=%v, resp=%+v)", xid, zxid, *datResp)
	return mkZKResp(xid, zxid, datResp)
}

func (z *zkEtcd) SetData(xid Xid, op *SetDataRequest) ZKResponse {
	if err := validatePath(op.Path); err != nil {
		zxid, err := z.incrementAndGetZxid()
		if err != nil {
			return mkErr(err)
		}
		return mkZKErr(xid, zxid, errBadArguments)
	}

	p := mkPath(op.Path)
	var statResp etcd.TxnResponse
	applyf := func(s v3sync.STM) error {
		if s.Rev("/zk/ver/"+p) == 0 {
			if PerfectZXidMode {
				s.Put("/zk/err-node", "2")
			}
			return ErrNoNode
		}
		currentVersion := Ver(decodeInt64([]byte(s.Get("/zk/ver/" + p))))
		if op.Version != Ver(-1) && op.Version != currentVersion {
			return ErrBadVersion

		}
		s.Put("/zk/key/"+p, string(op.Data))
		s.Put("/zk/ver/"+p, string(encodeInt64(int64(currentVersion+1))))
		s.Put("/zk/mtime/"+p, encodeTime())

		resp, err := z.c.Txn(z.c.Ctx()).Then(statGets(p)...).Commit()
		if err != nil {
			return err
		}
		statResp = *resp
		return nil
	}
	var apiErr error
	resp, err := z.doSTM(wrapErr(&apiErr, applyf))
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(resp.Header.Revision)
	switch apiErr {
	case nil:
	case ErrNoNode:
		return mkZKErr(xid, zxid, errNoNode)
	case ErrBadVersion:
		return mkZKErr(xid, zxid, errBadVersion)
	default:
		return mkZKErr(xid, zxid, errAPIError)
	}

	sdresp := &SetDataResponse{}
	sdresp.Stat = statTxn(&statResp)

	glog.V(7).Infof("SetData(%v) = (zxid=%v, resp=%+v)", xid, zxid, *sdresp)
	return mkZKResp(xid, zxid, sdresp)
}

func (z *zkEtcd) GetAcl(xid Xid, op *GetAclRequest) ZKResponse {
	resp := &GetAclResponse{}
	p := mkPath(op.Path)

	gets := []etcd.Op{etcd.OpGet("/zk/acl/" + p)}
	gets = append(gets, statGets(p)...)
	txnresp, err := z.c.Txn(z.c.Ctx()).Then(gets...).Commit()
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(txnresp.Header.Revision)
	resps := txnresp.Responses
	txnresp.Responses = resps[1:]
	resp.Stat = statTxn(txnresp)
	if resp.Stat.Ctime == 0 {
		return mkZKErr(xid, zxid, errNoNode)
	}
	resp.Acl = decodeACLs(resps[0].GetResponseRange().Kvs[0].Value)

	glog.V(7).Infof("GetAcl(%v) = (zxid=%v, resp=%+v)", xid, zxid, *resp)
	return mkZKResp(xid, zxid, resp)
}

func (z *zkEtcd) SetAcl(xid Xid, op *SetAclRequest) ZKResponse {
	if err := validatePath(op.Path); err != nil {
		zxid, err := z.incrementAndGetZxid()
		if err != nil {
			return mkErr(err)
		}
		return mkZKErr(xid, zxid, errBadArguments)
	}
	panic("setAcl")
}

func (z *zkEtcd) GetChildren(xid Xid, op *GetChildrenRequest) ZKResponse {
	p := mkPath(op.Path)
	txnresp, err := z.c.Txn(z.c.Ctx()).Then(statGets(p)...).Commit()
	if err != nil {
		return mkErr(err)
	}

	s := statTxn(txnresp)
	if len(p) != 2 && s.Ctime == 0 {
		return mkZKErr(xid, ZXid(txnresp.Header.Revision), errNoNode)
	}

	children := txnresp.Responses[5].GetResponseRange()
	resp := &GetChildrenResponse{}
	for _, kv := range children.Kvs {
		zkkey := strings.Replace(string(kv.Key), getListPfx(p), "", 1)
		resp.Children = append(resp.Children, zkkey)
	}
	zxid := ZXid(children.Header.Revision)

	if op.Watch {
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeChildrenChanged,
				State: StateSyncConnected,
				Path:  op.Path,
			}
			glog.V(7).Infof("WatchChild (%v,%v,%+v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(zxid, xid, p, EventNodeChildrenChanged, f)
	}

	glog.V(7).Infof("GetChildren(%v) = (zxid=%v, resp=%+v)", xid, zxid, *resp)
	return mkZKResp(xid, zxid, resp)
}

func (z *zkEtcd) Sync(xid Xid, op *SyncRequest) ZKResponse {
	// linearized read
	resp, err := z.c.Get(z.c.Ctx(), "/zk/ver/"+mkPath(op.Path))
	if err != nil {
		return mkErr(err)
	}

	zxid := ZXid(resp.Header.Revision)
	if len(resp.Kvs) == 0 {
		return mkZKErr(xid, zxid, errNoNode)
	}

	glog.V(7).Infof("Sync(%v) = (zxid=%v, resp=%+v)", xid, zxid, *resp)
	return mkZKResp(xid, zxid, &CreateResponse{op.Path})
}

func (z *zkEtcd) Multi(xid Xid, op *MultiRequest) ZKResponse { panic("multi") }

func (z *zkEtcd) Close(xid Xid, op *CloseRequest) ZKResponse {
	resp, _ := z.c.Revoke(z.c.Ctx(), etcd.LeaseID(z.s.Sid()))
	zxid := ZXid(0)
	if resp != nil {
		zxid = ZXid(resp.Header.Revision)
	}
	return mkZKResp(xid, zxid, &CloseResponse{})
}

func (z *zkEtcd) SetAuth(xid Xid, op *SetAuthRequest) ZKResponse { panic("setAuth") }

func (z *zkEtcd) SetWatches(xid Xid, op *SetWatchesRequest) ZKResponse {
	for _, dw := range op.DataWatches {
		dataPath := dw
		p := mkPath(dataPath)
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeDataChanged,
				State: StateSyncConnected,
				Path:  dataPath,
			}
			glog.V(7).Infof("WatchData* (%v,%v,%v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(op.RelativeZxid, xid, p, EventNodeDataChanged, f)
	}

	ops := make([]etcd.Op, len(op.ExistWatches))
	for i, ew := range op.ExistWatches {
		ops[i] = etcd.OpGet(
			"/zk/ver/"+mkPath(ew),
			etcd.WithSerializable(),
			etcd.WithRev(int64(op.RelativeZxid)))
	}

	resp, err := z.c.Txn(z.c.Ctx()).Then(ops...).Commit()
	if err != nil {
		return mkErr(err)
	}
	curZXid := ZXid(resp.Header.Revision)

	for i, ew := range op.ExistWatches {
		existPath := ew
		p := mkPath(existPath)

		ev := EventNodeDeleted
		if len(resp.Responses[i].GetResponseRange().Kvs) == 0 {
			ev = EventNodeCreated
		}
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  ev,
				State: StateSyncConnected,
				Path:  existPath,
			}
			glog.V(7).Infof("WatchExist* (%v,%v,%v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(op.RelativeZxid, xid, p, ev, f)
	}
	for _, cw := range op.ChildWatches {
		childPath := cw
		p := mkPath(childPath)
		f := func(newzxid ZXid) {
			wresp := &WatcherEvent{
				Type:  EventNodeChildrenChanged,
				State: StateSyncConnected,
				Path:  childPath,
			}
			glog.V(7).Infof("WatchChild* (%v,%v,%v)", xid, newzxid, *wresp)
			z.s.Send(-1, -1, wresp)
		}
		z.s.Watch(op.RelativeZxid, xid, p, EventNodeChildrenChanged, f)
	}

	swresp := &SetWatchesResponse{}

	glog.V(7).Infof("SetWatches(%v) = (zxid=%v, resp=%+v)", xid, curZXid, *swresp)
	return mkZKResp(xid, curZXid, swresp)
}

func (z *zkEtcd) doSTM(applyf func(s v3sync.STM) error) (*etcd.TxnResponse, error) {
	return v3sync.NewSTMSerializable(z.c.Ctx(), z.c, applyf)
}

// incrementAndGetZxid forces a write to the err-node to increment the Zxid
// to keep the numbers aligned with Zookeeper's semantics. It is gated on the
// PerfectZxid global at the moment (which is hardcoded true).
func (z *zkEtcd) incrementAndGetZxid() (ZXid, error) {
	applyf := func(s v3sync.STM) (err error) {
		if PerfectZXidMode {
			s.Put("/zk/err-node", "1")
		}
		return nil
	}
	resp, err := z.doSTM(applyf)
	if err != nil {
		return -1, err
	}
	return ZXid(resp.Header.Revision), nil
}

func encodeACLs(acls []ACL) string {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(acls)
	return b.String()
}

func decodeACLs(acls []byte) (ret []ACL) {
	var b bytes.Buffer
	b.Write(acls)
	gob.NewDecoder(&b).Decode(&ret)
	return ret
}

func encodeTime() string {
	return encodeInt64(time.Now().UnixNano() / 1000)
}

func decodeInt64(v []byte) int64 { x, _ := binary.Varint(v); return x }

func encodeInt64(v int64) string {
	b := make([]byte, binary.MaxVarintLen64)
	return string(b[:binary.PutVarint(b, v)])
}

func mkErr(err error) ZKResponse { return ZKResponse{Err: err} }

func rev2zxid(rev int64) ZXid {
	// zxid is -1 because etcd starts at 1 but zk starts at 0
	return ZXid(rev - 1)
}

func mkZKErr(xid Xid, zxid ZXid, err ErrCode) ZKResponse {
	return ZKResponse{Hdr: &ResponseHeader{xid, zxid - 1, err}}
}

func mkZKResp(xid Xid, zxid ZXid, resp interface{}) ZKResponse {
	return ZKResponse{Hdr: &ResponseHeader{xid, zxid - 1, 0}, Resp: resp}
}

// wrapErr is to pass back error info but still get the txn response
func wrapErr(err *error, f func(s v3sync.STM) error) func(s v3sync.STM) error {
	return func(s v3sync.STM) error {
		if ferr := f(s); ferr != nil {
			*err = ferr
		}
		return nil
	}
}
