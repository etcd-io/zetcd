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

type opBundle struct {
	apply func(v3sync.STM) error
	reply func(Xid, ZXid) ZKResponse
}

// PerfectZXid is enabled to insert err writes to match zookeeper's zxids
var PerfectZXidMode bool = true

func NewZKEtcd(c *etcd.Client, s Session) ZK { return &zkEtcd{c, s} }

func (z *zkEtcd) Create(xid Xid, op *CreateRequest) ZKResponse {
	b := z.mkCreateTxnOp(op)
	resp, zkErr := z.doWrappedSTM(xid, b.apply)
	if resp == nil {
		return zkErr
	}
	glog.V(7).Infof("Create(%v) = (zxid=%v); txnresp: %+v", xid, resp.Header.Revision, *resp)
	return b.reply(xid, ZXid(resp.Header.Revision))
}

func (z *zkEtcd) mkCreateTxnOp(op *CreateRequest) opBundle {
	var p, respPath string // path of new node, passed back from txn

	// validate the path is a correct zookeeper path
	zkPath := op.Path
	// zookeeper sequence keys must be checked presuming a number is added,
	// ZK upstream implements it by transforming the proposed path as below.
	if (op.Flags & FlagSequence) != 0 {
		zkPath = fmt.Sprintf("%s1", zkPath)
	}
	if err := validatePath(zkPath); err != nil {
		return mkErrTxnOp(ErrInvalidACL)
	}

	opts := []etcd.OpOption{}
	if (op.Flags & FlagEphemeral) != 0 {
		opts = append(opts, etcd.WithLease(etcd.LeaseID(z.s.Sid())))
	}
	if (op.Flags & ^(FlagSequence | FlagEphemeral)) != 0 {
		// support seq flag
		panic("unsupported create flags")
	}

	pp := mkPath(path.Dir(op.Path))
	pkey := mkPathCVer(pp)
	apply := func(s v3sync.STM) (err error) {
		defer func() {
			if err != nil {
				updateErrRev(s)
			}
		}()

		if len(op.Acl) == 0 {
			return ErrInvalidACL
		}
		if pp != rootPath && len(s.Get(mkPathCTime(pp))) == 0 {
			// no parent
			return ErrNoNode
		}

		p, respPath = mkPath(op.Path), op.Path
		if op.Flags&FlagSequence != 0 {
			count := int32(decodeInt64([]byte(s.Get(mkPathCount(pp)))))
			// force as int32 to get integer overflow as per zk docs
			cstr := fmt.Sprintf("%010d", count)
			p += cstr
			respPath += cstr
			count++
			s.Put(mkPathCount(pp), encodeInt64(int64(count)))
		} else if len(s.Get(mkPathCTime(p))) != 0 {
			return ErrNodeExists
		}

		t := encodeTime()

		// update parent key's version by blindly writing an empty value
		s.Put(pkey, "")

		// creating a znode will NOT update its parent mtime
		// s.Put("/zk/mtime/"+pp, t)

		s.Put(mkPathKey(p), string(op.Data), opts...)
		s.Put(mkPathCTime(p), t, opts...)
		s.Put(mkPathMTime(p), t, opts...)
		s.Put(mkPathVer(p), encodeInt64(0), opts...)
		s.Put(mkPathCVer(p), "", opts...)
		s.Put(mkPathACL(p), encodeACLs(op.Acl), opts...)
		s.Put(mkPathCount(p), encodeInt64(0), opts...)
		return nil
	}
	reply := func(xid Xid, zxid ZXid) ZKResponse {
		z.s.Wait(zxid, p, EventNodeCreated)
		return mkZKResp(xid, zxid, &CreateResponse{respPath})
	}
	return opBundle{apply, reply}
}

func (z *zkEtcd) GetChildren2(xid Xid, op *GetChildren2Request) ZKResponse {
	resp := &GetChildren2Response{}
	p := mkPath(op.Path)

	txnresp, err := z.c.Txn(z.c.Ctx()).Then(statGets(p)...).Commit()
	if err != nil {
		return mkErr(err)
	}

	resp.Stat = statTxn(txnresp)
	if op.Path != "/" && resp.Stat.Ctime == 0 {
		return mkZKErr(xid, ZXid(txnresp.Header.Revision), errNoNode)
	}

	children := txnresp.Responses[5].GetResponseRange()
	for _, kv := range children.Kvs {
		zkkey := strings.Replace(string(kv.Key), getListPfx(p), "", 1)
		resp.Children = append(resp.Children, zkkey)
	}

	z.s.Wait(resp.Stat.Pzxid, p, EventNodeChildrenChanged)

	zxid := ZXid(txnresp.Header.Revision)
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
	b := z.mkDeleteTxnOp(op)
	resp, zkErr := z.doWrappedSTM(xid, b.apply)
	if resp == nil {
		return zkErr
	}
	glog.V(7).Infof("Delete(%v) = (zxid=%v, resp=%+v)", xid, resp.Header.Revision, *resp)
	return b.reply(xid, ZXid(resp.Header.Revision))
}

func (z *zkEtcd) mkDeleteTxnOp(op *DeleteRequest) opBundle {
	p := mkPath(op.Path)
	pp := mkPath(path.Dir(op.Path))
	key := mkPathKey(p)

	apply := func(s v3sync.STM) error {
		if pp != rootPath && len(s.Get(mkPathCTime(pp))) == 0 {
			// no parent
			updateErrRev(s)
			return ErrNoNode
		}
		// was s.Rev(mkPathCTime(p)), but stm will not
		// set the rev of a new key until committed
		if len(s.Get(mkPathCTime(p))) == 0 {
			updateErrRev(s)
			return ErrNoNode
		}
		if op.Version != Ver(-1) {
			ver := Ver(decodeInt64([]byte(s.Get(mkPathVer(p)))))
			if op.Version != ver {
				return ErrBadVersion
			}
		}

		crev := s.Rev(mkPathCVer(p))
		gresp, gerr := z.c.Get(z.c.Ctx(), getListPfx(p),
			// TODO: monotonic revisions from serializable
			// etcd.WithSerializable(),
			etcd.WithPrefix(),
			etcd.WithCountOnly(),
			etcd.WithRev(crev),
			etcd.WithLimit(1))
		if gerr != nil {
			return gerr
		}
		if gresp.Count > 0 {
			updateErrRev(s)
			return ErrNotEmpty
		}

		s.Put(mkPathCVer(pp), "")

		// deleting a znode will NOT update its parent mtime
		// s.Put("/zk/mtime/"+pp, encodeTime())

		s.Del(key)
		s.Del(mkPathCTime(p))
		s.Del(mkPathMTime(p))
		s.Del(mkPathVer(p))
		s.Del(mkPathCVer(p))
		s.Del(mkPathACL(p))
		s.Del(mkPathCount(p))

		return nil
	}

	reply := func(xid Xid, zxid ZXid) ZKResponse {
		z.s.Wait(zxid, p, EventNodeDeleted)
		return mkZKResp(xid, zxid, &DeleteResponse{})
	}

	return opBundle{apply, reply}
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
	z.s.Wait(exResp.Stat.Czxid, p, EventNodeCreated)

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

	z.s.Wait(datResp.Stat.Mzxid, p, EventNodeDataChanged)

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
	b := z.mkSetDataTxnOp(op)
	resp, zkErr := z.doWrappedSTM(xid, b.apply)
	if resp == nil {
		return zkErr
	}
	glog.V(7).Infof("SetData(%v) = (zxid=%v); txnresp: %+v", xid, resp.Header.Revision, *resp)
	return b.reply(xid, ZXid(resp.Header.Revision))
}

func (z *zkEtcd) mkSetDataTxnOp(op *SetDataRequest) opBundle {
	if err := validatePath(op.Path); err != nil {
		return mkErrTxnOp(ErrBadArguments)
	}

	p := mkPath(op.Path)
	var statResp etcd.TxnResponse
	apply := func(s v3sync.STM) error {
		if s.Rev(mkPathVer(p)) == 0 {
			updateErrRev(s)
			return ErrNoNode
		}
		currentVersion := Ver(decodeInt64([]byte(s.Get(mkPathVer(p)))))
		if op.Version != Ver(-1) && op.Version != currentVersion {
			return ErrBadVersion

		}
		s.Put(mkPathKey(p), string(op.Data))
		s.Put(mkPathVer(p), string(encodeInt64(int64(currentVersion+1))))
		s.Put(mkPathMTime(p), encodeTime())

		resp, err := z.c.Txn(z.c.Ctx()).Then(statGets(p)...).Commit()
		if err != nil {
			return err
		}
		statResp = *resp
		return nil
	}

	reply := func(xid Xid, zxid ZXid) ZKResponse {
		z.s.Wait(zxid, p, EventNodeDataChanged)
		return mkZKResp(xid, zxid, &SetDataResponse{Stat: statTxn(&statResp)})
	}

	return opBundle{apply, reply}
}

func (z *zkEtcd) GetAcl(xid Xid, op *GetAclRequest) ZKResponse {
	resp := &GetAclResponse{}
	p := mkPath(op.Path)

	gets := []etcd.Op{etcd.OpGet(mkPathACL(p))}
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
	if op.Path != "/" && s.Ctime == 0 {
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
	resp, err := z.c.Get(z.c.Ctx(), mkPathVer(mkPath(op.Path)))
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

func (z *zkEtcd) Multi(xid Xid, mreq *MultiRequest) ZKResponse {
	bs := make([]opBundle, len(mreq.Ops))
	for i, op := range mreq.Ops {
		switch req := op.Op.(type) {
		case *CreateRequest:
			bs[i] = z.mkCreateTxnOp(req)
		case *DeleteRequest:
			bs[i] = z.mkDeleteTxnOp(req)
		case *SetDataRequest:
			bs[i] = z.mkSetDataTxnOp(req)
		case *CheckVersionRequest:
			bs[i] = z.mkCheckVersionPathTxnOp(req)
		default:
			panic(fmt.Sprintf("unknown multi %+v %T", op.Op, op.Op))
		}
	}

	apply := func(s v3sync.STM) error {
		for _, b := range bs {
			if err := b.apply(s); err != nil {
				return err
			}
		}
		return nil
	}

	reply := func(xid Xid, zxid ZXid) ZKResponse {
		ops := make([]MultiResponseOp, len(bs))
		for i, b := range bs {
			resp := b.reply(xid, zxid)
			ops[i].Header = MultiHeader{Err: 0}
			switch r := resp.Resp.(type) {
			case *CreateResponse:
				ops[i].Header.Type = opCreate
				ops[i].String = r.Path
			case *SetDataResponse:
				ops[i].Header.Type = opSetData
				ops[i].Stat = &r.Stat
			case *DeleteResponse:
				ops[i].Header.Type = opDelete
			case *struct{}:
				ops[i].Header.Type = opCheck
			default:
				panic(fmt.Sprintf("unknown multi %+v %T", resp, resp))
			}
		}
		mresp := &MultiResponse{
			Ops:        ops,
			DoneHeader: MultiHeader{Type: opMulti},
		}
		return mkZKResp(xid, zxid, mresp)
	}

	resp, err := z.doSTM(apply)
	if resp == nil {
		// txn aborted, possibly due to any API error
		if _, ok := errorToErrCode[err]; !ok {
			// aborted due to non-API error
			return mkErr(err)
		}
		zxid, zerr := z.incrementAndGetZxid()
		if zerr != nil {
			return mkErr(zerr)
		}
		// zkdocker seems to always return API error...
		zkresp := apiErrToZKErr(xid, zxid, err)
		zkresp.Hdr.Err = errAPIError
		return zkresp
	}

	mresp := reply(xid, ZXid(resp.Header.Revision))
	glog.V(7).Infof("Multi(%v) = (zxid=%v); txnresp: %+v", *mreq, resp.Header.Revision, *resp)
	return mresp
}

func (z *zkEtcd) mkCheckVersionPathTxnOp(op *CheckVersionRequest) opBundle {
	if err := validatePath(op.Path); err != nil {
		return mkErrTxnOp(ErrBadArguments)
	}
	p := mkPath(op.Path)
	apply := func(s v3sync.STM) (err error) {
		if op.Version == Ver(-1) {
			return nil
		}
		ver := s.Get(mkPathVer(p))
		if len(ver) == 0 {
			return ErrNoNode
		}
		zkVer := Ver(decodeInt64([]byte(ver)))
		if op.Version != zkVer {
			return ErrBadVersion
		}
		return nil
	}
	reply := func(xid Xid, zxid ZXid) ZKResponse {
		return mkZKResp(xid, zxid, &struct{}{})
	}
	return opBundle{apply, reply}
}

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
			mkPathVer(mkPath(ew)),
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

func (z *zkEtcd) doWrappedSTM(xid Xid, applyf func(s v3sync.STM) error) (*etcd.TxnResponse, ZKResponse) {
	var apiErr error
	resp, err := z.doSTM(wrapErr(&apiErr, applyf))
	if err != nil {
		return nil, mkErr(err)
	}
	if apiErr != nil {
		return nil, apiErrToZKErr(xid, ZXid(resp.Header.Revision), apiErr)
	}
	return resp, ZKResponse{}
}

func apiErrToZKErr(xid Xid, zxid ZXid, apiErr error) ZKResponse {
	errCode, ok := errorToErrCode[apiErr]
	if !ok {
		errCode = errAPIError
	}
	return mkZKErr(xid, zxid, errCode)
}

func (z *zkEtcd) doSTM(applyf func(s v3sync.STM) error) (*etcd.TxnResponse, error) {
	return v3sync.NewSTMSerializable(z.c.Ctx(), z.c, applyf)
}

// incrementAndGetZxid forces a write to the err-node to increment the Zxid
// to keep the numbers aligned with Zookeeper's semantics. It is gated on the
// PerfectZxid global at the moment (which is hardcoded true).
func (z *zkEtcd) incrementAndGetZxid() (ZXid, error) {
	applyf := func(s v3sync.STM) (err error) {
		updateErrRev(s)
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

// updateErrRev puts to a dummy key to increase the zxid for an error.
func updateErrRev(s v3sync.STM) {
	if PerfectZXidMode {
		s.Put(mkPathErrNode(), "1")
	}
}

func mkErrTxnOp(err error) opBundle {
	return opBundle{
		apply: func(s v3sync.STM) error {
			updateErrRev(s)
			return err
		},
	}
}
