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
	etcd "github.com/coreos/etcd/clientv3"
)

func statGets(p string) []etcd.Op {
	return []etcd.Op{
		etcd.OpGet(mkPathCTime(p), etcd.WithSerializable()),
		etcd.OpGet(mkPathMTime(p), etcd.WithSerializable(),
			etcd.WithSort(etcd.SortByModRevision, etcd.SortDescend)),
		etcd.OpGet(mkPathKey(p), etcd.WithSerializable()),
		etcd.OpGet(mkPathCVer(p), etcd.WithSerializable()),
		etcd.OpGet(mkPathACL(p), etcd.WithSerializable(), etcd.WithKeysOnly()),
		// to compute num children
		etcd.OpGet(getListPfx(p), etcd.WithSerializable(), etcd.WithPrefix()),
	}
}

func statTxn(txnresp *etcd.TxnResponse) (s Stat) {
	ctime := txnresp.Responses[0].GetResponseRange()
	mtime := txnresp.Responses[1].GetResponseRange()
	node := txnresp.Responses[2].GetResponseRange()
	cver := txnresp.Responses[3].GetResponseRange()
	aver := txnresp.Responses[4].GetResponseRange()
	children := txnresp.Responses[5].GetResponseRange()

	// XXX hack: need to format zk / node instead of this garbage
	if len(ctime.Kvs) != 0 {
		s.Ctime = decodeInt64(ctime.Kvs[0].Value)
		s.Czxid = rev2zxid(ctime.Kvs[0].ModRevision)
		s.Pzxid = s.Czxid
	}
	if len(mtime.Kvs) != 0 {
		s.Mzxid = rev2zxid(mtime.Kvs[0].ModRevision)
		s.Mtime = decodeInt64(mtime.Kvs[0].Value)
		s.Version = Ver(mtime.Kvs[0].Version - 1)
	}
	if len(cver.Kvs) != 0 {
		s.Cversion = Ver(cver.Kvs[0].Version - 1)
		s.Pzxid = rev2zxid(cver.Kvs[0].ModRevision)
	}
	if len(aver.Kvs) != 0 {
		s.Aversion = Ver(aver.Kvs[0].Version - 1)
	}
	if len(node.Kvs) != 0 {
		s.EphemeralOwner = Sid(node.Kvs[0].Lease)
		s.DataLength = int32(len(node.Kvs[0].Value))
	}
	s.NumChildren = int32(len(children.Kvs))
	return s
}
