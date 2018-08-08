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

package xchk

import (
	"bytes"
	"sort"
	"time"

	"github.com/etcd-io/zetcd"
	"github.com/golang/glog"
)

// zkXchk takes incoming ZK requests and forwards them to a remote ZK server
type zkXchk struct {
	s *session

	cZK  zetcd.ZK
	oZK  zetcd.ZK
	errc chan<- error
}

func newZK(s *session, cZKf, oZKf zetcd.ZKFunc, errc chan<- error) (*zkXchk, error) {
	cZK, cerr := cZKf(s.candidate)
	if cerr != nil {
		return nil, cerr
	}
	oZK, oerr := oZKf(s.oracle)
	if oerr != nil {
		return nil, oerr
	}
	return &zkXchk{s, cZK, oZK, errc}, nil
}

func (xchk *zkXchk) Create(xid zetcd.Xid, op *zetcd.CreateRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Create(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Create(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.CreateResponse), or.Resp.(*zetcd.CreateResponse)
	if crr.Path != orr.Path {
		err = errPath
	}
	return or
}

func (xchk *zkXchk) Delete(xid zetcd.Xid, op *zetcd.DeleteRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Delete(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Delete(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	return or
}

func (xchk *zkXchk) Exists(xid zetcd.Xid, op *zetcd.ExistsRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Exists(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Exists(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.ExistsResponse), or.Resp.(*zetcd.ExistsResponse)

	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) GetData(xid zetcd.Xid, op *zetcd.GetDataRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetData(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetData(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetDataResponse), or.Resp.(*zetcd.GetDataResponse)

	if !bytes.Equal(crr.Data, orr.Data) {
		err = errData
	}
	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) SetData(xid zetcd.Xid, op *zetcd.SetDataRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetData(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetData(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SetDataResponse), or.Resp.(*zetcd.SetDataResponse)

	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) GetAcl(xid zetcd.Xid, op *zetcd.GetAclRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetAcl(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetAcl(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetAclResponse), or.Resp.(*zetcd.GetAclResponse)

	if len(crr.Acl) != len(orr.Acl) {
		err = errNumAcl
		return or
	}

	for i := range crr.Acl {
		if crr.Acl[i] != orr.Acl[i] {
			err = errAcl
			return or
		}
	}

	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}

	return or
}

func (xchk *zkXchk) SetAcl(xid zetcd.Xid, op *zetcd.SetAclRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetAcl(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetAcl(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SetAclResponse), or.Resp.(*zetcd.SetAclResponse)

	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}

	return or
}

func (xchk *zkXchk) GetChildren(xid zetcd.Xid, op *zetcd.GetChildrenRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetChildren(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetChildren(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetChildrenResponse), or.Resp.(*zetcd.GetChildrenResponse)

	if len(crr.Children) != len(orr.Children) {
		err = errNumChildren
		return or
	}

	sort.StringSlice(crr.Children).Sort()
	sort.StringSlice(orr.Children).Sort()
	for i := range crr.Children {
		if crr.Children[i] != orr.Children[i] {
			err = errChildren
			return or
		}
	}

	return or
}

func (xchk *zkXchk) Sync(xid zetcd.Xid, op *zetcd.SyncRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Sync(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Sync(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.SyncResponse), or.Resp.(*zetcd.SyncResponse)
	if crr.Path != orr.Path {
		err = errPath
	}
	return or
}

func (xchk *zkXchk) Ping(xid zetcd.Xid, op *zetcd.PingRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Ping(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Ping(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) GetChildren2(xid zetcd.Xid, op *zetcd.GetChildren2Request) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.GetChildren2(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.GetChildren2(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	if err != nil || or.Resp == nil {
		return or
	}
	crr, orr := cr.Resp.(*zetcd.GetChildren2Response), or.Resp.(*zetcd.GetChildren2Response)
	if len(crr.Children) != len(orr.Children) {
		err = errNumChildren
		return or
	}

	sort.StringSlice(crr.Children).Sort()
	sort.StringSlice(orr.Children).Sort()
	for i := range crr.Children {
		if crr.Children[i] != orr.Children[i] {
			err = errChildren
			return or
		}
	}
	if !xchk.xchkStat(crr.Stat, orr.Stat) {
		err = errStat
	}
	return or
}

func (xchk *zkXchk) Multi(xid zetcd.Xid, op *zetcd.MultiRequest) zetcd.ZKResponse { panic("wut") }

func (xchk *zkXchk) Close(xid zetcd.Xid, op *zetcd.CloseRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.Close(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.Close(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) SetAuth(xid zetcd.Xid, op *zetcd.SetAuthRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetAuth(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetAuth(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	return or
}

func (xchk *zkXchk) SetWatches(xid zetcd.Xid, op *zetcd.SetWatchesRequest) zetcd.ZKResponse {
	cf := func() zetcd.ZKResponse { return xchk.cZK.SetWatches(xid, op) }
	of := func() zetcd.ZKResponse { return xchk.oZK.SetWatches(xid, op) }
	cr, or, err := xchk.xchkResp(cf, of)
	defer func() { xchk.reportErr(cr, or, err) }()
	return or
}

type zkfunc func() zetcd.ZKResponse

func xchkHdr(cresp, oresp zetcd.ZKResponse) error {
	if cresp.Err != nil || oresp.Err != nil {
		return errErr
	}
	if cresp.Hdr == nil || oresp.Hdr == nil {
		return errErr
	}
	if cresp.Hdr.Err != oresp.Hdr.Err {
		return errErr
	}
	if cresp.Hdr.Zxid != oresp.Hdr.Zxid {
		return errZXid
	}
	return nil
}

func (xchk *zkXchk) xchkResp(cf, of zkfunc) (cresp zetcd.ZKResponse, oresp zetcd.ZKResponse, err error) {
	cch, och := make(chan zetcd.ZKResponse, 1), make(chan zetcd.ZKResponse, 1)
	go func() { cch <- cf() }()
	go func() { och <- of() }()
	select {
	case cresp = <-cch:
	case oresp = <-och:
	}
	select {
	case cresp = <-cch:
	case oresp = <-och:
	case <-time.After(time.Second):
		xchk.reportErr(cresp, oresp, errBadAuth)
		select {
		case cresp = <-cch:
		case oresp = <-och:
		}
	}
	return cresp, oresp, xchkHdr(cresp, oresp)
}

func (xchk *zkXchk) reportErr(cr, or zetcd.ZKResponse, err error) {
	if err == nil {
		return
	}
	xerr := &XchkError{err: err, cr: cr, or: or}
	glog.Warning(xerr)
	if xchk.errc != nil {
		select {
		case xchk.errc <- xerr:
		case <-xchk.s.StopNotify():
			return
		}
	}
}

func (xchk *zkXchk) xchkStat(cStat, oStat zetcd.Stat) bool {
	ctdiff, otdiff := cStat.Ctime-cStat.Mtime, oStat.Ctime-oStat.Mtime
	if ctdiff != otdiff && otdiff == 0 {
		// expect equal times to be equal
		return false
	}

	if oStat.EphemeralOwner != 0 {
		csid, _ := xchk.s.sp.get(oStat.EphemeralOwner)
		if cStat.EphemeralOwner != csid {
			return false
		}
		// ephemeral owners confirmed to be equivalent
		cStat.EphemeralOwner = oStat.EphemeralOwner
	} else if cStat.EphemeralOwner != 0 {
		return false
	}

	// times will never be equivalent, so fake it
	cStat.Ctime, cStat.Mtime = oStat.Ctime, oStat.Mtime
	return cStat == oStat
}
