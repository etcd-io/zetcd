// Copyright 2017 CoreOS, Inc.
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
	"fmt"

	"github.com/etcd-io/zetcd"
)

var (
	errStat        = fmt.Errorf("stat mismatch")
	errData        = fmt.Errorf("data mismatch")
	errAcl         = fmt.Errorf("acl mismatch")
	errNumAcl      = fmt.Errorf("acl length mismatch")
	errPath        = fmt.Errorf("path mismatch")
	errErr         = fmt.Errorf("err mismatch")
	errZXid        = fmt.Errorf("zxid mismatch")
	errNumChildren = fmt.Errorf("number of children mismatch")
	errChildren    = fmt.Errorf("children paths mismatch")
	errBadAuth     = fmt.Errorf("auth mismatch")
)

type XchkError struct {
	err error
	cr  zetcd.ZKResponse
	or  zetcd.ZKResponse

	acr zetcd.AuthResponse
	aor zetcd.AuthResponse
}

func (xe *XchkError) Error() string {
	switch {
	case xe.err == errErr || xe.err == errZXid:
		return fmt.Sprintf("xchk failed (%v)\ncandidate: %+v\noracle: %+v\n", xe.err, xe.cr.Hdr, xe.or.Hdr)
	case xe.cr.Resp != nil && xe.or.Resp != nil:
		return fmt.Sprintf("xchk failed (%v)\ncandidate: %+v\noracle: %+v\n", xe.err, xe.cr.Resp, xe.or.Resp)
	case xe.cr.Hdr != nil && xe.or.Hdr != nil:
		return fmt.Sprintf("xchk failed (%v)\ncandidate: %+v\noracle: %+v\n", xe.err, xe.cr.Hdr, xe.or.Hdr)
	default:
		return fmt.Sprintf("xchk failed (%v)\ncandidate: %+v\noracle: %+v", xe.err, xe.cr, xe.or)
	}
}
