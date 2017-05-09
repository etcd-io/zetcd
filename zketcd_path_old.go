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

// +build !path_debug,path_old

package zetcd

import "fmt"

func mkPath(zkPath string) string {
	p := zkPath
	if p[0] != '/' {
		p = "/" + p
	}
	depth := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			depth++
		}
	}
	return string(append([]byte{byte(depth)}, []byte(p)...))
}

func incPath(zetcdPath string) string {
	b := []byte(zetcdPath)
	b[0]++
	return string(b)
}

func mkPathErrNode() string       { return "/zk/err-node" }
func mkPathKey(p string) string   { return "/zk/key/" + p }
func mkPathVer(p string) string   { return "/zk/ver/" + p }
func mkPathCVer(p string) string  { return "/zk/cver/" + p }
func mkPathCTime(p string) string { return "/zk/ctime/" + p }
func mkPathMTime(p string) string { return "/zk/mtime/" + p }
func mkPathACL(p string) string   { return "/zk/acl/" + p }
func mkPathCount(p string) string { return "/zk/count/" + p }

func mkPathSession(lid uint64) string { return fmt.Sprintf("/zk/ses/%x", lid) }
