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

var (
	errPathEmpty         = errors.New("path cannot be null or zero-length")
	errPathBadPrefix     = errors.New("path must start with / character")
	errPathTrailingSlash = errors.New("path must not end with a / character")
	errPathNullCharacter = errors.New("null character not allowed")
	errPathEmptyNodeName = errors.New("empty node name specified")
	errPathIsRelative    = errors.New("relative paths not allowed")
	errPathBadCharacter  = errors.New("invalid character")

	rootPath = mkPath("/")
)

// validatePath checks the supplied path against the ZK rules for valid node
// paths. Implements upstream ZK checks from
// ./src/java/main/org/apache/zookeeper/common/PathUtils.java
func validatePath(zkPath string) error {
	if len(zkPath) == 0 {
		return errPathEmpty
	}

	if zkPath[0] != '/' {
		return errPathBadPrefix
	}

	if len(zkPath) == 1 { // done checking - it's the root
		return nil
	}

	if zkPath[len(zkPath)-1] == '/' {
		return errPathTrailingSlash
	}

	var c rune
	chars := []rune(zkPath)

	for i, lastc := 1, rune('/'); i < len(chars); i, lastc = i+1, chars[i] {
		c = chars[i]

		if c == 0 {
			return errPathNullCharacter
		} else if c == '/' && lastc == '/' {
			return errPathEmptyNodeName
		} else if c == '.' && lastc == '.' {
			if chars[i-2] == '/' && ((i+1) == len(chars) || chars[i+1] == '/') {
				return errPathIsRelative
			}
		} else if c == '.' {
			if chars[i-1] == '/' && (i+1 == len(chars) || chars[i+1] == '/') {
				return errPathIsRelative
			}
		} else if c > 0x0000 && c < 0x001f ||
			c > 0x007f && c < 0x009F ||
			c > 0xd800 && c < 0xf8ff ||
			c > 0xfff0 && c < 0xffff {
			return errPathBadCharacter
		}
	}

	return nil
}

func getListPfx(p string) string {
	if p != rootPath {
		// /abc => 1 => listing dir needs search on p[0] = 2
		return mkPathMTime(incPath(p) + "/")
	}
	return mkPathMTime(p)
}
