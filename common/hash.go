// Copyright © 2024 Meroxa, Inc.
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

package common

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateFileHash creates a unique hash based on file name, mod time, and size.
func GenerateFileHash(fileName string, modTime time.Time, fileSize int64) string {
	data := fmt.Sprintf("%s|%s|%d", fileName, modTime.Format(time.RFC3339), fileSize)
	hash := md5.Sum([]byte(data)) //nolint: gosec // MD5 used for non-cryptographic unique identifier
	return hex.EncodeToString(hash[:])
}
