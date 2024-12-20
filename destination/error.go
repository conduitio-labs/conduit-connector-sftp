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

package destination

import (
	"fmt"
)

var ErrUntrustedKey = fmt.Errorf("host key does not match the trusted key")

type MismatchKeyTypeError struct {
	key1, key2 string
}

func (e MismatchKeyTypeError) Error() string {
	return fmt.Sprintf("host key type mismatch: got %s, want %s", e.key1, e.key2)
}

func NewMismatchKeyTypeError(key1, key2 string) MismatchKeyTypeError {
	return MismatchKeyTypeError{key1, key2}
}

type InvalidChunkError struct {
	message string
}

func (e InvalidChunkError) Error() string {
	return fmt.Sprintf("invalid chunk: %s", e.message)
}

func NewInvalidChunkError(msg string) InvalidChunkError {
	return InvalidChunkError{msg}
}
