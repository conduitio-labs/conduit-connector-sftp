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

package config

import (
	"fmt"
)

type RequiredWithError struct {
	field     string
	withField string
}

func (e RequiredWithError) Error() string {
	return fmt.Sprintf("%q value is required if %q is provided", e.field, e.withField)
}

func NewRequiredWithError(field, withField string) RequiredWithError {
	return RequiredWithError{field: field, withField: withField}
}
