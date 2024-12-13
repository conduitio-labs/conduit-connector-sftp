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

package source

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Position represents SFTP's position.
type Position struct {
	LastProcessedFileTimestamp time.Time `json:"lastProcessedFileTimestamp"`
}

// ParseSDKPosition parses opencdc.Position and returns Position.
func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	if position == nil {
		return &Position{}, nil
	}

	var pos Position
	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal position: %w", err)
	}

	return &pos, nil
}
