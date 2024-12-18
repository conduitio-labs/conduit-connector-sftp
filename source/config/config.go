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
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/config"
)

//go:generate paramgen -output=paramgen.go Config

type Config struct {
	config.Config

	// Pattern to filter files in the source directory.
	FilePattern string `json:"filePattern" default:"*"`
	// This period is used by iterator to poll for new data at regular intervals.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"5s"`
}

// Validate executes manual validations.
func (c Config) Validate() error {
	err := c.Config.Validate()

	return err
}
