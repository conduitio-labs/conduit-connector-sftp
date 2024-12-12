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

//go:generate paramgen -output=paramgen.go Config

package config

import (
	"fmt"
)

// Config contains shared config parameters, common to the source and destination.
type Config struct {
	// Host is the host of the sftp server to connect.
	Host string `json:"host" validate:"required"`
	// Port is the port of the sftp server to connect.
	Port string `json:"port" default:"22"`
	// User is the SFTP user. (required with password and not required when using ssh key)
	Username string `json:"username"`
	// Password is the SFTP password. (required with username and not required when using ssh key)
	Password string `json:"password"`
	// PrivateKeyPath is the private key for ssh login.
	PrivateKeyPath string `json:"private_key_path"`
}

// Validate is used for custom validation for sftp authentication configuration.
func (c *Config) Validate() error {
	if c.Username != "" && c.Password == "" {
		return fmt.Errorf("%w value is required if %w is provided", ConfigPassword, ConfigUsername)
	}

	if c.Password != "" && c.Username == "" {
		return fmt.Errorf("%w value is required if %w is provided", ConfigUsername, ConfigPassword)
	}

	if c.Username == "" && c.Password == "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("please provide %w and %w or %w for sftp authentication", ConfigUsername, ConfigPassword, ConfigPrivateKeyPath)
	}

	return nil
}
