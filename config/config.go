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

// Config contains shared config parameters, common to the source and destination.
type Config struct {
	// Host is the host of the sftp server to connect.
	Host string `json:"host" validate:"required"`
	// Port is the port of the sftp server to connect.
	Port string `json:"port" default:"22"`
	// User is the SFTP user.
	User string `json:"user"`
	// Password is the SFTP password.
	Password string `json:"password"`
}