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
	"testing"

	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

func TestNewSource(t *testing.T) {
	is := is.New(t)

	con := NewSource()
	is.True(con != nil)
	is.True(con.Parameters() != nil)
}

func TestSource_Configure_allFieldsSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := t.Context()

	s := Source{}

	// Test valid configuration using private key
	err := s.Configure(ctx, commonsConfig.Config{
		"address":        "sftp.example.com:22",
		"username":       "testuser",
		"privateKeyPath": "/path/to/privatekey",
		"hostKey":        "ssh-rsa AAAAB3NzaC1...",
		"directoryPath":  "/path/to/directory",
	})
	is.NoErr(err)

	// Test valid configuration using password
	err = s.Configure(ctx, commonsConfig.Config{
		"address":       "sftp.example.com:22",
		"username":      "testuser",
		"password":      "testpass",
		"hostKey":       "ssh-rsa AAAAB3NzaC1...",
		"directoryPath": "/path/to/directory",
	})
	is.NoErr(err)
}

func TestSource_Configure_missingAuthentication(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := t.Context()

	s := Source{}

	err := s.Configure(ctx, commonsConfig.Config{
		"address":       "sftp.example.com:22",
		"username":      "testuser",
		"hostKey":       "ssh-rsa AAAAB3NzaC1...",
		"directoryPath": "/path/to/directory",
	})
	is.True(err != nil)
	is.Equal(err.Error(), `error validating configuration: validate config: both "password" and "privateKeyPath" can not be empty`)
}

func TestSource_Configure_missingHostKey(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := t.Context()

	s := Source{}

	err := s.Configure(ctx, commonsConfig.Config{
		"address":       "sftp.example.com:22",
		"username":      "testuser",
		"password":      "testpass",
		"directoryPath": "/path/to/directory",
	})
	is.True(err != nil)
	is.Equal(err.Error(), `invalid config: config invalid: error validating "hostKey": required parameter is not provided`)
}

func TestSource_Teardown_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := t.Context()

	s := &Source{}

	err := s.Teardown(ctx)
	is.NoErr(err)
}
