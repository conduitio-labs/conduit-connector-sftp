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
	"context"
	"testing"
	"time"

	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
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
	ctx := context.Background()

	s := Source{}

	// Test valid configuration using private key
	err := s.Configure(ctx, commonsConfig.Config{
		"address":         "sftp.example.com:22",
		"username":        "testuser",
		"privateKeyPath":  "/path/to/privatekey",
		"serverPublicKey": "ssh-rsa AAAAB3NzaC1...",
		"directory":       "/path/to/directory",
	})
	is.NoErr(err)

	// Test valid configuration using password
	err = s.Configure(ctx, commonsConfig.Config{
		"address":         "sftp.example.com:22",
		"username":        "testuser",
		"password":        "testpass",
		"serverPublicKey": "ssh-rsa AAAAB3NzaC1...",
		"directory":       "/path/to/directory",
	})
	is.NoErr(err)
}

func TestSource_Configure_missingAuthentication(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := Source{}

	err := s.Configure(ctx, commonsConfig.Config{
		"address":         "sftp.example.com:22",
		"username":        "testuser",
		"serverPublicKey": "ssh-rsa AAAAB3NzaC1...",
		"directory":       "/path/to/directory",
	})
	is.True(err != nil)
	is.Equal(err.Error(), `error validating configuration: either "password" or "privateKeyPath" must be provided for sftp authentication`)
}

func TestSource_Configure_missingServerPublicKey(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := Source{}

	err := s.Configure(ctx, commonsConfig.Config{
		"address":   "sftp.example.com:22",
		"username":  "testuser",
		"password":  "testpass",
		"directory": "/path/to/directory",
	})
	is.True(err != nil)
	is.Equal(err.Error(), `config invalid: error validating "serverPublicKey": required parameter is not provided`)
}

func TestSource_Read_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	st := opencdc.StructuredData{
		"key": "value",
	}
	expectedRecord := opencdc.Record{
		Position: opencdc.Position(`"lastProcessedFileTimestamp": testTimestamp`),
		Metadata: nil,
		Key:      st,
		Payload:  opencdc.Change{After: st},
	}

	s := &Source{
		ch: make(chan opencdc.Record, 1),
	}
	s.ch <- expectedRecord

	record, err := s.Read(ctx)
	is.NoErr(err)
	is.Equal(record, expectedRecord)
}

func TestSource_Read_SourceNotInitialized(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	var s *Source
	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "source not opened for reading")

	s = &Source{}
	_, err = s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "source not opened for reading")
}

func TestSource_Read_ClosedChannel(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch: make(chan opencdc.Record),
	}
	close(s.ch)

	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "error reading data, records channel closed unexpectedly")
}

func TestSource_Teardown_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch:   make(chan opencdc.Record),
		done: make(chan struct{}),
	}

	// Simulate iterator finishing
	close(s.done)

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}

func TestSource_Teardown_WithPendingIterator(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch:   make(chan opencdc.Record),
		done: make(chan struct{}),
	}

	go func() {
		close(s.done)
		time.Sleep(100 * time.Millisecond)
	}()

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}
