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
	"sync"
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
	ctx := context.Background()

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
	ctx := context.Background()

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
		ch: make(chan opencdc.Record),
		wg: &sync.WaitGroup{},
	}

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}

func TestSource_Teardown_WithPendingGoroutines(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch: make(chan opencdc.Record),
		wg: &sync.WaitGroup{},
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond)
	}()

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}
