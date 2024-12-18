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
	"errors"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func TestSource_Configure(t *testing.T) {
	t.Parallel()

	t.Run("source configure success", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		err := s.Configure(context.Background(), map[string]string{
			config.ConfigAddress:       "locahost:22",
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)
	})

	t.Run("source configure failure", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		err := s.Configure(context.Background(), map[string]string{
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.True(err != nil)
		is.Equal(err.Error(),
			`invalid config: config invalid: error validating "address": required parameter is not provided`)
	})

	t.Run("source configure fail config validate", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		err := s.Configure(context.Background(), map[string]string{
			config.ConfigAddress:       "locahost:22",
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.True(err != nil)
		is.Equal(err.Error(),
			`error validating configuration: validate config: both "password" and "privateKeyPath" can not be empty`)
	})
}

func TestSource_Open(t *testing.T) {
	t.Parallel()

	hostKey, err := setupHostKey()
	if err != nil {
		fmt.Println(err)
		return
	}

	t.Run("source open success", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("source open error sshConfig", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		ctx := context.Background()

		err := s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       "hostKey",
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to create SSH config: failed to parse host key: ssh: no key found")

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("source open error read private key", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		ctx := context.Background()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:        "localhost:2222",
			config.ConfigHostKey:        hostKey,
			config.ConfigUsername:       "user",
			config.ConfigPrivateKeyPath: "privatekey",
			config.ConfigDirectoryPath:  "/upload",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to create SSH config: failed to read private key file: open privatekey: no such file or directory")

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("source open error ssh dial", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		ctx := context.Background()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:22",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to dial SSH: ssh: handshake failed: ssh: host key mismatch")
	})

	t.Run("source open error remote path not exist", func(t *testing.T) {
		is := is.New(t)

		s := NewSource()

		ctx := context.Background()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.True(err != nil)
		is.Equal(err.Error(), "remote path does not exist: file does not exist")
	})
}

func TestSource_Read(t *testing.T) {
	t.Parallel()

	hostKey, err := setupHostKey()
	if err != nil {
		fmt.Println(err)
		return
	}

	t.Run("source read success", func(t *testing.T) {
		err := writeTestFile("test.txt", "Hello World!")
		if err != nil {
			fmt.Println(err)
			return
		}

		is := is.New(t)
		s := NewSource()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		// wait for a record to be available
		var record opencdc.Record
		for {
			record, err = s.Read(ctx)
			if err == nil {
				break
			}
			if !errors.Is(err, sdk.ErrBackoffRetry) {
				t.Fatalf("Unexpected error: %v", err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("Timeout waiting for record")
			case <-time.After(100 * time.Millisecond):
				// short wait before retrying
				continue
			}
		}
		is.Equal(record.Operation, opencdc.OperationCreate)
		is.Equal(record.Key, opencdc.StructuredData{"filename": "test.txt"})
		is.Equal(record.Payload.After, opencdc.RawData([]byte("Hello World!")))

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("source no new records to read error backoff retry", func(t *testing.T) {
		is := is.New(t)
		s := NewSource()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = s.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		// wait for a record to be available
		var record opencdc.Record
		for {
			record, err = s.Read(ctx)
			if errors.Is(err, sdk.ErrBackoffRetry) {
				break
			}
			if !errors.Is(err, sdk.ErrBackoffRetry) {
				t.Fatalf("Unexpected error: %v", err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("Timeout waiting for record")
			case <-time.After(100 * time.Millisecond):
				// short wait before retrying
				continue
			}
		}
		is.Equal(err, sdk.ErrBackoffRetry)
		is.Equal(record, opencdc.Record{})

		err = s.Teardown(ctx)
		is.NoErr(err)
	})
}

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func setupHostKey() (string, error) {
	cmd := exec.Command("ssh-keyscan", "-t", "rsa", "-p", "2222", "localhost")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error setupHostKey: %w", err)
	}

	return string(output), nil
}

func writeTestFile(name string, content string) error {
	sshConfig := &ssh.ClientConfig{
		User: "user",
		Auth: []ssh.AuthMethod{
			ssh.Password("pass"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshClient, err := ssh.Dial("tcp", "localhost:2222", sshConfig)
	if err != nil {
		return fmt.Errorf("failed to dial SSH: %w", err)
	}
	defer sshClient.Close()

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}
	defer client.Close()

	remoteFilePath := fmt.Sprintf("/upload/%s", name)
	remoteFile, err := client.Create(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer remoteFile.Close()

	_, err = remoteFile.Write([]byte(content))
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}
