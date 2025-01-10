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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
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
			config.ConfigDirectoryPath: "/source",
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
			config.ConfigDirectoryPath: "/source",
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
			config.ConfigDirectoryPath:  "/source",
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
		is.Equal(err.Error(), "failed to dial SSH: ssh: handshake failed: host key type mismatch: got ecdsa-sha2-nistp256, want ssh-rsa")
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
		is.Equal(err.Error(), `remote path "/home/root" does not exist: file does not exist`)
	})
}

func TestSource_Read(t *testing.T) {
	hostKey, err := setupHostKey()
	if err != nil {
		t.Fatalf("failed to setup host key: %v", err)
	}

	configuration := map[string]string{
		"address":            "localhost:2222",
		"hostKey":            hostKey,
		"username":           "user",
		"password":           "pass",
		"directoryPath":      "/source",
		"filePattern":        "*",
		"fileChunkSizeBytes": "1024",
	}

	t.Run("success reading new file", func(t *testing.T) {
		is := is.New(t)

		_, err = writeTestFile("test.txt", "Hello World!")
		if err != nil {
			t.Fatal(err)
		}

		s := &Source{}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = s.Configure(ctx, configuration)
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		record, err := s.Read(ctx)
		is.NoErr(err)

		// Verify record contents
		is.Equal(record.Operation, opencdc.OperationCreate)
		is.Equal(record.Metadata["filename"], "test.txt")
		is.Equal(string(record.Payload.After.(opencdc.RawData)), "Hello World!")

		// Verify position was updated
		pos, err := ParseSDKPosition(record.Position)
		is.NoErr(err)
		is.True(pos.LastProcessedFileTimestamp.After(time.Time{}))

		err = s.Teardown(ctx)
		is.NoErr(err)

		err = removeTestFile("test.txt")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("no new files available", func(t *testing.T) {
		is := is.New(t)
		s := &Source{}
		ctx := context.Background()

		err = s.Configure(ctx, configuration)
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		record, err := s.Read(ctx)
		is.True(errors.Is(err, sdk.ErrBackoffRetry))
		is.Equal(record, opencdc.Record{})

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("file pattern filtering", func(t *testing.T) {
		is := is.New(t)
		s := &Source{}
		ctx := context.Background()

		// Create additional test files with different extensions
		_, err = writeTestFile("test.csv", "1,2,3")
		is.NoErr(err)
		_, err = writeTestFile("test.json", `{"key": "value"}`)
		is.NoErr(err)

		err = s.Configure(ctx, map[string]string{
			"address":            "localhost:2222",
			"hostKey":            hostKey,
			"username":           "user",
			"password":           "pass",
			"directoryPath":      "/source",
			"filePattern":        "*.csv",
			"fileChunkSizeBytes": "1024",
		})
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		record, err := s.Read(ctx)
		is.NoErr(err)
		is.Equal(record.Metadata["filename"], "test.csv")

		err = s.Teardown(ctx)
		is.NoErr(err)
	})
}

func TestSource_ReadLargeFile(t *testing.T) {
	hostKey, err := setupHostKey()
	if err != nil {
		t.Fatalf("failed to setup host key: %v", err)
	}

	// Setup a large test file that will be chunked
	largeContent := strings.Repeat("Large file content\n", 1000)
	stat, err := writeTestFile("large.txt", largeContent)
	if err != nil {
		t.Fatal(err)
	}
	defer removeTestFile("large.txt")

	configuration := map[string]string{
		"address":            "localhost:2222",
		"hostKey":            hostKey,
		"username":           "user",
		"password":           "pass",
		"directoryPath":      "/source",
		"filePattern":        "*.txt",
		"fileChunkSizeBytes": "1024",
	}

	t.Run("success reading large file in chunks", func(t *testing.T) {
		is := is.New(t)
		s := &Source{}
		ctx := context.Background()

		err = s.Configure(ctx, configuration)
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		// Read first chunk
		record1, err := s.Read(ctx)
		is.NoErr(err)

		// Verify first chunk metadata
		is.Equal(record1.Metadata["filename"], "large.txt")
		is.Equal(record1.Metadata["is_chunked"], "true")
		is.Equal(record1.Metadata["chunk_index"], "1")
		expectedTotalChunks := len(largeContent) / 1024
		if len(largeContent)%1024 != 0 {
			expectedTotalChunks++
		}
		is.Equal(record1.Metadata["total_chunks"], fmt.Sprintf("%d", expectedTotalChunks))

		// Read and verify middle chunk
		record2, err := s.Read(ctx)
		is.NoErr(err)
		is.Equal(record2.Metadata["chunk_index"], "2")

		// Read until last chunk
		var lastRecord opencdc.Record
		for {
			record, err := s.Read(ctx)
			if errors.Is(err, sdk.ErrBackoffRetry) {
				break
			}
			is.NoErr(err)
			lastRecord = record
		}

		is.Equal(lastRecord.Metadata["chunk_index"], lastRecord.Metadata["total_chunks"])

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("resume from middle chunk", func(t *testing.T) {
		is := is.New(t)
		s := &Source{}
		ctx := context.Background()

		err = s.Configure(ctx, configuration)
		is.NoErr(err)

		pos := &Position{
			LastProcessedFileTimestamp: stat.ModTime().UTC(),
			ChunkInfo: &ChunkInfo{
				Filename:    "large.txt",
				ChunkIndex:  2,
				ModTime:     stat.ModTime().UTC().Format(time.RFC3339),
				TotalChunks: len(largeContent)/1024 + 1,
			},
		}
		posBytes, err := json.Marshal(pos)
		is.NoErr(err)

		err = s.Open(ctx, opencdc.Position(posBytes))
		is.NoErr(err)

		record, err := s.Read(ctx)
		is.NoErr(err)
		is.Equal(record.Metadata["chunk_index"], "3")

		err = s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("file modified during chunking", func(t *testing.T) {
		is := is.New(t)
		s := &Source{}
		ctx := context.Background()

		err = s.Configure(ctx, configuration)
		is.NoErr(err)

		err = s.Open(ctx, nil)
		is.NoErr(err)

		record1, err := s.Read(ctx)
		is.NoErr(err)
		is.Equal(record1.Metadata["chunk_index"], "1")

		// Modify file between chunks
		newContent := strings.Repeat("New file content\n", 1000)
		_, err = writeTestFile("large.txt", newContent)
		is.NoErr(err)

		// Next read should detect modification and start over
		record2, err := s.Read(ctx)
		is.NoErr(err)
		// Should start reading the modified file from beginning
		is.Equal(record2.Metadata["chunk_index"], "1")

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

func writeTestFile(name string, content string) (os.FileInfo, error) {
	sshConfig := &ssh.ClientConfig{
		User: "user",
		Auth: []ssh.AuthMethod{
			ssh.Password("pass"),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshClient, err := ssh.Dial("tcp", "localhost:2222", sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial SSH: %w", err)
	}
	defer sshClient.Close()

	client, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create SFTP client: %w", err)
	}
	defer client.Close()

	remoteFilePath := fmt.Sprintf("/source/%s", name)
	remoteFile, err := client.Create(remoteFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer remoteFile.Close()

	stat, err := remoteFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	_, err = remoteFile.Write([]byte(content))
	if err != nil {
		return nil, fmt.Errorf("failed to write to file: %w", err)
	}

	return stat, nil
}

func removeTestFile(name string) error {
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

	remoteFilePath := fmt.Sprintf("/source/%s", name)
	err = client.Remove(remoteFilePath)
	if err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}
