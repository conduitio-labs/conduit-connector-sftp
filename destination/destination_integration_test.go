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

package destination

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	t.Run("destination configure success", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		err := d.Configure(context.Background(), map[string]string{
			config.ConfigAddress:       "locahost:22",
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)
	})

	t.Run("destination configure failure", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		err := d.Configure(context.Background(), map[string]string{
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.True(err != nil)
		is.Equal(err.Error(),
			`invalid config: config invalid: error validating "address": required parameter is not provided`)
	})

	t.Run("destination configure fail config validate", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		err := d.Configure(context.Background(), map[string]string{
			config.ConfigAddress:       "locahost:22",
			config.ConfigHostKey:       "host-key",
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.True(err != nil)
		is.Equal(err.Error(),
			`error validating configuration: both "password" and "privateKeyPath" can not be empty`)
	})
}

func TestDestination_Open(t *testing.T) {
	t.Parallel()

	hostKey, err := setupHostKey()
	if err != nil {
		fmt.Println(err)
		return
	}

	t.Run("destination open success", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		d.Teardown(ctx)
	})

	t.Run("destination open error sshConfig", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       "hostKey",
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to create SSH config: failed to parse host key: ssh: no key found")

		d.Teardown(ctx)
	})

	t.Run("destination open error read private key", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:        "localhost:2222",
			config.ConfigHostKey:        hostKey,
			config.ConfigUsername:       "user",
			config.ConfigPrivateKeyPath: "privatekey",
			config.ConfigDirectoryPath:  "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to create SSH config: failed to read private key file: open privatekey: no such file or directory")

		d.Teardown(ctx)
	})

	t.Run("destination open error ssh dial", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:22",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "root",
			config.ConfigPassword:      "root",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), "failed to dial SSH: ssh: handshake failed: host key type mismatch: got ecdsa-sha2-nistp256, want ssh-rsa")
	})

	t.Run("destination open error remote path not exist", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/home/root",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), "remote path does not exist: file does not exist")
	})
}

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	hostKey, err := setupHostKey()
	if err != nil {
		fmt.Println(err)
		return
	}

	t.Run("destination write success", func(t *testing.T) {
		is := is.New(t)
		d := NewDestination()
		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		content := []byte(`Hello World!`)

		records := []opencdc.Record{
			sdk.Util.Source.NewRecordCreate(
				nil,
				map[string]string{
					opencdc.MetadataCollection: "upload",
					opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
					"filename":                 "example.txt",
					"source_path":              "/upload",
					"hash":                     "55fa9e9cb76faa2e544668384538b19a",
					"file_size":                fmt.Sprintf("%d", len(content)),
					"mod_time":                 time.Now().UTC().Format(time.RFC3339),
				},
				opencdc.StructuredData{"filename": "example.txt"},
				opencdc.RawData(content),
			),
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})

	t.Run("destination write success filename from key", func(t *testing.T) {
		is := is.New(t)
		d := NewDestination()
		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		content := []byte(`Hello World!`)

		records := []opencdc.Record{
			sdk.Util.Source.NewRecordCreate(
				nil,
				map[string]string{
					opencdc.MetadataCollection: "upload",
					opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
					"source_path":              "/upload",
					"hash":                     "55fa9e9cb76faa2e544668384538b19a",
					"file_size":                fmt.Sprintf("%d", len(content)),
					"mod_time":                 time.Now().UTC().Format(time.RFC3339),
				},
				opencdc.StructuredData{"filename": "example.txt"},
				opencdc.RawData(content),
			),
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})

	t.Run("destination write failure", func(t *testing.T) {
		is := is.New(t)
		d := NewDestination()
		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		content := []byte(`Hello World!`)

		records := []opencdc.Record{
			sdk.Util.Source.NewRecordCreate(
				nil,
				map[string]string{
					opencdc.MetadataCollection: "upload",
					opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
					"filename":                 "",
					"source_path":              "/upload",
					"hash":                     "55fa9e9cb76faa2e544668384538b19a",
					"file_size":                fmt.Sprintf("%d", len(content)),
					"mod_time":                 time.Now().UTC().Format(time.RFC3339),
				},
				opencdc.StructuredData{"filename": ""},
				opencdc.RawData(content),
			),
		}

		_, err = d.Write(ctx, records)
		is.True(err != nil)
		is.Equal(err.Error(), `failed to create remote file: sftp: "Failure" (SSH_FX_FAILURE)`)

		d.Teardown(ctx)
	})

	t.Run("destination large file upload", func(t *testing.T) {
		is := is.New(t)
		d := NewDestination()
		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			config.ConfigAddress:       "localhost:2222",
			config.ConfigHostKey:       hostKey,
			config.ConfigUsername:      "user",
			config.ConfigPassword:      "pass",
			config.ConfigDirectoryPath: "/upload",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		records := []opencdc.Record{
			sdk.Util.Source.NewRecordCreate(
				nil,
				map[string]string{
					opencdc.MetadataCollection: "upload",
					opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
					"filename":                 "largefile.txt",
					"chunk_index":              "1",
					"total_chunks":             "2",
					"is_chunked":               "true",
					"hash":                     "55fa9e9cb76faa2e544668384538b19a",
					"file_size":                "26",
					"mod_time":                 time.Now().UTC().Format(time.RFC3339),
				},
				opencdc.StructuredData{"filename": "largefile.txt"},
				opencdc.RawData([]byte(`Hello World!1`)),
			),
			sdk.Util.Source.NewRecordCreate(
				nil,
				map[string]string{
					opencdc.MetadataCollection: "upload",
					opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
					"filename":                 "largefile.txt",
					"chunk_index":              "2",
					"total_chunks":             "2",
					"is_chunked":               "true",
					"hash":                     "55fa9e9cb76faa2e544668384538b19a",
					"file_size":                "26",
					"mod_time":                 time.Now().UTC().Format(time.RFC3339),
				},
				opencdc.StructuredData{"filename": "largefile.txt"},
				opencdc.RawData([]byte(`Hello World!2`)),
			),
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})
}

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
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
