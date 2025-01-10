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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-sftp/common"
	"github.com/conduitio-labs/conduit-connector-sftp/config"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Destination struct {
	sdk.UnimplementedDestination

	config     config.Config
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware(
		sdk.DestinationWithSchemaExtractionConfig{
			PayloadEnabled: lang.Ptr(false),
			KeyEnabled:     lang.Ptr(false),
		},
	)...)
}

func (d *Destination) Parameters() commonsConfig.Parameters {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg commonsConfig.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	err = d.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening a SFTP Destination...")
	sshConfig, err := common.SSHConfigAuth(d.config.HostKey, d.config.Username, d.config.Password, d.config.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("failed to create SSH config: %w", err)
	}

	d.sshClient, err = ssh.Dial("tcp", d.config.Address, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to dial SSH: %w", err)
	}

	d.sftpClient, err = sftp.NewClient(d.sshClient)
	if err != nil {
		d.sshClient.Close()
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}

	_, err = d.sftpClient.Stat(d.config.DirectoryPath)
	if err != nil {
		return fmt.Errorf("remote path does not exist: %w", err)
	}

	return nil
}

func (d *Destination) Write(_ context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		chunked, ok := record.Metadata["is_chunked"]
		if ok && chunked == "true" {
			err := d.handleChunkedRecord(record)
			if err != nil {
				return i, err
			}
			continue
		}

		err := d.uploadFile(record)
		if err != nil {
			return i, err
		}
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the SFTP Destination")

	var errs []error
	if d.sftpClient != nil {
		if err := d.sftpClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close SFTP client: %w", err))
		}
	}

	if d.sshClient != nil {
		if err := d.sshClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close SSH client: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("error teardown: %w", errors.Join(errs...))
	}

	return nil
}

func (d *Destination) handleChunkedRecord(record opencdc.Record) error {
	metaData, err := d.extractMetadata(record)
	if err != nil {
		return err
	}

	var remoteFile *sftp.File
	path := fmt.Sprintf("%s/%s.tmp", d.config.DirectoryPath, metaData.hash)
	if metaData.index == 1 {
		remoteFile, err = d.sftpClient.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create remote file: %w", err)
		}
	} else {
		remoteFile, err = d.sftpClient.OpenFile(path, os.O_WRONLY|os.O_APPEND)
		if err != nil {
			return fmt.Errorf("failed to open remote file: %w", err)
		}
	}

	reader := bytes.NewReader(record.Payload.After.Bytes())
	_, err = reader.WriteTo(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to write content to remote file: %w", err)
	}
	remoteFile.Close()

	if metaData.index == metaData.totalChunks {
		// compare the uploaded filesize with the source filesize to confirm successful upload
		err = d.compareFileSize(path, metaData.filesize)
		if err != nil {
			return err
		}

		err = d.renameFile(path, fmt.Sprintf("%s/%s", d.config.DirectoryPath, metaData.filename))
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) uploadFile(record opencdc.Record) error {
	metaData, err := d.extractMetadata(record)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%s", d.config.DirectoryPath, metaData.filename)
	remoteFile, err := d.sftpClient.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create remote file: %w", err)
	}
	defer remoteFile.Close()

	reader := bytes.NewReader(record.Payload.After.Bytes())
	_, err = reader.WriteTo(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to write content to remote file: %w", err)
	}

	// compare the uploaded filesize with the source filesize to confirm successful upload
	err = d.compareFileSize(path, metaData.filesize)
	if err != nil {
		return err
	}

	return nil
}

func (d *Destination) structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return opencdc.StructuredData{}, nil
	}

	structuredData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

func (d *Destination) compareFileSize(path string, size int64) error {
	stat, err := d.sftpClient.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat remote file: %w", err)
	}

	if stat.Size() != size {
		return NewInvalidFileError(fmt.Sprintf("uploaded filesize(%v) is different than source filesize(%v)", stat.Size(), size))
	}

	return nil
}

type metadata struct {
	index       int64
	totalChunks int64
	hash        string
	filename    string
	filesize    int64
}

func (d *Destination) extractMetadata(record opencdc.Record) (metadata, error) {
	var index, total int64
	chunked, ok := record.Metadata["is_chunked"]
	if ok && chunked == "true" {
		chunkIndex, ok := record.Metadata["chunk_index"]
		if !ok {
			return metadata{}, NewInvalidChunkError("chunk_index not found")
		}

		var err error
		index, err = strconv.ParseInt(chunkIndex, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse chunk_index: %w", err)
		}

		totalChunks, ok := record.Metadata["total_chunks"]
		if !ok {
			return metadata{}, NewInvalidChunkError("total_chunk not found")
		}

		total, err = strconv.ParseInt(totalChunks, 10, 64)
		if err != nil {
			return metadata{}, fmt.Errorf("failed to parse total_chunks: %w", err)
		}
	}

	hash, ok := record.Metadata["hash"]
	if !ok {
		return metadata{}, NewInvalidChunkError("hash not found")
	}

	filename, ok := record.Metadata["filename"]
	if !ok {
		structuredKey, err := d.structurizeData(record.Key)
		if err != nil {
			return metadata{}, err
		}
		name, ok := structuredKey["filename"].(string)
		if !ok {
			return metadata{}, NewInvalidChunkError("invalid filename")
		}
		filename = name
	}

	fileSize, ok := record.Metadata["file_size"]
	if !ok {
		return metadata{}, NewInvalidChunkError("file_size not found")
	}

	size, err := strconv.ParseInt(fileSize, 10, 64)
	if err != nil {
		return metadata{}, fmt.Errorf("failed to parse file_size: %w", err)
	}

	return metadata{
		index:       index,
		totalChunks: total,
		hash:        hash,
		filename:    filename,
		filesize:    size,
	}, nil
}

func (d *Destination) renameFile(path, newPath string) error {
	// check if file already exists then remove it before renaming
	_, err := d.sftpClient.Stat(newPath)
	if err == nil {
		err = d.sftpClient.Remove(newPath)
		if err != nil {
			return fmt.Errorf("failed to remove remote file: %w", err)
		}
	}

	err = d.sftpClient.Rename(path, newPath)
	if err != nil {
		return fmt.Errorf("failed to rename remote file: %w", err)
	}

	return nil
}
