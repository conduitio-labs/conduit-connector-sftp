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
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type fileInfo struct {
	name    string
	modTime time.Time
}

type Iterator struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	position   *Position
	config     config.Config
	files      []fileInfo
	done       chan struct{}
	ch         chan opencdc.Record
}

// NewTableIterator creates a new iterator goroutine and polls redshift for new records.
func NewIterator(
	ctx context.Context,
	sshClient *ssh.Client,
	sftpClient *sftp.Client,
	position *Position,
	config config.Config,
	done chan struct{},
	ch chan opencdc.Record,
) error {
	iter := &Iterator{
		sshClient:  sshClient,
		sftpClient: sftpClient,
		position:   position,
		config:     config,
		done:       done,
		ch:         ch,
	}

	err := iter.loadFiles()
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	go iter.start(ctx)

	return nil
}

// start polls redshift for new records and writes it into the source channel.
func (iter *Iterator) start(ctx context.Context) {
	defer close(iter.done)

	for {
		hasNext, err := iter.hasNext()
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("iterator shutting down...")
			return //nolint:nlreturn // compact code style
		}

		if !hasNext {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("context cancelled, iterator shutting down...")
				return //nolint:nlreturn // compact code style

			case <-time.After(iter.config.PollingPeriod):
				continue
			}
		}

		record, err := iter.next(ctx)
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("iterator shutting down...")
			return //nolint:nlreturn // compact code style
		}

		select {
		case iter.ch <- record:

		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Msg("context cancelled, iterator shutting down...")
			return //nolint:nlreturn // compact code style
		}
	}
}

// hasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) hasNext() (bool, error) {
	if len(iter.files) != 0 {
		return true, nil
	}

	if err := iter.loadFiles(); err != nil {
		return false, fmt.Errorf("load files: %w", err)
	}

	if len(iter.files) != 0 {
		return true, nil
	}

	return false, nil
}

// next returns the next record.
func (iter *Iterator) next(_ context.Context) (opencdc.Record, error) {
	fileInfo := iter.files[0]
	fullPath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	file, err := iter.sftpClient.Open(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("read file: %w", err)
	}

	// Create record metadata
	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: iter.config.DirectoryPath,
		opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
		"filename":                 fileInfo.name,
		"source_path":              fullPath,
		"file_size":                fmt.Sprintf("%d", len(content)),
		"mod_time":                 fileInfo.modTime.Format(time.RFC3339),
	}

	// Create record position
	position := &Position{
		LastProcessedFileTimestamp: fileInfo.modTime,
	}
	positionBytes, err := json.Marshal(position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	// update record position
	iter.position = position

	// remove processed file
	iter.files = iter.files[1:]

	// Create OpenCDC record
	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		opencdc.StructuredData{"filename": fileInfo.name},
		opencdc.RawData(content),
	), nil
}

// loadFiles finds files matching the pattern that haven't been processed.
func (iter *Iterator) loadFiles() error {
	files, err := iter.sftpClient.ReadDir(iter.config.DirectoryPath)
	if err != nil {
		return fmt.Errorf("read directory: %w", err)
	}

	var unprocessedFiles []fileInfo
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		modTime := file.ModTime().UTC()

		// Check file pattern match and modification time
		if matched, _ := filepath.Match(iter.config.FilePattern, filename); matched &&
			modTime.After(iter.position.LastProcessedFileTimestamp) {
			unprocessedFiles = append(unprocessedFiles, fileInfo{
				name:    filename,
				modTime: modTime,
			})
		}
	}

	// Sort unprocessed files by modification time in ascending order
	sort.Slice(unprocessedFiles, func(i, j int) bool {
		return unprocessedFiles[i].modTime.Before(unprocessedFiles[j].modTime)
	})

	iter.files = unprocessedFiles

	return nil
}
