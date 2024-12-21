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
	"crypto/md5" //nolint: gosec // MD5 used for non-cryptographic unique identifier
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

var (
	ErrChunkProcessingCancelled         = errors.New("context cancelled while processing chunks")
	ErrLargeFileProcessingUnexpectedEnd = errors.New("unexpected end during large file processing")
)

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

type Iterator struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	position   *Position
	config     config.Config
	files      []fileInfo
	ch         chan opencdc.Record
	wg         *sync.WaitGroup
}

// NewIterator creates a new iterator goroutine and polls SFTP for new records.
func NewIterator(
	ctx context.Context,
	sshClient *ssh.Client,
	sftpClient *sftp.Client,
	position *Position,
	config config.Config,
	ch chan opencdc.Record,
	wg *sync.WaitGroup,
) error {
	iter := &Iterator{
		sshClient:  sshClient,
		sftpClient: sftpClient,
		position:   position,
		config:     config,
		ch:         ch,
		wg:         wg,
	}

	err := iter.loadFiles()
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}

	go iter.start(ctx)

	return nil
}

// start polls sftp for new records and writes it into the source channel.
func (iter *Iterator) start(ctx context.Context) {
	defer iter.wg.Done()

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
func (iter *Iterator) next(ctx context.Context) (opencdc.Record, error) {
	fileInfo := iter.files[0]

	if fileInfo.size >= iter.config.FileChunkSizeBytes {
		return iter.processLargeFile(ctx, fileInfo)
	}
	return iter.processFile(fileInfo)
}

func (iter *Iterator) processFile(fileInfo fileInfo) (opencdc.Record, error) {
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

	metadata := iter.createMetadata(fileInfo, fullPath, len(content))
	position := &Position{LastProcessedFileTimestamp: fileInfo.modTime}

	positionBytes, err := json.Marshal(position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	iter.position = position
	iter.files = iter.files[1:]

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		opencdc.StructuredData{"filename": fileInfo.name},
		opencdc.RawData(content),
	), nil
}

func (iter *Iterator) processLargeFile(ctx context.Context, fileInfo fileInfo) (opencdc.Record, error) {
	fullPath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	file, err := iter.sftpClient.Open(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	totalChunks := int(math.Ceil(float64(fileInfo.size) / float64(iter.config.FileChunkSizeBytes)))

	startChunkIndex := 1
	if iter.position.ChunkInfo != nil && iter.position.ChunkInfo.Filename == fileInfo.name {
		startChunkIndex = iter.position.ChunkInfo.ChunkIndex
	}

	return iter.processChunks(ctx, file, fileInfo, startChunkIndex, totalChunks)
}

func (iter *Iterator) processChunks(ctx context.Context, file *sftp.File, fileInfo fileInfo, startChunkIndex, totalChunks int) (opencdc.Record, error) {
	fullPath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	for chunkIndex := startChunkIndex; chunkIndex <= totalChunks; chunkIndex++ {
		chunk, err := iter.readChunk(file, fileInfo.size, chunkIndex)
		if err != nil {
			return opencdc.Record{}, err
		}

		position := &Position{
			LastProcessedFileTimestamp: fileInfo.modTime,
			ChunkInfo: &ChunkInfo{
				Filename:    fileInfo.name,
				ChunkIndex:  chunkIndex,
				TotalChunks: totalChunks,
				ModTime:     fileInfo.modTime.Format(time.RFC3339),
			},
		}

		positionBytes, err := json.Marshal(position)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
		}

		iter.position = position

		// Add chunk information to metadata
		metadata := iter.createMetadata(fileInfo, fullPath, len(chunk))
		metadata["chunk_index"] = fmt.Sprintf("%d", chunkIndex)
		metadata["total_chunks"] = fmt.Sprintf("%d", totalChunks)
		metadata["hash"] = hash(fileInfo.modTime.Format(time.RFC3339))
		metadata["is_chunked"] = "true"

		record := sdk.Util.Source.NewRecordCreate(
			positionBytes,
			metadata,
			opencdc.StructuredData{"filename": fileInfo.name},
			opencdc.RawData(chunk),
		)

		// If this isn't the last chunk, send directly to channel
		if chunkIndex < totalChunks {
			select {
			case iter.ch <- record:
			case <-ctx.Done():
				return opencdc.Record{}, ErrChunkProcessingCancelled
			}
		} else {
			// For the last chunk, move to next file and return the record
			iter.files = iter.files[1:]
			iter.position.ChunkInfo = nil
			return record, nil
		}
	}

	// This should never be reached as we always return in the loop
	return opencdc.Record{}, ErrLargeFileProcessingUnexpectedEnd
}

func (iter *Iterator) readChunk(file *sftp.File, fileSize int64, chunkIndex int) ([]byte, error) {
	offset := int64(chunkIndex-1) * iter.config.FileChunkSizeBytes
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek file: %w", err)
	}

	chunkSize := int(math.Min(float64(iter.config.FileChunkSizeBytes), float64(fileSize-offset)))
	chunk := make([]byte, chunkSize)
	_, err = io.ReadFull(file, chunk)
	if err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	return chunk, nil
}

func (iter *Iterator) createMetadata(fileInfo fileInfo, fullPath string, contentLength int) opencdc.Metadata {
	return opencdc.Metadata{
		opencdc.MetadataCollection: iter.config.DirectoryPath,
		opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
		"filename":                 fileInfo.name,
		"source_path":              fullPath,
		"file_size":                fmt.Sprintf("%d", contentLength),
		"mod_time":                 fileInfo.modTime.Format(time.RFC3339),
	}
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
				size:    file.Size(),
				modTime: modTime,
			})
		}
	}

	// Sort unprocessed files by modification time to maintain order
	sort.Slice(unprocessedFiles, func(i, j int) bool {
		return unprocessedFiles[i].modTime.Before(unprocessedFiles[j].modTime)
	})

	iter.files = unprocessedFiles

	return nil
}

// hash creates a unique identifier for a file based on its modification time.
func hash(modTime string) string {
	h := md5.New() //nolint: gosec // MD5 used for non-cryptographic unique identifier
	h.Write([]byte(modTime))
	return hex.EncodeToString(h.Sum(nil))
}
