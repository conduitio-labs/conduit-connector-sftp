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
	"os"
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
	ErrFileModifiedDuringRead           = errors.New("file was modified during read")
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
			if errors.Is(err, ErrFileModifiedDuringRead) {
				continue
			}
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
	file := iter.files[0]

	if file.size >= iter.config.FileChunkSizeBytes {
		return iter.processLargeFile(ctx, file.name)
	}
	return iter.processFile(ctx, file.name)
}

func (iter *Iterator) processFile(ctx context.Context, filename string) (opencdc.Record, error) {
	fullPath := filepath.Join(iter.config.DirectoryPath, filename)

	// Get initial file stat.
	initialStat, err := iter.sftpClient.Stat(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("stat file: %w", err)
	}

	file, err := iter.sftpClient.Open(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("read file: %w", err)
	}

	if err := iter.validateFile(ctx, initialStat, fullPath); err != nil {
		return opencdc.Record{}, err
	}

	metadata := iter.createMetadata(initialStat, fullPath, len(content))
	position := &Position{LastProcessedFileTimestamp: initialStat.ModTime().UTC()}

	positionBytes, err := json.Marshal(position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	iter.position = position
	iter.files = iter.files[1:]

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		opencdc.StructuredData{"filename": filename},
		opencdc.RawData(content),
	), nil
}

func (iter *Iterator) processLargeFile(ctx context.Context, filename string) (opencdc.Record, error) {
	fullPath := filepath.Join(iter.config.DirectoryPath, filename)

	// Get initial file stat.
	initialStat, err := iter.sftpClient.Stat(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("stat file: %w", err)
	}

	file, err := iter.sftpClient.Open(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	totalChunks := int(math.Ceil(float64(initialStat.Size()) / float64(iter.config.FileChunkSizeBytes)))

	startChunkIndex := 1
	if iter.position.ChunkInfo != nil && iter.position.ChunkInfo.Filename == filename &&
		iter.position.ChunkInfo.ModTime == initialStat.ModTime().UTC().Format(time.RFC3339) {
		startChunkIndex = iter.position.ChunkInfo.ChunkIndex
	}

	return iter.processChunks(ctx, file, initialStat, startChunkIndex, totalChunks)
}

func (iter *Iterator) processChunks(ctx context.Context, file *sftp.File, fileInfo os.FileInfo, startChunkIndex, totalChunks int) (opencdc.Record, error) {
	fullPath := filepath.Join(iter.config.DirectoryPath, fileInfo.Name())

	for chunkIndex := startChunkIndex; chunkIndex <= totalChunks; chunkIndex++ {
		chunk, err := iter.readChunk(file, fileInfo.Size(), chunkIndex)
		if err != nil {
			return opencdc.Record{}, err
		}

		if err := iter.validateFile(ctx, fileInfo, fullPath); err != nil {
			return opencdc.Record{}, err
		}

		position := &Position{
			LastProcessedFileTimestamp: fileInfo.ModTime().UTC(),
			ChunkInfo: &ChunkInfo{
				Filename:    fileInfo.Name(),
				ChunkIndex:  chunkIndex,
				TotalChunks: totalChunks,
				ModTime:     fileInfo.ModTime().UTC().Format(time.RFC3339),
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
		metadata["hash"] = generateFileHash(fileInfo.Name(), fileInfo.ModTime().UTC(), fileInfo.Size())
		metadata["is_chunked"] = "true"

		record := sdk.Util.Source.NewRecordCreate(
			positionBytes,
			metadata,
			opencdc.StructuredData{"filename": fileInfo.Name()},
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

func (iter *Iterator) createMetadata(fileInfo os.FileInfo, fullPath string, contentLength int) opencdc.Metadata {
	return opencdc.Metadata{
		opencdc.MetadataCollection: iter.config.DirectoryPath,
		opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
		"filename":                 fileInfo.Name(),
		"source_path":              fullPath,
		"file_size":                fmt.Sprintf("%d", contentLength),
		"mod_time":                 fileInfo.ModTime().UTC().Format(time.RFC3339),
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

		fileName := file.Name()
		modTime := file.ModTime().UTC()

		// Check file pattern match and modification time
		if matched, _ := filepath.Match(iter.config.FilePattern, fileName); matched &&
			modTime.After(iter.position.LastProcessedFileTimestamp) {
			unprocessedFiles = append(unprocessedFiles, fileInfo{
				name:    fileName,
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

// validateFile ensures file integrity during read.
func (iter *Iterator) validateFile(ctx context.Context, fileInfo os.FileInfo, fullPath string) error {
	finalStat, err := iter.sftpClient.Stat(fullPath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	if !fileInfo.ModTime().Equal(finalStat.ModTime()) {
		sdk.Logger(ctx).Info().Msgf(`file "%s" modified during read`, fileInfo.Name())
		// Move modified file to the end of the queue to retry later
		iter.files = append(iter.files[1:], iter.files[0])
		return ErrFileModifiedDuringRead
	}

	return nil
}

// generateFileHash creates a unique hash based on file name, mod time, and size.
func generateFileHash(fileName string, modTime time.Time, fileSize int64) string {
	data := fmt.Sprintf("%s|%s|%d", fileName, modTime.Format(time.RFC3339), fileSize)
	hash := md5.Sum([]byte(data)) //nolint: gosec // MD5 used for non-cryptographic unique identifier
	return hex.EncodeToString(hash[:])
}
