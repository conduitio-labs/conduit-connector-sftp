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
	"context" //nolint: gosec // MD5 used for non-cryptographic unique identifier
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/common"
	"github.com/conduitio-labs/conduit-connector-sftp/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
)

var ErrFileModifiedDuringRead = errors.New("file was modified during read")

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

type Iterator struct {
	sftpClient *sftp.Client
	position   *Position
	config     config.Config
	files      []fileInfo
}

func NewIterator(sftpClient *sftp.Client, position *Position, config config.Config) *Iterator {
	return &Iterator{
		sftpClient: sftpClient,
		position:   position,
		config:     config,
	}
}

// hasNext indicates whether the source has the next record to return or not.
func (iter *Iterator) hasNext() (bool, error) {
	if len(iter.files) != 0 {
		return true, nil
	}

	if err := iter.loadFiles(); err != nil {
		return false, fmt.Errorf("load files: %w", err)
	}

	return len(iter.files) != 0, nil
}

// Next returns the next record.
func (iter *Iterator) Next(ctx context.Context) (opencdc.Record, error) {
	if err := ctx.Err(); err != nil {
		return opencdc.Record{}, err
	}

	hasNext, err := iter.hasNext()
	if err != nil {
		return opencdc.Record{}, err
	}
	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	file := iter.files[0]
	return iter.processFile(ctx, file)
}

type fileReadResult struct {
	content     []byte
	metadata    opencdc.Metadata
	position    *Position
	chunkIndex  int
	totalChunks int
}

func (iter *Iterator) processFile(ctx context.Context, fileInfo fileInfo) (opencdc.Record, error) {
	filePath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	// Handle initial file validation and opening
	initialStat, file, err := iter.prepareFileRead(filePath, fileInfo)
	if err != nil {
		if errors.Is(err, ErrFileModifiedDuringRead) || errors.Is(err, fs.ErrNotExist) {
			return iter.handleFileError(ctx, err)
		}
		return opencdc.Record{}, err
	}
	defer file.Close()

	if err := ctx.Err(); err != nil {
		return opencdc.Record{}, err
	}

	isLargeFile := fileInfo.size >= iter.config.FileChunkSizeBytes
	result, err := iter.readFileContent(ctx, file, initialStat, fileInfo, isLargeFile)
	if err != nil {
		if errors.Is(err, ErrFileModifiedDuringRead) || errors.Is(err, fs.ErrNotExist) {
			return iter.handleFileError(ctx, err)
		}
		return opencdc.Record{}, err
	}

	// Update iterator state
	iter.position = result.position
	if !isLargeFile || result.chunkIndex == result.totalChunks {
		iter.files = iter.files[1:]
		iter.position.ChunkInfo = nil
	}

	positionBytes, err := json.Marshal(result.position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		result.metadata,
		opencdc.StructuredData{"filename": fileInfo.name},
		opencdc.RawData(result.content),
	), nil
}

func (iter *Iterator) prepareFileRead(filePath string, fileInfo fileInfo) (os.FileInfo, *sftp.File, error) {
	initialStat, err := iter.sftpClient.Stat(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("stat file: %w", err)
	}

	if !initialStat.ModTime().UTC().Equal(fileInfo.modTime) {
		return nil, nil, ErrFileModifiedDuringRead
	}

	file, err := iter.sftpClient.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open file: %w", err)
	}

	return initialStat, file, nil
}

func (iter *Iterator) readFileContent(ctx context.Context, file *sftp.File, stat os.FileInfo, fileInfo fileInfo, isLargeFile bool) (fileReadResult, error) {
	result := fileReadResult{
		position: &Position{LastProcessedFileTimestamp: stat.ModTime().UTC()},
	}

	if isLargeFile {
		return iter.readLargeFileChunk(ctx, file, stat, fileInfo)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return result, fmt.Errorf("read file: %w", err)
	}

	filePath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	if err := iter.validateFile(ctx, stat, filePath); err != nil {
		return result, err
	}

	result.content = content
	result.metadata = iter.createMetadata(stat, filePath, len(content))

	return result, nil
}

func (iter *Iterator) readLargeFileChunk(ctx context.Context, file *sftp.File, stat os.FileInfo, fileInfo fileInfo) (fileReadResult, error) {
	result := fileReadResult{
		position:    &Position{LastProcessedFileTimestamp: stat.ModTime().UTC()},
		totalChunks: int(math.Ceil(float64(stat.Size()) / float64(iter.config.FileChunkSizeBytes))),
	}

	result.chunkIndex = 1
	if iter.position.ChunkInfo != nil &&
		iter.position.ChunkInfo.Filename == fileInfo.name &&
		iter.position.ChunkInfo.ModTime == stat.ModTime().UTC().Format(time.RFC3339) {
		result.chunkIndex = iter.position.ChunkInfo.ChunkIndex + 1
	}

	if err := ctx.Err(); err != nil {
		return result, err
	}

	offset := int64(result.chunkIndex-1) * iter.config.FileChunkSizeBytes
	chunkSize := int(math.Min(float64(iter.config.FileChunkSizeBytes), float64(stat.Size()-offset)))

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return result, fmt.Errorf("seek file: %w", err)
	}

	chunk := make([]byte, chunkSize)
	if _, err := io.ReadFull(file, chunk); err != nil {
		return result, fmt.Errorf("read chunk: %w", err)
	}

	filePath := filepath.Join(iter.config.DirectoryPath, fileInfo.name)

	if err := iter.validateFile(ctx, stat, filePath); err != nil {
		return result, err
	}

	result.content = chunk

	result.metadata = iter.createMetadata(stat, filePath, len(chunk))
	result.metadata["chunk_index"] = fmt.Sprintf("%d", result.chunkIndex)
	result.metadata["total_chunks"] = fmt.Sprintf("%d", result.totalChunks)
	result.metadata["is_chunked"] = "true"

	result.position.ChunkInfo = &ChunkInfo{
		Filename:    stat.Name(),
		ChunkIndex:  result.chunkIndex,
		TotalChunks: result.totalChunks,
		ModTime:     stat.ModTime().UTC().Format(time.RFC3339),
	}

	return result, nil
}

func (iter *Iterator) handleFileError(ctx context.Context, err error) (opencdc.Record, error) {
	if errors.Is(err, ErrFileModifiedDuringRead) {
		if err := iter.loadFiles(); err != nil {
			return opencdc.Record{}, fmt.Errorf("load files: %w", err)
		}
		return iter.Next(ctx)
	}
	// File was deleted/moved/renamed, skip to next file
	iter.files = iter.files[1:]
	return iter.Next(ctx)
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

		// Check if the file matches the pattern and is unprocessed or partially processed.
		if iter.shouldProcessFile(fileName, modTime) {
			unprocessedFiles = append(unprocessedFiles, fileInfo{
				name:    fileName,
				size:    file.Size(),
				modTime: modTime,
			})
		}
	}

	// Sort unprocessed files by modification time to maintain order.
	sort.Slice(unprocessedFiles, func(i, j int) bool {
		return unprocessedFiles[i].modTime.Before(unprocessedFiles[j].modTime)
	})

	iter.files = unprocessedFiles
	return nil
}

// helper method to determine if a file should be processed.
func (iter *Iterator) shouldProcessFile(fileName string, modTime time.Time) bool {
	if matched, _ := filepath.Match(iter.config.FilePattern, fileName); matched &&
		modTime.After(iter.position.LastProcessedFileTimestamp) {
		return true
	}

	if iter.position.ChunkInfo != nil &&
		fileName == iter.position.ChunkInfo.Filename &&
		modTime.Equal(iter.position.LastProcessedFileTimestamp) {
		return true
	}

	return false
}

// validateFile ensures file integrity during read.
func (iter *Iterator) validateFile(ctx context.Context, fileInfo os.FileInfo, filePath string) error {
	finalStat, err := iter.sftpClient.Stat(filePath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	if !fileInfo.ModTime().Equal(finalStat.ModTime()) {
		sdk.Logger(ctx).Info().Msgf(`file "%s" modified during read`, fileInfo.Name())
		return ErrFileModifiedDuringRead
	}

	return nil
}

func (iter *Iterator) createMetadata(fileInfo os.FileInfo, filePath string, contentLength int) opencdc.Metadata {
	return opencdc.Metadata{
		opencdc.MetadataCollection: iter.config.DirectoryPath,
		opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
		"filename":                 fileInfo.Name(),
		"source_path":              filePath,
		"file_size":                fmt.Sprintf("%d", contentLength),
		"mod_time":                 fileInfo.ModTime().UTC().Format(time.RFC3339),
		"hash":                     common.GenerateFileHash(fileInfo.Name(), fileInfo.ModTime().UTC(), fileInfo.Size()),
	}
}
