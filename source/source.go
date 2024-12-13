package source

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/conduitio-labs/conduit-connector-sftp/source/config"
	commonsConfig "github.com/conduitio/conduit-commons/config"
)

type Source struct {
	sdk.UnimplementedSource

	config     config.Config
	sshClient  *ssh.Client
	sftpClient *sftp.Client

	position *Position
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware(
		// disable schema extraction by default, because the source produces raw payload data
		sdk.SourceWithSchemaExtractionConfig{
			PayloadEnabled: lang.Ptr(false),
		},
	)...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() commonsConfig.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfgRaw commonsConfig.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	// Establish SSH connection
	sshConfig, err := s.createSSHConfig()
	if err != nil {
		return fmt.Errorf("create SSH config: %w", err)
	}

	s.sshClient, err = ssh.Dial("tcp", s.config.Address, sshConfig)
	if err != nil {
		return fmt.Errorf("dial SSH: %w", err)
	}

	// Create SFTP client
	s.sftpClient, err = sftp.NewClient(s.sshClient)
	if err != nil {
		s.sshClient.Close()
		return fmt.Errorf("create SFTP client: %w", err)
	}

	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	fmt.Println("trying to read-------------")
	// List files in the source directory
	files, err := s.listUnprocessedFiles()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("list files: %w", err)
	}

	fmt.Println("got files------------ ", len(files))

	if len(files) == 0 {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	// Process the first unprocessed file
	filename := files[0]
	record, err := s.processFile(ctx, filename)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("process file %s: %w", filename, err)
	}

	fmt.Println("file processed------- ", filename)
	fmt.Println("here is the record------- ", record)

	return record, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	var errs []error

	if s.sftpClient != nil {
		if err := s.sftpClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close SFTP client: %w", err))
		}
	}

	if s.sshClient != nil {
		if err := s.sshClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close SSH client: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %v", errs)
	}
	return nil
}

// createSSHConfig creates SSH configuration for authentication
func (s *Source) createSSHConfig() (*ssh.ClientConfig, error) {
	var authMethod ssh.AuthMethod

	// Prefer private key if provided
	if s.config.PrivateKeyPath != "" {
		// Read private key file
		privateKeyBytes, err := os.ReadFile(s.config.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("read private key file: %w", err)
		}

		var signer ssh.Signer
		// Check if key requires a passphrase
		if s.config.Password != "" {
			// Key is passphrase-protected
			signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKeyBytes, []byte(s.config.Password))
		} else {
			// Try parsing key without passphrase
			signer, err = ssh.ParsePrivateKey(privateKeyBytes)
		}

		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		authMethod = ssh.PublicKeys(signer)
	} else if s.config.Username != "" && s.config.Password != "" {
		// Use password authentication
		authMethod = ssh.Password(s.config.Password)
	} else {
		return nil, fmt.Errorf("no valid authentication method provided")
	}

	return &ssh.ClientConfig{
		User:            s.config.Username,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Caution: Not secure for production
		Timeout:         30 * time.Second,        
	}, nil
}

// listUnprocessedFiles finds files matching the pattern that haven't been processed
func (s *Source) listUnprocessedFiles() ([]string, error) {
	files, err := s.sftpClient.ReadDir(s.config.SourceDirectory)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	unprocessedFiles := []string{}
	for _, file := range files {
		if !file.IsDir() {
			filename := file.Name()
			// Check file pattern match and modification time
			if matched, _ := filepath.Match(s.config.FilePattern, filename); matched &&
				file.ModTime().After(s.position.LastProcessedFileTimestamp) {
				unprocessedFiles = append(unprocessedFiles, filename)
			}
		}
	}

	fmt.Println("unprocessed files---------- ", len(unprocessedFiles))

	return unprocessedFiles, nil
}

// processFile converts a file into an OpenCDC record
func (s *Source) processFile(_ context.Context, filename string) (opencdc.Record, error) {
	fullPath := filepath.Join(s.config.SourceDirectory, filename)

	// Get file info
	fileInfo, err := s.sftpClient.Stat(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("get file info: %w", err)
	}

	// Open the file
	file, err := s.sftpClient.Open(fullPath)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Read file contents
	content, err := io.ReadAll(file)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("read file: %w", err)
	}

	// Create record metadata
	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: s.config.SourceDirectory,
		opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
		"filename":                 filename,
		"source_path":              fullPath,
		"file_size":                fmt.Sprintf("%d", len(content)),
		"mod_time":                 fileInfo.ModTime().UTC().Format(time.RFC3339),
	}

	// Create record position
	position := &Position{
		LastProcessedFileTimestamp: fileInfo.ModTime(),
	}
	positionBytes, err := json.Marshal(position)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	// update record position
	s.position = position

	// Create OpenCDC record
	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		opencdc.StructuredData{"filename": filename},
		opencdc.RawData(content),
	), nil
}
