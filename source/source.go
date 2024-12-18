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

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
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

	config   config.Config
	position *Position

	sshClient  *ssh.Client
	sftpClient *sftp.Client

	ch   chan opencdc.Record
	done chan struct{}
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

	err = s.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
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

	s.ch = make(chan opencdc.Record)
	s.done = make(chan struct{})

	err = NewIterator(ctx, s.sshClient, s.sftpClient, s.position, s.config, s.done, s.ch)
	if err != nil {
		return fmt.Errorf("creating iterator: %w", err)
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from SFTP Source...")

	if s == nil || s.ch == nil {
		return opencdc.Record{}, errors.New("source not opened for reading")
	}

	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case record, ok := <-s.ch:
		if !ok {
			return opencdc.Record{}, fmt.Errorf("error reading data, records channel closed unexpectedly")
		}
		return record, nil //nolint:nlreturn // compact code style
	default:
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Trace().
		Str("position", string(position)).
		Msg("got ack")

	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	var errs []error

	// Ensure the iterator has finished
	if s.done != nil {
		select {
		case <-s.done:
			sdk.Logger(ctx).Debug().Msg("Teardown: Iterator finished.")
		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Msg("Teardown: Context cancelled while waiting for iterator.")
		}
	}

	if s.ch != nil {
		// close the read channel for write
		close(s.ch)
		// reset read channel to nil, to avoid reading buffered records
		s.ch = nil
	}

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

	return errors.Join(errs...)
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

	// Parse the trusted host public key
	parsedPublicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s.config.ServerPublicKey))
	if err != nil {
		return nil, fmt.Errorf("parse trusted host key: %w", err)
	}

	hostKeyCallback := func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		// Byte-to-byte comparison of the keys
		if key.Type() != parsedPublicKey.Type() {
			return fmt.Errorf("host key type mismatch: got %s, want %s",
				key.Type(), parsedPublicKey.Type())
		}

		if !bytes.Equal(key.Marshal(), parsedPublicKey.Marshal()) {
			return fmt.Errorf("host key does not match the trusted key")
		}

		return nil
	}

	return &ssh.ClientConfig{
		User:            s.config.Username,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}, nil
}
