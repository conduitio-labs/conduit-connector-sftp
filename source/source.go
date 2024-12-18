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
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/conduitio-labs/conduit-connector-sftp/source/config"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

var (
	ErrSourceNotOpened = errors.New("source not opened for reading")
	ErrChannelClosed   = errors.New("error reading data, records channel closed unexpectedly")
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
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	err = s.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening a SFTP Source...")
	sshConfig, err := s.sshConfigAuth()
	if err != nil {
		return fmt.Errorf("failed to create SSH config: %w", err)
	}

	s.sshClient, err = ssh.Dial("tcp", s.config.Address, sshConfig)
	if err != nil {
		return fmt.Errorf("failed to dial SSH: %w", err)
	}

	s.sftpClient, err = sftp.NewClient(s.sshClient)
	if err != nil {
		s.sshClient.Close()
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}

	_, err = s.sftpClient.Stat(s.config.DirectoryPath)
	if err != nil {
		return fmt.Errorf("remote path does not exist: %w", err)
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
		return opencdc.Record{}, ErrSourceNotOpened
	}

	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case record, ok := <-s.ch:
		if !ok {
			return opencdc.Record{}, ErrChannelClosed
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
	sdk.Logger(ctx).Info().Msg("Tearing down the SFTP Source")
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

	return errors.Join(errs...)
}

func (s *Source) sshConfigAuth() (*ssh.ClientConfig, error) {
	sshConfig := &ssh.ClientConfig{
		User: s.config.Username,
	}

	//nolint:dogsled // not required here.
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s.config.HostKey))
	if err != nil {
		return nil, fmt.Errorf("failed to parse host key: %w", err)
	}

	sshConfig.HostKeyCallback = ssh.FixedHostKey(hostKey)

	if s.config.PrivateKeyPath != "" {
		auth, err := s.authWithPrivateKey()
		if err != nil {
			return nil, err
		}

		sshConfig.Auth = []ssh.AuthMethod{auth}
		return sshConfig, nil
	}

	sshConfig.Auth = []ssh.AuthMethod{ssh.Password(s.config.Password)}
	return sshConfig, nil
}

func (s *Source) authWithPrivateKey() (ssh.AuthMethod, error) {
	key, err := os.ReadFile(s.config.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	if s.config.Password != "" {
		signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(s.config.Password))
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
		return ssh.PublicKeys(signer), nil
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return ssh.PublicKeys(signer), nil
}
