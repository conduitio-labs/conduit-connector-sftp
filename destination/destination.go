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
	"errors"
	"fmt"
	"net"
	"os"

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
	sshConfig, err := d.sshConfigAuth()
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
		filename, ok := record.Metadata["filename"]
		if !ok {
			filename = string(record.Key.Bytes())
		}

		err := d.uploadFile(filename, record.Payload.After.Bytes())
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

func (d *Destination) sshConfigAuth() (*ssh.ClientConfig, error) {
	//nolint:dogsled // not required here.
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(d.config.HostKey))
	if err != nil {
		return nil, fmt.Errorf("failed to parse server public key: %w", err)
	}

	hostKeyCallback := func(_ string, _ net.Addr, key ssh.PublicKey) error {
		if key.Type() != hostKey.Type() {
			return NewMismatchKeyTypeError(key.Type(), hostKey.Type())
		}

		if !bytes.Equal(key.Marshal(), hostKey.Marshal()) {
			return ErrUntrustedKey
		}

		return nil
	}

	sshConfig := &ssh.ClientConfig{
		User:            d.config.Username,
		HostKeyCallback: hostKeyCallback,
	}

	if d.config.PrivateKeyPath != "" {
		auth, err := d.authWithPrivateKey()
		if err != nil {
			return nil, err
		}

		sshConfig.Auth = []ssh.AuthMethod{auth}
		return sshConfig, nil
	}

	sshConfig.Auth = []ssh.AuthMethod{ssh.Password(d.config.Password)}
	return sshConfig, nil
}

func (d *Destination) authWithPrivateKey() (ssh.AuthMethod, error) {
	key, err := os.ReadFile(d.config.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	if d.config.Password != "" {
		signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(d.config.Password))
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

func (d *Destination) uploadFile(filename string, content []byte) error {
	remoteFile, err := d.sftpClient.Create(fmt.Sprintf("%s/%s", d.config.DirectoryPath, filename))
	if err != nil {
		return fmt.Errorf("failed to create remote file: %w", err)
	}
	defer remoteFile.Close()

	reader := bytes.NewReader(content)
	_, err = reader.WriteTo(remoteFile)
	if err != nil {
		return fmt.Errorf("failed to write content to remote file: %w", err)
	}

	return nil
}
