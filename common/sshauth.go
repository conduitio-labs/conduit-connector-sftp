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

package common

import (
	"bytes"
	"fmt"
	"net"
	"os"

	"golang.org/x/crypto/ssh"
)

var ErrUntrustedKey = fmt.Errorf("host key does not match the trusted key")

type MismatchKeyTypeError struct {
	key1, key2 string
}

func (e MismatchKeyTypeError) Error() string {
	return fmt.Sprintf("host key type mismatch: got %s, want %s", e.key1, e.key2)
}

func NewMismatchKeyTypeError(key1, key2 string) MismatchKeyTypeError {
	return MismatchKeyTypeError{key1, key2}
}

func SSHConfigAuth(remoteHostKey, username, password, privateKeyPath string) (*ssh.ClientConfig, error) {
	//nolint:dogsled // not required here.
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(remoteHostKey))
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
		User:            username,
		HostKeyCallback: hostKeyCallback,
	}

	if privateKeyPath != "" {
		auth, err := authWithPrivateKey(privateKeyPath, password)
		if err != nil {
			return nil, err
		}

		sshConfig.Auth = []ssh.AuthMethod{auth}
		return sshConfig, nil
	}

	sshConfig.Auth = []ssh.AuthMethod{ssh.Password(password)}
	return sshConfig, nil
}

func authWithPrivateKey(privateKeyPath, password string) (ssh.AuthMethod, error) {
	key, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	if password != "" {
		signer, err := ssh.ParsePrivateKeyWithPassphrase(key, []byte(password))
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
