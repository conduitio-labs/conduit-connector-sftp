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

package config

import (
	"fmt"
	"testing"

	"github.com/matryer/is"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      Config
		wantErr error
	}{
		{
			name: "success_with_password",
			in: Config{
				Address:         "sftp.example.com",
				Username:        "user",
				Password:        "pass123",
				Directory:       "/uploads",
				ServerPublicKey: "publickey123",
			},
		},
		{
			name: "success_with_private_key",
			in: Config{
				Address:         "sftp.example.com",
				Username:        "user",
				PrivateKeyPath:  "/path/to/private/key",
				Directory:       "/uploads",
				ServerPublicKey: "publickey123",
			},
		},
		{
			name: "failure_no_authentication",
			in: Config{
				Address:         "sftp.example.com",
				Username:        "user",
				Directory:       "/uploads",
				ServerPublicKey: "publickey123",
			},
			wantErr: fmt.Errorf("either %q or %q must be provided for sftp authentication", KeyPassword, KeyPrivateKeyPath),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			err := tt.in.Validate()
			if tt.wantErr == nil {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}
