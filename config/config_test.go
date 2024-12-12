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
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	type fields struct {
		Address        string
		Username       string
		Password       string
		PrivateKeyPath string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "success: password authentication",
			fields: fields{
				Address:  "localhost:22",
				Username: "user",
				Password: "pass",
			},
			wantErr: nil,
		},
		{
			name: "success: publickey authentication",
			fields: fields{
				Address:        "localhost:22",
				PrivateKeyPath: "path",
			},
			wantErr: nil,
		},
		{
			name: "success: no authentication",
			fields: fields{
				Address: "localhost:22",
			},
			wantErr: ErrAuthRequired,
		},
		{
			name: "success: missing password",
			fields: fields{
				Address:  "localhost:22",
				Username: "user",
			},
			wantErr: NewRequiredWithError(ConfigPassword, ConfigUsername),
		},
		{
			name: "success: missing username",
			fields: fields{
				Address:  "localhost:22",
				Password: "pass",
			},
			wantErr: NewRequiredWithError(ConfigUsername, ConfigPassword),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Address:        tt.fields.Address,
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				PrivateKeyPath: tt.fields.PrivateKeyPath,
			}
			err := c.Validate()
			if err != nil && err.Error() != tt.wantErr.Error() {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
