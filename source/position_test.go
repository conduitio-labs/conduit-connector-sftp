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
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      opencdc.Position
		wantPos *Position
		wantErr error
	}{
		{
			name:    "success_position_is_nil",
			in:      nil,
			wantPos: &Position{},
		},
		{
			name: "success_valid_position",
			in: opencdc.Position(`{
				"lastProcessedFileTimestamp": "2024-06-01T12:00:00Z"
			}`),
			wantPos: &Position{
				LastProcessedFileTimestamp: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "failure_invalid_json",
			in:      opencdc.Position("invalid"),
			wantErr: errors.New("unmarshal position: invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := ParseSDKPosition(tt.in)
			if tt.wantErr == nil {
				is.NoErr(err)
				is.Equal(got, tt.wantPos)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}
