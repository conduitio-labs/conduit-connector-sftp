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

package sftp

import (
	"fmt"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-sftp/config"
	"github.com/conduitio-labs/conduit-connector-sftp/destination"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
	id int64
}

func (d *driver) GenerateRecord(_ *testing.T, _ opencdc.Operation) opencdc.Record {
	atomic.AddInt64(&d.id, 1)

	content := []byte("hello world")
	filename := fmt.Sprintf("%d.txt", d.id)

	return sdk.Util.Source.NewRecordCreate(
		nil,
		map[string]string{
			opencdc.MetadataCollection: "upload",
			opencdc.MetadataCreatedAt:  time.Now().UTC().Format(time.RFC3339),
			"filename":                 filename,
			"source_path":              "/upload",
			"file_size":                fmt.Sprintf("%d", len(content)),
			"mod_time":                 time.Now().UTC().Format(time.RFC3339),
		},
		opencdc.StructuredData{"filename": filename},
		opencdc.RawData(content),
	)
}

func (d *driver) ReadFromDestination(_ *testing.T, records []opencdc.Record) []opencdc.Record {
	return records
}

func TestAcceptance(t *testing.T) {
	hostKey, err := setupHostKey()
	if err != nil {
		fmt.Println(err)
		return
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewDestination:   destination.NewDestination,
					NewSource:        nil,
				},
				DestinationConfig: map[string]string{
					config.ConfigAddress:       "localhost:2222",
					config.ConfigHostKey:       hostKey,
					config.ConfigUsername:      "user",
					config.ConfigPassword:      "pass",
					config.ConfigDirectoryPath: "/upload",
				},
			},
		},
	})
}

func setupHostKey() (string, error) {
	cmd := exec.Command("ssh-keyscan", "-t", "rsa", "-p", "2222", "localhost")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("error setupHostKey: %w", err)
	}
	return string(output), nil
}
