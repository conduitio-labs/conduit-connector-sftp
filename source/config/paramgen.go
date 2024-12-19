// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigAddress        = "address"
	ConfigDirectoryPath  = "directoryPath"
	ConfigFilePattern    = "filePattern"
	ConfigHostKey        = "hostKey"
	ConfigPassword       = "password"
	ConfigPollingPeriod  = "pollingPeriod"
	ConfigPrivateKeyPath = "privateKeyPath"
	ConfigUsername       = "username"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigAddress: {
			Default:     "",
			Description: "Address is the address of the sftp server to connect.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigDirectoryPath: {
			Default:     "",
			Description: "DirectoryPath is the path to the directory to read/write data.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigFilePattern: {
			Default:     "*",
			Description: "Pattern to filter files in the source directory.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigHostKey: {
			Default:     "",
			Description: "HostKey is the key used for host key callback validation.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPassword: {
			Default:     "",
			Description: "Password is the SFTP password (can be used as passphrase for private key).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigPollingPeriod: {
			Default:     "5s",
			Description: "This period is used by iterator to poll for new data at regular intervals.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		ConfigPrivateKeyPath: {
			Default:     "",
			Description: "PrivateKeyPath is the private key for ssh login.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigUsername: {
			Default:     "",
			Description: "User is the SFTP user.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}