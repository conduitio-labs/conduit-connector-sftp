package config

import (
	"github.com/conduitio-labs/conduit-connector-sftp/config"
)

//go:generate paramgen -output=paramgen.go Config

type Config struct {
	config.Config

	// Directory on the remote SFTP server to retrieve files from.
	SourceDirectory string `json:"sourceDirectory" validate:"required"`
	// Pattern to filter files in the source directory.
	FilePattern string `json:"filePattern" default:"*"`
}

// Validate executes manual validations.
func (c Config) Validate() error {
	err := c.Config.Validate()

	return err
}
