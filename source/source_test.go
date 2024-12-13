package source_test

import (
	"context"
	"testing"

	source "github.com/conduitio-labs/conduit-connector-sftp/source"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := source.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
