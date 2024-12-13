package sftp_test

import (
	"context"
	"testing"

	sftp "github.com/conduitio-labs/conduit-connector-sftp"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := sftp.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
