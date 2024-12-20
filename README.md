# Conduit Connector Template

The SFTP connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It
provides both, a source and a destination SFTP connector.

## How to build it

Run `make build`.

## Testing

Run `make test` to run all the unit and integration tests.

## Source

### Configuration Options

## Destination

Destination connects to a remote server. It takes an `opencdc.Record`, extracts filename from the metadata and upload the file to the remote server.

### Configuration Options

| name           | description                                                                                           | required |
| -------------- | ----------------------------------------------------------------------------------------------------- | -------- |
| `address` | Address is the address of the sftp server to connect.| **true** |
| `hostKey` | HostKey is the key used for host key callback validation.| **true** |
| `username`| User is the username of the SFTP user. | **true** |
| `password`| Password is the SFTP password (can be used as passphrase for private key). | false |
| `privateKeyPath`| PrivateKeyPath is the private key for ssh login.| false |
| `directoryPath` | DirectoryPath is the path to the directory to read/write data. | true |

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=64b333ae-77ad-4895-a5cd-a73bb14362d9)
