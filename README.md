# Conduit Connector SFTP

The SFTP connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It
provides both, a source and a destination SFTP connector.

## How to build it

Run `make build`.

## Testing

Run `make test` to run all the unit and integration tests.

## Source

The source SFTP connector monitors a directory on an SFTP server for files matching a specified pattern. It reads these files and converts them into `opencdc.Record` that can be processed by Conduit. For handling large files, it splits them into smaller chunks, enabling smooth data handling through the Conduit pipeline.
The connector supports both password and private key authentication methods.

### Configuration Options

| name           | description                                                                                           | required | default value |
| -------------- | ----------------------------------------------------------------------------------------------------- | -------- | -------- |
| `address` | Address is the address of the sftp server to connect.| **true** |  |
| `hostKey` | HostKey is the key used for host key callback validation.| **true** |  |
| `username`| User is the username of the SFTP user. | **true** |  |
| `password`| Password is the SFTP password (can be used as passphrase for private key). | false |  |
| `privateKeyPath`| PrivateKeyPath is the private key for ssh login.| false |  |
| `directoryPath` | DirectoryPath is the path to the directory to read data. | **true** |  |
| `filePattern` | Pattern to match files that should be read (e.g., "*.txt") | false | `*` |
| `fileChunkSizeBytes` | Maximum size of a file chunk in bytes to split large files. | false | `3145728` |

## Destination

Destination connects to a remote server. It takes an `opencdc.Record`, extracts filename from the metadata and upload the file to the remote server. The connector supports both password and private key authentication methods. The connector will sync only those files that are in the source directory itself.
Destination also supports large file uploads. Source can provide large file content chunk by chunk (one record per chunk). Each record should have following in metadata:

* `filename`: Filename of the file with extension.
* `file_size`: Integer size of the file.
* `chunk_index`: Index of the chunk (starting from 1).
* `total_chunks`: Total number of chunks.
* `hash`: Unique hash, which is used to create temporary file till the last chunk is uploaded.

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
