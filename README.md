# z3 [![Build Status](https://travis-ci.org/presslabs/z3.svg)](https://travis-ci.org/presslabs/z3)

# Welcome to z3

z3 is a ZFS to S3 backup tool. This is basically plumbing around `zfs send` and `zfs receive`
so you should have at least a basic understanding of what those commands do.

z3 was developed by the awesome engineering team at [Presslabs](https://www.presslabs.com/), 
a Managed WordPress Hosting provider.

For more open-source projects, check [Presslabs Code](https://www.presslabs.com/code/). 

## Usage
`z3 status` will show you the current state, what snapshots you have on S3 and on the local
zfs dataset.

`z3 backup` perform full or incremental backups of your dataset.

`z3 restore` restores your dataset to a certain snapshot.

See `zfs SUBCOMMAND --help` for more info.

### Installing
`pip install z3`

z3 is tested on python 2.7 with latest boto 2 and boto 2.2.2 (python-boto version on Ubuntu 12.04).

#### Optional dependencies
```
# Install pv to get some progress indication while uploading.
apt-get install pv

# Install pigz to provide the pigz compressors.
apt-get install pigz

# Install gnupg to provide public-key encryption and compression with gpg.
apt-get install gnupg gnupg-agent
```

### Configuring
Most options can be configured as command line flags, environment variables or in a config file,
in that order of precedence.
The config file is read from `/etc/z3_backup/z3.conf` if it exists, some defaults are provided by the tool.
BUCKET `S3_KEY_ID` and `S3_SECRET` can't be provided on the command line.
For a list of all options see `z3/sample.conf`.

You'll usually want z3 to only backup certain snapshots (hourly/daily/weekly).
To do that you can specify a `SNAPSHOT_PREFIX` (defaults to `zfs-auto-snap:daily`).

Defaults for `SNAPSHOT_PREFIX` and `COMPRESSOR` can be set per filesystem like so:
```
[fs:tank/spam]
SNAPSHOT_PREFIX=delicious-daily-spam
COMPRESSOR=pigz4

[fs:tank/ham]
SNAPSHOT_PREFIX=weekly-non-spam
```

### Dataset Size, Concurrency and Memory Usage
Since the data is streamed from `zfs send` it gets read in to memory in chunks.
Z3 estimates a good chunk size for you: no smaller than 5MB and large enough
to produce at most 9999 chunks. These are S3 limitation for multipart uploads.
Here are some example chunk sizes for different datasets:
 * 50 GiB: 5 MiB
 * 500 GIB: 53 MiB
 * 1 TiB: 110 MiB
 * 2 TiB: 220 MiB

Multiply that by `CONCURRENCY` to know how much memory your upload will use.

### Usage Examples

#### Status
```
# show global options
z3 --help

# show status of backups for default dataset
z3 status

# show status for other dataset; only snapshots named daily-spam-*
z3 --dataset tank/spam --snapshot-prefix daily-spam- status
```

#### Backup
```
# show backup options
z3 backup --help

# perform incremental backup the latest snapshot; use pigz4 compressor
z3 backup --compressor pigz4 --dry-run
# inspect the commands that would be executed
z3 backup --compressor pigz4

# perform full backup of a specific snapshot
z3 backup --full --snapshot the-part-after-the-at-sign --dry-run
# inspect the commands that would be executed
z3 backup --full --snapshot the-part-after-the-at-sign
```

#### Restore
```
# see restore options
z3 restore --help

# restore a dataset to a certain snapshot
z3 restore the-part-after-the-at-sign --dry-run
# inspect the commands that would be executed
z3 restore the-part-after-the-at-sign

# force rollback of filesystem (zfs recv -F)
z3 restore the-part-after-the-at-sign --force
```

### Encryption
Encryption of stored objects in S3 is normally provided through AWS Key Management Service (KMS). Alternatively, you can use gnupg for public-key encryption by specifying gpg as a `COMPRESSOR` and the public key to use as `GPG_RECIPIENT`. Note: compression and crypto algorithms used by gpg are derived from the public key preferences for `GPG_RECIPIENT`. Here is a usage example:
```
# inspect the key preferences for z3_backup
#   based on preference order, gpg will use AES256 cipher, and ZLIB compression
gpg --edit-key z3_backup

gpg> showpref
[ultimate] (1). z3_backup
     Cipher: AES256, AES192, AES, 3DES
     Digest: SHA256, SHA384, SHA512, SHA224, SHA1
     Compression: ZLIB, BZIP2, ZIP, Uncompressed
     Features: MDC, Keyserver no-modify

gpg> quit

# the following assumes that you have z3_backup in your gnupg public-key ring
# perform incremental backup the latest snapshot; use gpg compressor
z3 backup --compressor gpg --gpg-recipient z3_backup --dry-run
# after inspectng the commands that would be executed, perform the backup
z3 backup --compressor gpg --gpg-recipient z3_backup

# the following assumes that you have z3_backup in your gnupg private-key ring
# restore a dataset to a certain snapshot
z3 restore the-part-after-the-at-sign --dry-run
# after inspectng the commands that would be executed, perform the restore
z3 restore the-part-after-the-at-sign
```

### Other Commands
Other command line tools are provided.

`pput` reads a stream from standard in and uploads the data to S3.

`z3_ssh_sync` a convenience tool to allow you to push zfs snapshots to another host.
If you need replication you should checkout zrep. This exists because we've already
got zrep between 2 nodes and needed a way to push backups to a 3rd machine.

`z3_get` called by `z3 restore` to download a backup.

## Development Overview
### Running the tests
The test suite uses pytest.
Some of the tests upload data to S3, so you need to setup the following environment:
```
export S3_KEY_ID=""
export S3_SECRET=""
export BUCKET="mytestbucket"
```

To skip tests that use S3:
```
py.test --capture=no --tb=native _tests/ -k "not with_s3"
```

### The Data
Snapshots are obtained using `zfs send`, optionally piped trough a compressor (pigz by default),
and finally piped to `pput`.
Incremental snapshots are always handled individually, so if you have multiple snapshots to send
since the last time you've performed a backup they get exported as individual snapshots
(multiple calls to `zfs send -i dataset@snapA dataset@snapB`).

Your snapshots end up as individual keys in an s3 bucket, with a configurable prefix (`S3_PREFIX`).
S3 key metadata is used to identify if a snapshot is full (`isfull="true"`) or incremental.
The parent of an incremental snapshot is identified with the `parent` attribute.

S3 and ZFS snapshots are matched by name.

### Health checks
The S3 health checks are very rudimentary, basically if a snapshot is incremental check
that the parent exists and is healthy. Full backups are always assumed healthy.

If backup/restore encounter unhealthy snapshots they abort execution.

### pput
pput is a simple tool with one job, read data from stdin and upload it to S3.
It's usually invoked by z3.

Consistency is important, it's better to fail hard when something goes wrong
than silently upload inconsistent or partial data.
There are few anticipated errors (if a part fails to upload, retry MAX_RETRY times).
Any other problem is unanticipated, so just let the tool crash.

TL;DR Fail early, fail hard.
