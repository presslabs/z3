# z3 ZFS to S3 backup tool

[![Build Status](https://travis-ci.org/PressLabs/z3.svg)](https://travis-ci.org/PressLabs/z3)

### Usage
`z3 status` will show you the current state, what snapshots you have on S3 and on the local
zfs dataset.

`z3 backup` perform full or incremental backups of your dataset.

`z3 restore` restores your dataset to a certain snapshot.

See `zfs SUBCOMMAND --help` for more info.

### Installing
TODO

z3 is tested on python 2.7 with boto 2.2.2 (python-boto version on ubuntu 12.04) and latest.

### Configuring
Most options can be configured as command line flags, environment variables or in a config file,
in that order of precedence.

The config file is read from /etc/z3_backup/z3.conf if it exists, some defaults are provided by the tool.

BUCKET S3_KEY_ID and S3_SECRET can't be provided on the command line.

For a list of all options see z3/sample.conf

## Development Overview

### The data
Snapshots are obtained using `zfs send`.
Incremental snapshots are always 'squashed', so if you have multiple snapshots to send
since the last time you've performed a backup they get exported as individual snapshots
(multiple calls to `zfs send -i dataset@snapA dataset@snapB`).

Your snapshots end up as individual keys in an s3 bucket, with a configurable prefix (`S3_PREFIX`).
S3 key metadata is used to identify if a snapshot is full (`is_full="true"`) or incremental.
The parent of an incremental snapshot is identified with the `parent` attribute'.

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
