#!/bin/sh

FLAGS=""

if [ -n "$S3_KEY_ID" ]; then
    py.test
else
    py.test -k 'not with_s3'
fi
