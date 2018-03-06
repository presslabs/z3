import argparse
import sys

import boto.s3

from z3.config import get_config


def download(bucket, name):
    key = bucket.get_key(name)
    key.get_contents_to_file(sys.stdout)


def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='Read a key from s3 and write the content to stdout',
    )
    parser.add_argument('name', help='name of S3 key')
    args = parser.parse_args()
    extra_config = {}
    if 'HOST' in cfg:
        extra_config['host'] = cfg['HOST']
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'],
        cfg['S3_SECRET'], **extra_config).get_bucket(cfg['BUCKET'])
    download(bucket, args.name)

if __name__ == '__main__':
    main()
