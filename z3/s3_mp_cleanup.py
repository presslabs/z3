import argparse
from datetime import datetime

import boto

from z3.config import get_config


def cleanup_multipart(bucket, max_days=1, dry_run=False):
    max_age_seconds = max_days * 24 * 3600
    now = datetime.utcnow()
    fmt = "{} | {:30} | {:20}"
    print fmt.format("A", "key", "initiated")
    for multi in bucket.list_multipart_uploads():
        delta = now-boto.utils.parse_ts(multi.initiated)
        if delta.total_seconds() >= max_age_seconds:
            print fmt.format("X", multi.key_name, multi.initiated)
            if not dry_run:
                multi.cancel_upload()
        else:
            print fmt.format(" ", multi.key_name, multi.initiated)


def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='Cleanup hanging multipart s3 uploads',
    )
    parser.add_argument('--max-age',
                        dest='max_days',
                        default=1,
                        type=int,
                        help='maximum age in days')
    parser.add_argument('--dry',
                        dest='dry_run',
                        action='store_true',
                        help='Don\'t cancel any upload')
    args = parser.parse_args()
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    cleanup_multipart(
        bucket,
        max_days=args.max_days,
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
