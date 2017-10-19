import argparse
import sys, re

import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from z3.config import get_config

MB = 1024 ** 2
def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='Read a key from s3 and write the content to stdout',
    )
    parser.add_argument('name', help='name of S3 key')
    args = parser.parse_args()
    config = TransferConfig(max_concurrency=int(cfg['CONCURRENCY']), multipart_chunksize=int(re.sub('M', '', cfg['CHUNK_SIZE'])) * MB)
    if 'S3_KEY_ID' in cfg:
        s3 = boto3.client('s3'), aws_access_key_id=cfg['S3_KEY_ID'], aws_secret_access_key=cfg['S3_SECRET'])
    else:
        s3 = boto3.client('s3')
    try:
        s3.download_fileobj(cfg['BUCKET'], args.name, sys.stdout, Config = config)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

if __name__ == '__main__':
    main()
