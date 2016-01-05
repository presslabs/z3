from datetime import datetime

import boto

from z3.pput import UploadSupervisor, StreamHandler
from z3.config import get_config
from .fixtures import sample_data


def test_integration(sample_data):
    cfg = get_config()
    stream_handler = StreamHandler(sample_data)
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    key_name = "z3_test_" + datetime.now().strftime("%Y%m%d_%H-%M-%S")
    sup = UploadSupervisor(stream_handler, key_name, bucket=bucket)
    etag = sup.main_loop()
    assert etag == '"d229c1fc0e509475afe56426c89d2724-2"'
