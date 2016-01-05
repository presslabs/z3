## z3/pput This is Work In Progress.

Currently provides one tool pput (parallel put) for multipart uploading to s3.
To be determined if we add the zfs snapshot handling logic.

### Design choices
pput is a simple tool with one job, read data from stdin and upload it to S3.

This tool was primarily written to move baskup data, so consistency is important,
it's better to fail hard when something goes wrong than silently upload
inconsistent data or partial data.

There are few anticipated errors (if a part fails to upload, retry 3 times).
Any other problem is unanticipated, so just let the tool crash.

TL;DR Fail early, fail hard.

### TODO:
 * failed upload cleanup

