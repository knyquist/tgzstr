# https://github.com/RaRe-Technologies/smart_open

import sys
import argparse
import tarfile
from smart_open import smart_open
import logging

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket',
                        help='S3 bucket destination')
    parser.add_argument('prefix',
                        help='S3 destination prefix')
    parser.add_argument('-s',
                        '--chunksize',
                        default=4096)
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    with tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz') as tarball:
        for eachfile in tarball:
            if eachfile.isdir():
                log.info('Skipping dir reference: {}'.format(eachfile))
                continue
            else:
                filename_with_path = eachfile.name[2:]
                log.info('Tar file name: {}'.format(eachfile.name))
                s3_dest = 's3://{bucket_name}/{dest_prefix}/{file_path}'.format(dest_prefix=args.prefix,
                                                                                bucket_name=args.bucket,
                                                                                file_path=filename_with_path)

                log.info('Streaming to s3...')
                tar_stream = tarball.extractfile(eachfile) # returns a BufferedReader
                with smart_open(s3_dest, 'wb') as dest_file:
                    for chunk in iter(lambda: tar_stream.read(args.chunksize), b''):
                        dest_file.write(chunk)

                log.info('Done with {}'.format(filename_with_path))

if __name__ == '__main__':
    main()

