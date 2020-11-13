#!/usr/bin/env python2.7

import os
import re
from shutil import rmtree
from argparse import ArgumentParser
from random import randint
import logging
import boto
from swiftclient import client as swiftclient
from cStringIO import StringIO
from datetime import datetime
# import vipr_tools.ecs_bucket

# Add a custom level for normal output
NORMAL                      = 25
logging.addLevelName(NORMAL, 'NORMAL')
def normal(self,message,*args,**kwargs):
    self._log(NORMAL,message,args,**kwargs)
logging.Logger.normal = normal

DEFAULT_STOP_FILE               = '/tmp/stop_create_test_files'
DEFAULT_MAX_ERRORS              = 5
DEFAULT_TMP_DIRECTORY           = '/tmp/create_test_files_%s' % datetime.now().strftime('%s')
DEFAULT_S3_PORT                 = 9020
DEFAULT_SWIFT_PORT              = 9024
SWIFT_AUTH_URL_FMT              = 'http://{0}:{1}/auth/v1.0'

# For now no sense in going beyond terabytes
multipliers = {
               ('k','kb'):1024,
               ('m','mb'):1024*1024,
               ('g','gb'):1024*1024*1024,
               ('t','tb'):1024*1024*1024*1024
               }

DEFAULT_FILENAME_FORMAT         = 'test_file_%06d.txt'

def human2bytes(n):
    res = re.match('(?P<num>\d+)(?P<mult>\S+)*',str(n))
    if res:
        n = int(res.group('num'))
        if res.group('mult'):
            for k,v in multipliers.items():
                if res.group('mult').lower() in k:
                    return n * v
    return int(n)

def write_file(fname,size,log,lower=None,upper=None,add_content=None,buffer_size=human2bytes('128k')):
    if add_content is None:
        write_data  = ('This is line in file: %s\n' % fname).replace('line','line %015d')
    else:
        write_data  = ('%s - This is line in file: %s\n' % (add_content,fname)).replace('line','line %015d')
    write_len = len(write_data % 0)
    log.debug("write_len is: %d" % write_len)
    data = open(fname,'w')
    total_written = 0
    if lower:
        size = randint(lower,upper)
        log.debug("The random size between {0:,} and {1:,} is: {2:,}".format(lower,upper,size))
    write_buffer = StringIO()
    count = 0
    for i in xrange(size/write_len):
        if write_buffer.tell() >= buffer_size:
            write_buffer.seek(0)
            data.write(write_buffer.read(buffer_size))
            count += 1
            remainder = write_buffer.read()
            log.debug("Current line number is %d, wrote buffer %d of size %d to data file, remainder: >>%s<<" % (i,count,buffer_size,remainder))
            write_buffer.close()
            write_buffer = StringIO()
            write_buffer.write(remainder)
        write_buffer.write(write_data % i)
        # log.debug("Wrote data of length: %d" % len(write_data % i))
        total_written += len(write_data % i)
    if size % write_len > 0:
        write_buffer.write('='*((size % write_len) - 1) + '\n')
        log.debug("Wrote filler of length: %d" % len('='*((size % write_len) - 1) + '\n'))
        total_written += len('='*((size % write_len) - 1) + '\n')
    write_buffer.seek(0)
    data.write(write_buffer.read())
    log.debug("Wrote final bytes of length: %d" % write_buffer.tell())
    write_buffer.close()
    data.flush()
    data.close()
    if total_written != size:
        raise RuntimeError("Bytes written %s to file %s not equal to requested size: %s" % ('{:,}'.format(total_written),
                                                                                            fname,
                                                                                            '{:,}'.format(size)))
    log.info("Wrote file %s with size: %d" % (fname,size))
    return size

def write_non_compressible_file(fname,size,log,lower=0,upper=0,buffer_size=human2bytes('128k')):
    if lower > 0:
        size = randint(lower,upper)
    data = open(fname,'w')
    data_written =0
    if size < buffer_size:
        log.debug("Writing data of size: %d to file" % (size))
        data.write(open('/dev/urandom','r').read(size))
    else:
        count = 0
        while data_written < size:
            count += 1
            if (size - data_written) < buffer_size:
                data.write(open('/dev/urandom','r').read((size - data_written)))
                log.debug("Wrote last buffer %d of size: %d to file data written: %d" % (count,(size - data_written),data_written))
                data_written += (size - data_written)
            else:
                data.write(open('/dev/urandom','r').read(buffer_size))
                data_written += buffer_size
                log.debug("Wrote buffer %d of size: %d to file data written: %d" % (count,buffer_size,data_written))
    data.flush()
    data.close()
    total_written = os.lstat(fname).st_size
    if total_written != size:
        raise RuntimeError("Bytes written %s to non-compressible file %s not equal to requested size: %s" % (fname,
                                                                                                             '{:,}'.format(total_written),
                                                                                                             '{:,}'.format(size)))
    log.info("Wrote non-compressible file %s with size: %d" % (fname,size))
    return size

if __name__ == '__main__':
    parser = ArgumentParser(description="Create test file(s) ")
    parser.add_argument('-p','--path',help='The directory path for the files',dest='path',default='.')
    parser.add_argument('-f','--format',help='The filename format for created files',dest='format',default=DEFAULT_FILENAME_FORMAT)
    parser.add_argument('-a','--add-content',help='Add string to file content',dest='add_content',default=None)
    parser.add_argument('-c','--count',help='The number of files to create in the bucket',dest='count',default=100,type=int)
    parser.add_argument('-s','--size',help='The file size to create',dest='size',default='1K')
    parser.add_argument('-n','--number',help='Start number for filenames',dest='number',default=0,type=int)
    parser.add_argument('-d','--datanode',help='Datanode to use for S3/Swift writes',dest='datanode',default=None)
    parser.add_argument('-b','--bucket',help="The bucket to use for S3/Swift writes, will be created if it doesn't exist",
                        dest='bucket',default=None)
    parser.add_argument('-F','--folder',help='The folder within the S3/Swift bucket',dest='folder',default=None)
    parser.add_argument('-U','--user',help='The S3/Swift username',dest='user',default=None)
    parser.add_argument('-S','--secret',help='The S3 secret key',dest='secret',default=None)
    parser.add_argument('-P','--swift-password',help='The Swift password',dest='swift_password',default=None)
    parser.add_argument('-sp','--swift-port',help='The Swift port, default=%d' % DEFAULT_SWIFT_PORT,default=DEFAULT_SWIFT_PORT)
    parser.add_argument('-t','--tmpdir',help='Temporary directory to use for large objects',dest='tmpdir',default=DEFAULT_TMP_DIRECTORY)
    parser.add_argument('-k','--keep-tmpdir',help='Keep the temporary director for objects',dest='keep',default=False,action='store_true')
    parser.add_argument('--lower',help='The lower limit for random sized files',dest='lower',default=None)
    parser.add_argument('--upper',help='The upper limit for random sized files',dest='upper',default=None)
    parser.add_argument('--no-clear-path',help='Answer to clear existing path is: No',dest='clear_path',default=True,action='store_false')
    parser.add_argument('-v','--verbose',help='Show status messages',dest='verbose',default=False,action='store_true')
    parser.add_argument('--summary',help='Print a summary of files and bytes written when done',dest='summary',default=False,action='store_true')
    parser.add_argument('--stop-file',help='The stop file to cleanly stop the creates',dest='stop_file',default=DEFAULT_STOP_FILE)
    parser.add_argument('--no-exit-on-error',help='Do not exit after encountering an error on file',dest='exit_on_error',default=True,
                        action='store_false')
    parser.add_argument('-m','--max-errors',help='If not exiting on error max errors in a row before exiting, default=%d' % DEFAULT_MAX_ERRORS,
                        dest='max_errors',default=DEFAULT_MAX_ERRORS,type=int)
    parser.add_argument('-nc','--non-compressible',dest='compressible',help='Make files that are non-compressible',default=True,action='store_false')
    parser.add_argument('-l','--log',help='Write log output to file',dest='log',default=None)
    parser.add_argument('--debug',dest='debug',help='Enable debug logging to stdout',default=False,action='store_true')
    args = parser.parse_args()

    log = logging.getLogger('Create test files')
    formatter = logging.Formatter('%(asctime)s %(levelname)s:(%(process)d): %(message)s')
    if args.debug:
        log.setLevel(logging.DEBUG)
    elif args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    if args.log is None:
        hdlr = logging.StreamHandler()
    else:
        hdlr = logging.FileHandler(args.log)

    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)

    s3connection = None
    s3bucket = None
    swift_client = None
    swift_container = None

    if args.bucket is None:
        if args.path != '.':
            if not os.path.isdir(args.path):
                try:
                    os.makedirs(args.path)
                except OSError,e:
                    if hasattr(e,'strerror') and e.strerror.lower() != 'file exists':
                        raise
            elif args.clear_path:
                ans = raw_input("Path %s exists, clear it? (y/n) " % args.path)
                if re.match('y(es)*',ans,re.IGNORECASE):
                    rmtree(args.path)
                    os.makedirs(args.path)
            os.chdir(args.path)
    else:
        for check_arg in ('user','datanode','bucket'):
            if eval("args.%s" % check_arg) is None:
                raise ValueError("A %s is needed for S3" % check_arg)

        if args.secret is not None:
            s3connection = boto.connect_s3(args.user,
                                           args.secret,
                                           proxy=args.datanode,
                                           proxy_port=DEFAULT_S3_PORT,
                                           proxy_user=None,
                                           proxy_pass=None,
                                           is_secure=False)
            try:
                s3bucket = s3connection.get_bucket(args.bucket)
            except boto.exception.S3ResponseError,e:
                log.debug("create_test_files.py: Hit exception (%s) %s when opening S3 bucket: %s" % (e.error_code,e,args.bucket))
                if e.error_code == "NoSuchBucket":
                    s3bucket = s3connection.create_bucket(args.bucket)
                else:
                    raise
            # Prepend s3 to the name format so we can differentiate s3 from NFS, etc.
            if not args.format.startswith('s3'):
                args.format = 's3_' + args.format
            log.debug("create_test_files.py: Opened S3 connection for bucket: %s" % s3bucket.name)
        elif args.swift_password is not None:
            swift_client = swiftclient.Connection(SWIFT_AUTH_URL_FMT.format(args.datanode,args.swift_port),args.user,args.swift_password)
            try:
                swift_account = swift_client.get_account()
                if args.bucket in [x['name'] for x in swift_account[-1]]:
                    log.info("Using Swift container: %s" % args.bucket)
                else:
                    raise ValueError("Invalid Swift container: %s" % args.bucket)
            except swiftclient.ClientException,e:
                if e.http_reason == 'Not Found':
                    # A current bug forces adding a default group in order for a user to access the NFS mounted container
                    # For now simply raise an exception so that the user can create the container separate from this interface
                    raise ValueError("Invalid Swift container: %s" % args.bucket)
            # Prepend swift to the name format so we can differentiate swift from NFS, etc.
            if not args.format.startswith('swift'):
                args.format = 'swift_' + args.format
        else:
            raise ValueError("An S3 secret or Swift password is required")

        if not os.path.isdir(args.tmpdir):
            os.makedirs(args.tmpdir)
        elif args.clear_path:
            ans = raw_input("Temporary directory %s exists, clear it? (y/n) " % args.tmpdir)
            if re.match('y(es)*',ans,re.IGNORECASE):
                rmtree(args.tmpdir)
                os.makedirs(args.tmpdir)
        os.chdir(args.tmpdir)

    args.size = human2bytes(args.size)
    if args.lower:
        args.lower = human2bytes(args.lower)

    if args.upper:
        args.upper = human2bytes(args.upper)

    if args.upper > human2bytes('2M'):
        verbose_count = 10
    else:
        verbose_count = 100

    if args.lower is not None:
        size_log = 'lower={:,} upper={:,}'.format(args.lower,args.upper)
    else:
        size_log = 'size={:,}'.format(args.size)
    log.info("Starting test file creation to path: %s count=%d start=%d %s stop_file=%s" % (args.path,args.count,args.number,size_log,args.stop_file))

    total_written = 0
    total_files = 0
    previous_exceptions = []

    if not args.compressible:
        args.format = args.format.replace('.txt','')

    for count in range(0,args.count):
        if os.path.exists(args.stop_file):
            log.warning('Found stop file, exiting...')
            break
        try:
            if args.bucket is None:
                if args.compressible:
                    bytes_written = write_file(args.format % (count + args.number),args.size,log,args.lower,
                                               args.upper,add_content=args.add_content)
                else:
                    bytes_written = write_non_compressible_file(args.format % (count + args.number),args.size,log,args.lower,
                                                                args.upper)
            else:
                if args.compressible:
                    bytes_written = write_file(args.format % (count + args.number),args.size,log,args.lower,
                                               args.upper,add_content=args.add_content)
                else:
                    bytes_written = write_non_compressible_file(args.format % (count + args.number),args.size,log,args.lower,
                                                                args.upper)

                log.debug("create_test_files.py: s3connection=%s s3bucket=%s swift_client=%s" % (s3connection,s3bucket,swift_client))

                if s3connection is not None and s3bucket is not None:
                    if s3bucket.get_key(args.format % (count + args.number)) is None:
                        if args.folder is None:
                            keyname = args.format % (count + args.number)
                        else:
                            keyname = '%s/%s' % (args.folder,args.format % (count + args.number))
                        s3key = s3bucket.new_key(keyname)
                        s3key.set_contents_from_filename(args.format % (count + args.number))
                        bytes_written = s3key.size
                        log.debug("create_test_files.py: Wrote S3 data to bucket %s and key: %s" % (s3bucket.name,s3key.name))
                elif swift_client is not None:
                    if args.folder is None:
                        objectname = args.format % (count + args.number)
                    else:
                        objectname = '%s/%s' % (args.folder,args.format % (count + args.number))
                    try:
                        swift_object = swift_client.head_object(args.bucket,objectname)
                    except swiftclient.ClientException,e:
                        if e.http_reason == 'Not Found':
                            swift_etag = swift_client.put_object(args.bucket,objectname,open(args.format % (count + args.number),'r').read(),
                                                                 headers={'Content-type':'text/plain' if args.compressible else 'application/octet-stream'})
                            swift_object = swift_client.head_object(args.bucket,objectname)
                    bytes_written = int(swift_object['content-length'])
                else:
                    raise ValueError("Invalid API type: None")
                os.unlink(args.format % (count + args.number))
        except Exception,e:
            if args.exit_on_error:
                raise
            previous_exceptions.append(e)
            if len(previous_exceptions) > args.max_errors:
                for e in previous_exceptions:
                    log.warning("Exception during write_file: %s" % e)
                raise previous_exceptions[-1]
            log.error("Got exception on file {0:,}: {1}".format(count,e))
            continue
        previous_exceptions = []
        total_files += 1
        total_written += bytes_written
        if count > 0 and count % verbose_count == 0:
            log.info("Test files created: %d test file: %s and bytes written: %s" % (count,args.format % (count + args.number),
                                                                                     '{:,}'.format(total_written)))
        log.info("Files written: {0:,} wrote file: {1} bytes written: {2:,}".format(count,args.format % (count + args.number),total_written))

    if args.bucket is not None and not args.keep:
        rmtree(args.tmpdir)

    if args.summary:
        log.normal("Total files created: {0:,} total bytes written: {1:,}".format(total_files,total_written))

    log.info('Finished total files created: {0:,} total bytes written: {1:,}'.format(total_files,total_written))