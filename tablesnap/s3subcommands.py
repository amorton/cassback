import copy
import socket

import boto

import snapsubcommands


class S3SnapConfig(object):

    def __init__(self, bucket_name, aws_key, aws_secret, prefix, 
        host_name, max_upload_size_mb, multipart_chunk_size_mb):

        self.bucket_name = bucket_name
        self.aws_key = key
        self.aws_secret = aws_secret
        self.bucket_name = bucket_name
        self.prefix = self.prefix
        self.host_name = name
        self.max_file_size_mb = self.max_file_size_mb 
        self.chunk_size_mb = self.chunk_size_mb

    @classmethod
    def from_args(cls, args):
        return S3SnapConfig(args.bucket_name, args.aws_key, args.aws_secret, 
            args.prefix, args.host_name or socket.getfqdn(), 
            args.max_upload_size_mb, args.multipart_chunk_size_mb)

class S3SnapSubCommand(snapsubcommands.SnapSubCommand):

    # command description used by the base sub command
    command_name = "snap-s3"
    command_help = "Upload new SSTables to Amazon S3"
    command_description = "Upload new SSTables to Amazon S3"


    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(S3SnapSubCommand, cls).add_sub_parser(sub_parsers)

        parser.add_argument('-k', '--aws-key', dest='aws_key', default=None,
            help="AWS API Key")
        parser.add_argument('-s', '--aws-secret', dest='aws_secret', 
            default=None, help="AWS API Secret Key")
        parser.add_argument('-p', '--prefix', dest='prefix', default=None,
            help='Set a string prefix for uploaded files in S3')
        parser.add_argument('-n', '--host-name', dest='host_name', default=None,
            help="Use this name instead of the FQDN to identify the "\
                "SSTables from this host.")
        parser.add_argument('--max-upload-size-mb', dest='max_upload_size_mb', 
            type=int, default=5120,
            help='Max size for files to be uploaded before doing multipart ')
        parser.add_argument('--multipart-chunk-size-mb', 
            dest='multipart_chunk_size_mb', default=256, type=int,
            help='Chunk size for multipart uploads (10%% of '
            'free memory if default is not available)')

        parser.add_argument('bucket-name',
            help='S3 bucket to upload to.')

        return parser

    def __init__(self, args):
        super(S3SnapSubCommand, self).__init__(args)
        self.s3_config = S3SnapConfig.from_args(args)

    def _create_uploader(self):
        return S3SnapUploader(copy.deepcopy(self.s3_config))


class S3SnapUploader(object):

    def __init__(self, s3_config):
        self.s3_config = s3_config

    def upload(self, file_path):
        pass


