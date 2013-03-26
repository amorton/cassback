cassback
=========

Tool for backing up and restoring Cassandra nodes. Based on the work by https://github.com/synack/tablesnap

Theory of Operation
-------------------

Like Tablesnap, cassback watches for changes to the Cassandra data directory. cassback uses the cross platform [watchdog](http://pythonhosted.org/watchdog/) package to watch for new, renamed and deleted files. 

New, non temporary, files are queued for uploading by an endpoint such as S3 or local disk copy. When a new file is noticed for a Keyspace, or when one is deleted, the manifest of components for the keyspace is updated and stored.

Endpoints are used by Commands and provide an abstraction to store and retrieve files to different locations. 

Installation
------------

cassback provides a Python installer and installs dependencies and registers the `cassback` script.

It can be installed by running

    python setup.py install

Or using pip

    pip install cassback/

For now you also need to create the logging dir `/var/log/cassback`. 

Configuration
-------------

Configuration is via the command line and supports reading a file for command line options. 

The command line provides help for all available commands. 

    $ cassback help
    usage: cassback [-h] [--survey-path SURVEY_PATH] [--meta-dir META_DIR]
                     [--aws-key AWS_KEY] [--aws-secret AWS_SECRET]
                     [--bucket-name BUCKET_NAME] [--key-prefix KEY_PREFIX]
                     [--max-upload-size-mb MAX_UPLOAD_SIZE_MB]
                     [--multipart-chunk-size-mb MULTIPART_CHUNK_SIZE_MB]
                     [--retries RETRIES] [--backup-base BACKUP_BASE]
                     [--endpoint {survey,s3,local}]
                     [--cassandra-version CASSANDRA_VERSION]
                     [--log-level {FATAL,CRITICAL,ERROR,WARN,INFO,DEBUG}]
                     [--log-file LOG_FILE]
                     {restore,help,show,list,purge,survey-report,validate,backup}
                     ...

    cassback - snap, slurp, purge

    optional arguments:
      -h, --help            show this help message and exit
      --endpoint {survey,s3,local}
                            Name of the endpoint to use for backup and restore.
                            (default: local)
      --cassandra-version CASSANDRA_VERSION
                            Cassandra version to backup from or restore to.
                            (default: 1.2.0)
      --log-level {FATAL,CRITICAL,ERROR,WARN,INFO,DEBUG}
                            Logging level. (default: INFO)
      --log-file LOG_FILE   Logging file. (default:
                            /var/log/cassback/cassback.log)
    ...

Help is also available for each command.

    $ cassback help backup
    usage: cassback backup [-h] [--threads THREADS]
                            [--report-interval-secs REPORT_INTERVAL_SECS]
                            [--recursive]
                            [--exclude-keyspace [EXCLUDE_KEYSPACES [EXCLUDE_KEYSPACES ...]]]
                            [--include-system-keyspace] [--ignore-existing]
                            [--ignore-changes]
                            [--cassandra_data_dir CASSANDRA_DATA_DIR]
                            [--host HOST]

    backup SSTables

    optional arguments:
      -h, --help            show this help message and exit
      --threads THREADS     Number of writer threads. (default: 4)
      --report-interval-secs REPORT_INTERVAL_SECS
                            Interval to report on the size of the work queue.
                            (default: 5)
      --recursive           Recursively watch the given path(s)s for new SSTables
                            (default: False)
      --exclude-keyspace [EXCLUDE_KEYSPACES [EXCLUDE_KEYSPACES ...]]
                            User keyspaces to exclude from backup. (default: None)
      --include-system-keyspace
                            Include the system keyspace. (default: False)
      --ignore-existing     Don't backup existing files. (default: False)
      --ignore-changes      Don't watch for file changes, exit immediately.
    ...

Most of the time you will need to specify an endpoint and a command. For example to backup using default settings to a local file.

    cassback --endpoint=local --backup-base=/tmp/cassback backup

Or to backup to S3, including the system keyspaces and using a non default Cassandra data directory. 

    cassback --endpoint s3 --aws-key AWS-KEY --aws-secret AWS-SECRET --bucket-name my-cassandra-backup-bucket backup --include-system --cassandra_data_dir=/mnt/md0/lib/cassandra/data

It's also possible to store command line arguments in a file. For example the call above to backup to S3 can use the s3.conf file

    --endpoint=s3
    --aws-key=AWS-KEY
    --aws-secret=AWS-SECRET
    --bucket-name=my-cassandra-backup-bucket

And then be written as

    cassback @s3.conf backup --include-system --cassandra_data_dir=/mnt/md0/lib/cassandra/data

Questions, Comments, and Help
-----------------------------
Raise issues on the github project or as questions in `#cassandra-ops` on `irc.freenode.net`.

