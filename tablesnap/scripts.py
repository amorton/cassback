"""Entry points for exported scripts."""

import argparse
import logging
import os.path
import sys
import traceback

import pkg_resources

from tablesnap import cassandra
from tablesnap.endpoints import endpoints

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Sub Commands take the command line args and call the function to do the 
# work. Sub commands are retrieved from the ``tablesnap.sub_commands`` entry 
# point using :mod:`pkg_resources`, see :func:`arg_parser`.
# The ones here are global.

def execute_help(args):
    """Global sub command than prints the help for a sub command.
    """
    temp_parser = arg_parser()
    if args.command:
        temp_parser.parse_args([args.command, "-h"])
    return temp_parser.format_help()
    
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
def arg_parser():
    """Builds a :class:`argparse.ArgumentParser` for the ``tablesnap`` 
    command. 
    
    The ``tablesnap`` script uses a sub command structure, like svn or 
    git. For example::
    
        tablesnap snap-s3 <watch-dir> <bucket-name>
        
    * ``tablesnap`` is the script name. 
    * ``snap-s3`` is the sub command
    * ``watch-dir`` is a positional argument common to all snap commands. 
    * ``bucket-name`` is positional argument for the ``snap-s3`` 
    command.
    """
    
    # This is the main parser that the script entry point uses.
    main_parser = argparse.ArgumentParser(
        description="tablesnap - snap, slurp, purge", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    # say we have sub commands
    sub_parsers = main_parser.add_subparsers(title="Commands", 
        description="Commands to help you snap, slurp, purge.")

    # Start adding sub commands
    # use set_default() to specify a function to call for each sub command.

    # Global / static sub commands
    # Show help for a sub command.
    parser = sub_parsers.add_parser("help", help="Get help.")
    parser.set_defaults(func=execute_help)
    parser.add_argument('command', type=str, default="", nargs="?",
        help='Command to print help for.')
    
    for entry_point in pkg_resources.iter_entry_points(
        "tablesnap.sub_commands"):
        
        # Load the class and add it's parser
        entry_point.load().add_sub_parser(sub_parsers)

    endpoint_names = []
    # Add all of the endpoints
    for entry_point in pkg_resources.iter_entry_points("tablesnap.endpoints"):
        
        # Load the class and add it's parser
        endpoint_class = entry_point.load()
        endpoint_class.add_arg_group(main_parser)
        endpoint_names.append(endpoint_class.name)

    # Global Configuration
    main_parser.add_argument("--endpoint", default="local", 
        choices=endpoint_names, 
        help="Name of the endpoint to use for backup and restore.")

    main_parser.add_argument("--cassandra-version", default="1.2.0", 
        dest="cassandra_version", 
        help="Cassandra version to backup from or restore to.")

    main_parser.add_argument("--log-level", default="INFO", 
        dest="log_level", 
        choices=["FATAL", "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG"],
        help="Logging level.")
    main_parser.add_argument("--log-file", default="./tablesnap.log", 
        dest="log_file", 
        help="Logging file.")

    return main_parser

def tablesnap_main():
    """Script entry point for the command line tool    
    """
    
    args = arg_parser().parse_args()

    logging.basicConfig(filename=os.path.abspath(args.log_file), 
        level=getattr(logging, args.log_level))

    log = logging.getLogger(__name__)
    log.debug("Got command args %(args)s" % vars())
    cassandra.set_version(args.cassandra_version)
    
    try:
        # parsing the args works out which function we want to call.
        sub_command = args.func(args)
        endpoints.validate_args(args)
        
        if callable(sub_command):
            rv, out = sub_command()
        else:
            rv = 0
            out = sub_command
        sys.stdout.write(str(out) + "\n")

    except (Exception) as exc:
        print "Error:"
        traceback.print_exc()
        sys.exit(1)
    sys.exit(rv)

