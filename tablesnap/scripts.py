"""Entry points for exported scripts."""

import argparse
import logging
import sys
import traceback

import pkg_resources

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
_log = logging.getLogger(__name__)

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

    return main_parser

def tablesnap_main():
    """Script entry point for the command line tool    
    """
    
    args = arg_parser().parse_args()
    
    _log.debug("Got command args %(args)s" % vars())
    try:
        # parsing the args works out which function we want to call.
        sub_command = args.func(args)
        if callable(sub_command):
            out = sub_command()
        else:
            out = sub_command
        _log.debug("Calling sub command %(sub_command)s" % vars())
        sys.stdout.write(out) 
    except (Exception) as exc:
        print "Error:"
        traceback.print_exc()
        sys.exit(1)
    sys.exit(0)

