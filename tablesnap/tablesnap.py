"""Base for commands etc"""

import argparse

class SubCommand(object):
    """Base for all SubCommands that can be called on the command line. 

    :cls:`SubCommand` instances are created by the script entry point and 
    their ``__call__`` method called. 
    """

    command_name = None
    """Command line name for the Sub Command."""

    command_help = None
    """Command line help for the Sub Command."""

    command_description = None
    """Command line description for the Sub Command."""

    @classmethod
    def _common_args(cls):
        """Returns a :cls:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.

        Common Args are used when adding a sub parser for a sub command.

        Sub classes should call this method to ensure the object is 
        correctly created.
        """
        return argparse.ArgumentParser(add_help=False, 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 

        Sub classes should call this method to ensure the sub parser is
        created correctly. The default function for the Arg Parser is 
        set to the constructor for the Sub Command.

        Returns the :cls:`argparser.ArgumentParser`.
        """
        
        parser = sub_parsers.add_parser(cls.command_name,
            parents=[cls._common_args()],
            help=cls.command_help, 
            description=cls.command_description, 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.set_defaults(func=cls)
        return parser

    def __call__(self):
        """Called to execute the Sub Command.
        """
        pass



