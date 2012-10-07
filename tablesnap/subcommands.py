"""Base for commands"""

import argparse

class SubCommand(object):
    """Base for all SubCommands that can be called on the command line. 

    :cls:`SubCommand` instances are created by the script entry point and 
    their ``__call__`` method called. 
    """

    command_name = None
    """Command line name for the Sub Command.

    Must be specified by sub classes.
    """

    command_help = None
    """Command line help for the Sub Command."""

    command_description = None
    """Command line description for the Sub Command."""

    @classmethod
    def _common_args(cls):
        """Returns a :cls:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.

        Common Args are used when adding a sub parser for a sub command.

        Sub classes may override this method but should pass the call up 
        to ensure the object is correctly created.
        """
        return argparse.ArgumentParser(add_help=False, 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 

        Sub classes may override this method but pass the call up to ensure 
        the sub parser is created correctly. 

        A default ``func`` argument is set on the :cls:``ArgumentParser`` to 
        point to the constructor for the SubCommand class.

        Returns the :cls:`argparser.ArgumentParser`.
        """
        
        assert cls.command_name, "command_name must be set."

        parser = sub_parsers.add_parser(cls.command_name,
            parents=[cls._common_args()],
            help=cls.command_help or "No help", 
            description=cls.command_description or "No help", 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.set_defaults(func=cls)
        return parser

    def __call__(self):
        """Called to execute the SubCommand.
        
        Must be implemented by sub classes.
        """
        raise NotImplementedError()
