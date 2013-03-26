from setuptools import setup

entry_points = """
[console_scripts]
cassback = cassback.scripts:cassback_main

[cassback.sub_commands]
BackupSubCommand = cassback.subcommands.backup_subcommand:BackupSubCommand
ListSubCommand = cassback.subcommands.list_subcommand:ListSubCommand
ValidateSubCommand = cassback.subcommands.validate_subcommand:ValidateSubCommand
RestoreSubCommand = cassback.subcommands.restore_subcommand:RestoreSubCommand
PurgeSubCommand = cassback.subcommands.purge_subcommand:PurgeSubCommand
ShowSubCommand = cassback.subcommands.show_subcommand:ShowSubCommand
SurveyReportSubCommand = cassback.subcommands.report_subcommand:SurveyReportSubCommand

[cassback.endpoints]
LocalEndpoint = cassback.endpoints.local_endpoint:LocalEndpoint
S3Endpoint = cassback.endpoints.s3_endpoint:S3Endpoint
SurveyEndpoint = cassback.endpoints.survey_endpoint:SurveyEndpoint
"""

setup(
    name='cassback',
    version='0.1.1',
    author='Aaron Morton',
    author_email='aaron@the-mortons.org',
    packages = ["cassback",
                "cassback.endpoints", 
                "cassback.subcommands"],
    install_requires=[
        'boto>=2.2',
        "watchdog>=0.6.", 
        "pytz>=2012j"
    ],
    entry_points=entry_points
)
