from setuptools import setup

entry_points = """
[console_scripts]
tablesnap = tablesnap.scripts:tablesnap_main

[tablesnap.sub_commands]
BackupSubCommand = tablesnap.subcommands.backup_subcommand:BackupSubCommand
ListSubCommand = tablesnap.subcommands.list_subcommand:ListSubCommand
ValidateSubCommand = tablesnap.subcommands.validate_subcommand:ValidateSubCommand
RestoreSubCommand = tablesnap.subcommands.restore_subcommand:RestoreSubCommand
PurgeSubCommand = tablesnap.subcommands.purge_subcommand:PurgeSubCommand
ShowSubCommand = tablesnap.subcommands.show_subcommand:ShowSubCommand
SurveyReportSubCommand = tablesnap.subcommands.report_subcommand:SurveyReportSubCommand

[tablesnap.endpoints]
LocalEndpoint = tablesnap.endpoints.local_endpoint:LocalEndpoint
S3Endpoint = tablesnap.endpoints.s3_endpoint:S3Endpoint
SurveyEndpoint = tablesnap.endpoints.survey_endpoint:SurveyEndpoint
"""

setup(
    name='tablesnap',
    version='0.7.3',
    author='Jeremy Grosser',
    author_email='jeremy@synack.me',
    packages = ["tablesnap",
                "tablesnap.endpoints", 
                "tablesnap.subcommands"],
    install_requires=[
        'boto>=2.2',
        "watchdog>=0.6.", 
        "pytz>=2012j"
    ],
    entry_points=entry_points
)
