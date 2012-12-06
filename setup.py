from setuptools import setup

entry_points = """
[console_scripts]
tablesnap = tablesnap.scripts:tablesnap_main

[tablesnap.sub_commands]
SnapSubCommand = tablesnap.subcommands:SnapSubCommand
ListSubCommand = tablesnap.subcommands:ListSubCommand
ValidateSubCommand = tablesnap.subcommands:ValidateSubCommand
SlurpSubCommand = tablesnap.subcommands:SlurpSubCommand
PurgeSubCommand = tablesnap.subcommands:PurgeSubCommand
ShowSubCommand = tablesnap.subcommands:ShowSubCommand
SurveyReportSubCommand = tablesnap.subcommands:SurveyReportSubCommand


[tablesnap.endpoints]
LocalEndpoint = tablesnap.endpoints:LocalEndpoint
S3Endpoint = tablesnap.endpoints:S3Endpoint
SurveyEndpoint = tablesnap.endpoints:SurveyEndpoint
"""

setup(
    name='tablesnap',
    version='0.7.0',
    author='Jeremy Grosser',
    author_email='jeremy@synack.me',
    packages = ["tablesnap",],
    install_requires=[
        'boto>=2.2',
        "watchdog>=0.6."
    ],
    entry_points=entry_points
)
