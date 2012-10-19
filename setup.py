from setuptools import setup

entry_points = """
[console_scripts]
tablesnap = tablesnap.scripts:tablesnap_main

[tablesnap.sub_commands]
S3SnapSubCommand = tablesnap.s3subcommands:S3SnapSubCommand
LocalSnapSubCommand = tablesnap.localsubcommands:LocalSnapSubCommand
LocalListSubCommand = tablesnap.localsubcommands:LocalListSubCommand
LocalValidateSubCommand = tablesnap.localsubcommands:LocalValidateSubCommand

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
