from setuptools import setup

entry_points = """
[console_scripts]
tablesnap = tablesnap:tablesnap_main

[sub_commands]
S3SnapSubCommand = tablesnap.s3subcommands:S3SnapSubCommand
"""

setup(
    name='tablesnap',
    version='0.7.0',
    author='Jeremy Grosser',
    author_email='jeremy@synack.me',
    packages = ["tablesnap",],
    install_requires=[
        'boto>=2.2',
        "watchdog"
    ],
    entry_points=entry_points
)
