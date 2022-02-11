import os
from configparser import ConfigParser

_defaultrc = u"""
[pyjob]
"""

config = ConfigParser(default_section='pyjob')
config.read_string(_defaultrc)
config.read(os.path.expanduser(os.getenv("PYJOBRC", '~/.pyjobrc')))
config.read('pyjob.ini')
