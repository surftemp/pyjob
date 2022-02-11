import logging

_log = logging.getLogger(__name__)


def split(s):
    return s.splitlines() if isinstance(s, str) else s


class Job():
    """Base class for cluster jobs

    Jobs comprise two parts: shell script for job setup and running simple programs,
    and a series of tasks (i.e. the main program to run). This split is done so we
    can add appropriate cluster commands to the main tasks - e.g. srun - if they are
    necessary to pass signals / or the cluster to collect apprioriate error codes.
    """

    def __init__(self, cmd, script=[], options={}, env=None):
        """Create a new pyjob.Job instance.

        Parameters:
        -----------
        cmd : str or list
            The main command(s) to run. May be a list or string (which will be
            split into lines)
        script: str or list, optional
            The script setup. Will be executed before the main commands
        options: dict, optional
            A dictionary of cluster options for this job
        env: str, optional
            The shell environment e.g. bash
        """
        self.command = split(cmd)
        self.script = split(script)
        self.options = options
        self.env = env

    def write(self, prefix=None, prolog=[], epilog=[]):
        scr = [self.shebang]
        scr += split(prolog)
        scr += ['#PYJOB script', '']
        scr += self.script
        if prefix:
            scr += (prefix + ' ' + c for c in self.command)
        else:
            scr += self.command
        scr += ['', '#PYJOB end']
        scr += split(epilog)
        return '\n'.join(scr)

    def __str__(self):
        opts = ('#opt {}={}'.format(k, v) for k, v in self.options.items())
        return self.write(prolog=opts)

    @property
    def jobid(self):
        if hasattr(self, 'ind'):
            return f'{self.id}-{self.ind}'
        else:
            return getattr(self, id, 'job')

    @property
    def shebang(self):
        """Return the shebang line for the script"""
        if not self.env:
            return '#!/bin/sh'
        elif self.env.startswith('/'):
            return '#!' + self.env
        else:
            return '#!/usr/bin/env ' + self.env

    @classmethod
    def fromfile(cls, filename, prefix=None):
        """Read job from file. Does not interpret batch system directives which will
        be placed in the prolog / epilog attributes.

        Parameters:
        -----------
        string : str or list
            A string containing the pyjob script

        prefix : str, optional
            Prefix used for main command (as opposed to script setup).
        """
        with open(filename) as fh:
            lines = fh.readlines()
        lines = [line.strip() for line in lines]
        return cls.fromstring(lines, prefix=prefix)

    @classmethod
    def fromstring(cls, string, prefix=None):
        """Return a new Job instance initialized from a string or list. Does not
        interpret batch system directives which will be placed in the prolog /
        epilog attributes.

        Parameters:
        -----------
        string : str or list
            A string containing the pyjob script

        prefix : str, optional
            Prefix used for main command (as opposed to script setup).
        """
        lines = split(string)
        # Extract the shebang
        env = lines.pop(0)
        if env == '#!/bin/sh':
            env = None
        elif env.startswith('#!/usr/bin/env'):
            env = env.split(maxsplit=1)[1]
        elif env.startswith('#!'):
            env = env[2:]
        # Extract script sections
        try:
            i1 = lines.index('#PYJOB script')
            i2 = lines.index('#PYJOB end')
            prolog = lines[:i1]
            epilog = lines[i2+1:]
            while not lines[i1+1]:
                i1 += 1
            while not lines[i2-1]:
                i2 -= 1
            command = lines[i1+1:i2]
        except ValueError:
            prolog = []
            epilog = []
            command = lines
        if prefix:
            pre = [line.startswith(prefix) for line in command]
            try:
                i1 = pre.index(True)
                script = command[:i1]
                command = command[i1:]
                i1 = len(prefix)
                command = [line[i1:].strip() if line.startswith(prefix) else line for line in command]
            except ValueError:
                # Existing script did not use prefix, so assume last line was command
                script = command[:-1]
                command = command[-1]
        else:
            script = []
        job = cls(command, script, env=env)
        job.prolog = prolog
        job.epilog = epilog
        return job
