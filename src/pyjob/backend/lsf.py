"""
Backend for using IBM Platform Load Sharing Facility (LSF)

NOTE - this has not been maintained since Jasmin migrated to Slurm
"""
import re
from pyjob.core import BatchSystemBase


class BatchSystem(BatchSystemBase):
    """IBM Platform LSF"""

    platform = 'lsf'
    PREFIX = '#BSUB'
    ENVVAR = {'JOBID': 'LSB_JOBID',
              'JOBINDEX': 'LSB_JOBINDEX'}
    SUBMIT_CMD = 'bsub'
    SUBMIT_OUT = re.compile(r'^Job\s<(?P<id>\d+)>')

    def encode_options(self, options):
        hdr = []
        if 'name' in options:
            hdr.append('-J {}'.format(options['name']))
        if 'queue' in options:
            hdr.append('-q {}'.format(options['queue']))
        if 'runtime' in options:
            hdr.append('-W {}'.format(options['runtime']))
        if 'logname' in options:
            logname = options['logname'].format(jobid='%J', ind='%I')
            hdr.append('-o {}.out'.format(logname))
            hdr.append('-e {}.err'.format(logname))
        if 'memlimit' in options:
            hdr.append('-M {}'.format(options['memlimit']))
            hdr.append('-R rusage[mem={}]'.format(options['memlimit']))
        if 'tmplimit' in options:
            hdr.append('-R rusage[tmp={}]'.format(options['tmplimit']))
        if 'exclude' in options:
            hosts = ['hname!='+host for host in options['exclude'].split()]
            hdr.append('-R "select[{}]"'.format(' && '.join(hosts)))
        return [self.PREFIX + ' ' + line for line in hdr]

    def decode_options(self, hdr):
        raise Exception('Not implemented')
        return {}
