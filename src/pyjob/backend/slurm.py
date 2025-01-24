"""
Backend for using Slurm Workload Manager
"""
import re
from pyjob.core import BatchSystemBase, trap_run, str2arr, arr2str

rcancel = re.compile(r'slurmstepd:.*JOB (\d+) ON (\w+) CANCELLED.*DUE TO ([\w\s]+)')
due2map = {
    'NODE FAILURE': 'NODEFAIL',
    'TIME LIMIT': 'TIMEOUT',
    }

class BatchSystem(BatchSystemBase):
    """Slurm workload manager"""

    platform = 'slurm'
    PREFIX = '#SBATCH'
    ENVVAR = {'JOBID': '{SLURM_ARRAY_JOB_ID:-$SLURM_JOB_ID}',
              'JOBINDEX': 'SLURM_ARRAY_TASK_ID'}
    SUBMIT_CMD = 'sbatch'
    SUBMIT_OUT = re.compile(r'^Submitted\sbatch\sjob\s(?P<id>\d+)')
    JOBSETUP = trap_run.splitlines()
    CMDPRE = 'run'

    def encode_options(self, options):
        hdr = []
        if 'name' in options:
            hdr.append('--job-name={}'.format(options['name']))
        if 'queue' in options:
            hdr.append('-p {}'.format(options['queue']))
        if 'account' in options:
            hdr.append('-A {}'.format(options['account']))
        if 'qos' in options:
            hdr.append('-q {}'.format(options['qos']))
        if 'array' in options:
            hdr.append('-a {}'.format(arr2str(options['array'])))
        if 'runtime' in options:
            tstr = options['runtime']
            if tstr.count(':') == 1:
                # Slurm interprets "xx:yy" as minutes:seconds so modify
                # string so it becomes "hh:mm:00"
                tstr = tstr + ':00'
            hdr.append('-t {}'.format(tstr))
        if 'logname' in options:
            if 'array' in options:
                logname = options['logname'].format(jobid='%A', ind='%a')
            else:
                logname = options['logname'].format(jobid='%j', ind='%a')
            hdr.append('-o {}.out'.format(logname))
            hdr.append('-e {}.err'.format(logname))
        if 'memlimit' in options:
            hdr.append('--mem={}'.format(options['memlimit']))
        if 'tmplimit' in options:
            hdr.append('--tmp={}'.format(options['tmplimit']))
        if 'exclude' in options:
            hosts = options['exclude'].split()
            hdr.append('--exclude={}'.format(','.join(hosts)))
        return [self.PREFIX + ' ' + line for line in hdr]

    def decode_options(self, hdr):
        opts = {}
        for line in hdr:
            line = line[len(self.PREFIX)+1:]
            if line.startswith('--job-name='):
                opts['name'] = line[11:]
            elif line.startswith('-p '):
                opts['queue'] = line[3:]
            elif line.startswith('-A '):
                opts['account'] = line[3:]
            elif line.startswith('-q '):
                opts['qos'] = line[3:]
            elif line.startswith('-a '):
                opts['array'] = str2arr(line[3:])
            elif line.startswith('-t '):
                opts['runtime'] = line[3:]
            elif line.startswith('-o '):
                opts['logname'] = line[3:-4].replace('%j', '{jobid}').replace('%A', '{jobid}').replace('%a', '{ind}')
            elif line.startswith('--mem='):
                opts['memlimit'] = line[6:]
            elif line.startswith('--tmp='):
                opts['tmplimit'] = line[6:]
            elif line.startswith('--exclude='):
                opts['exclude'] = ' '.join(line[10:].split(','))

        return opts

    def parse_log(self, script, job):
        if script.endswith('.shell'):
            stderr = script[:-6] + '.err'
        else:
            stderr = script
        try:
            with open(stderr) as fh:
                lines = fh.readlines()
        except FileNotFoundError:
            job.done = False
            job.result = 'LOST'
            return

        # Separate stderr into Slurm and job messages
        job.stderr = []
        job.baterr = []
        for line in lines:
            if line.startswith('pyjob:'):
                host = line.partition('host:')[2].split()
                job.host = host[0] if host else ''
            elif line.startswith('cpu-bind=MASK'):
                job.host = line[16:line.index(',')]
            elif line.startswith('srun:') or line.startswith('slurmstepd:'):
                job.baterr.append(line)
            else:
                job.stderr.append(line)

        # Last line in stderr should be DONE or EXIT
        if lines:
            status = lines[-1].strip()
        else:
            status = ''
        if status == 'DONE' or status.startswith('FAIL'):
            job.done = status == 'DONE'
            job.result = status
            job.stderr.pop()
        else:
            # Batch script did not complete
            job.done = False
            job.result = 'UNKNOWN'

        # Check the batch system messages
        if job.baterr:
            job.done = False
            job.result = 'BATCHERR'
        for line in job.baterr:
            m = rcancel.match(line)
            if m:
                job.host = m.group(2)
                result = m.group(3).strip()
                job.result = due2map.get(result, result)
                break
            if 'Timed out waiting' in line:
                job.result = 'TIMEOUT'
                break
            elif 'CANCELLED' in line:
                job.result = 'KILLED'
                break
            elif 'Out Of Memory' in line or 'oom-kill' in line:
                job.result = 'OOMEMORY'
                break
            elif 'Exited with exit code' in line:
                # Job returned an exit code
                job.result = 'ERROR'
                break

        # Job has completed but wrote to stderr
        if job.done and job.stderr:
            job.result = 'ERROR'
