import copy
import itertools
import logging
import os
import re
import subprocess

from pyjob.config import config
from pyjob.job import Job

_log = logging.getLogger(__name__)

_rarray = re.compile(r'(\d+)-(\d+)(?::(\d+))?')
_rformat = re.compile(r"({\w*})")


_trap_run = """run()
{
  trap 'kill -TERM $pid; wait $pid' INT USR1 USR2 TERM
  $@ &
  pid=$!
  wait $pid
  status=$?
  trap - INT USR1 USR2 TERM
  return $status
}"""


def _str2arr(text):
    """Convert job array definition to Python list and range objects."""
    arrdef = []
    for tok in text.split(','):
        m = _rarray.match(tok)
        if m:
            arrdef.append(range(int(m[1]), 1+int(m[2]), int(m[3] or 1)))
        else:
            arrdef.append(int(tok))
    return arrdef


def _arr2str(arrdef):
    """Convert Python list and range objects to job array definition."""
    if isinstance(arrdef, str):
        return arrdef
    elif isinstance(arrdef, range):
        if arrdef.step == 1:
            return '{}-{}'.format(arrdef.start, arrdef.stop-1)
        else:
            return '{}-{}:{}'.format(arrdef.start, arrdef.stop-1, arrdef.step)
    else:
        try:
            return ','.join([_arr2str(a) for a in arrdef])
        except TypeError:
            return str(arrdef)


def _arr2list(arrdef):
    try:
        flat = (_arr2list(a) for a in arrdef)
        return list(itertools.chain(*flat))
    except TypeError:
        return [arrdef]


def fmt2re(fmt):
    """Convert a format pattern to the inverse regular expression"""
    def ptransform(part):
        if part == '{}':
            return r'(.*)'
        elif part.startswith('{') and part.endswith('}'):
            return r'(?P<{}>.*)'.format(part[1:-1])
        else:
            return part
    return re.compile(''.join([ptransform(p) for p in _rformat.split(fmt)]))


class BatchSystem:
    """Base class for workload managers"""
    JOBSETUP = []
    JOBEND = []
    CMDPRE = ''

    def __init__(self):
        # Make sure we have a valid config section. An empty section
        # is fine as it will just return the defaults from [pyjob]
        if self.platform not in config:
            config[self.platform] = {}

    def submit(self, job, dryrun=False):
        """Submit a job to the Batch System"""
        cfg = config[self.platform]
        opts = dict(cfg)
        opts.update(job.options)
        # Set default stdio name
        default = '{jobid}-{ind}' if 'array' in opts else '{jobid}'
        if 'name' in opts:
            default = '{name}-' + default
        logname = os.path.join(opts.get('logpath', ''),
                               opts.get('logname', default))
        # Replace name now to simplify later logic
        logname = logname.replace('{name}', opts.get('name', ''))
        opts['logname'] = logname

        prolog = self.encode_options(opts)
        prolog += ['#PYJOB setup']
        prolog += [f'export {k}=${v}' for k, v in self.ENVVAR.items()]
        prolog += self.JOBSETUP
        prolog += cfg.get('jobsetup', '').splitlines()

        # Get the exit code from the last command executed
        epilog = ['status=$?']
        epilog += self.JOBEND
        epilog += cfg.get('jobend', '').splitlines()
        epilog += ['[ $status -eq 0 ] && echo DONE>&2 || echo FAIL $status>&2',
                   'exit $status',
                   '']

        script = job.write(self.CMDPRE, prolog, epilog)

        if dryrun:
            print(script)
            return

        bsub = subprocess.run(self.SUBMIT_CMD, input=script, capture_output=True,
                              text=True, check=True)
        match = self.SUBMIT_OUT.match(bsub.stdout)
        if match:
            jobid = match.group('id')
            fname = logname.format(jobid=jobid, ind='arr') + '.shell'
            with open(fname, 'w') as fh:
                fh.write(script)
            return jobid
        else:
            print(script)
            print(bsub.stdout)
            print(bsub.stderr)
            raise Exception

    def parse_script(self, script):
        job = Job.fromfile(script, self.CMDPRE)
        job.host = ''
        hdr = [line for line in job.prolog if line.startswith(self.PREFIX)]
        job.options = self.decode_options(hdr)
        # Split log into path and name
        path, name = os.path.split(job.options['logname'])
        job.options['logpath'] = path
        job.options['logname'] = name

        rname = fmt2re(name)
        m = rname.match(os.path.basename(script)[:-6])
        if m:
            job.id = m.group('jobid')
        else:
            job.id = os.path.basename(script)[:-6]
        # And parse platform specific output in stderr/out
        if 'array' in job.options:
            path = os.path.dirname(script)
            tasks = []
            for ind in _arr2list(job.options['array']):
                name = job.options['logname'].format(jobid=job.id, ind=ind)
                task = copy.copy(job)
                task.ind = ind
                self.parse_log(os.path.join(path, name+'.err'), task)
                tasks.append(task)
            return tasks
        else:
            self.parse_log(script, job)
            return job


class Lsf(BatchSystem):
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


class Slurm(BatchSystem):
    """Slurm workload manager"""

    platform = 'slurm'
    PREFIX = '#SBATCH'
    ENVVAR = {'JOBID': '{SLURM_ARRAY_JOB_ID:-$SLURM_JOB_ID}',
              'JOBINDEX': 'SLURM_ARRAY_TASK_ID'}
    SUBMIT_CMD = 'sbatch'
    SUBMIT_OUT = re.compile(r'^Submitted\sbatch\sjob\s(?P<id>\d+)')
    JOBSETUP = _trap_run.splitlines()
    CMDPRE = 'run'

    def encode_options(self, options):
        hdr = []
        if 'name' in options:
            hdr.append('--job-name={}'.format(options['name']))
        if 'queue' in options:
            hdr.append('-p {}'.format(options['queue']))
        if 'account' in options:
            hdr.append('-A {}'.format(options['account']))
        if 'array' in options:
            hdr.append('-a {}'.format(_arr2str(options['array'])))
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
            elif line.startswith('-a '):
                opts['array'] = _str2arr(line[3:])
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
            if line.startswith('cpu-bind=MASK'):
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
            if 'DUE TO TIME LIMIT' in line or 'Timed out waiting' in line:
                job.result = 'TIMEOUT'
                break
            elif 'DUE TO NODE FAILURE' in line:
                job.result = 'NODEFAIL'
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


def get(name=None):
    """Return an instance of the named Batch System"""

    platform = name or config['pyjob'].get('platform')
    for cls in BatchSystem.__subclasses__():
        if cls.platform == platform:
            return cls()
    else:
        raise Exception('Unknown platform: {}'.format(platform))
