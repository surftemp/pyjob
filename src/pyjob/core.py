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


trap_run = """run()
{
  trap 'kill -TERM $pid; wait $pid' INT USR1 USR2 TERM
  $@ &
  pid=$!
  wait $pid
  status=$?
  trap - INT USR1 USR2 TERM
  return $status
}"""


def str2arr(text):
    """Convert job array definition to Python list and range objects."""
    arrdef = []
    for tok in text.split(','):
        m = _rarray.match(tok)
        if m:
            arrdef.append(range(int(m[1]), 1+int(m[2]), int(m[3] or 1)))
        else:
            arrdef.append(int(tok))
    return arrdef


def arr2str(arrdef):
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
            return ','.join([arr2str(a) for a in arrdef])
        except TypeError:
            return str(arrdef)


def arr2list(arrdef):
    try:
        flat = (arr2list(a) for a in arrdef)
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


class BatchSystemBase:
    """Base class for workload managers"""
    JOBSETUP = []
    JOBEND = []
    CMDPRE = ''

    def __init__(self):
        # Make sure we have a valid config section. An empty section
        # is fine as it will just return the defaults from [pyjob]
        if self.platform not in config:
            config[self.platform] = {}

    def write_script(self, job):
        """Submit a job to the Batch System"""
        cfg = config[self.platform]
        opts = dict(cfg)
        opts.update(job.options)
        # Set default stdio name
        default = ('{name}-' if 'name' in opts else '') + '{jobid}' + \
                  ('-{ind}' if 'array' in opts else '')
        logname = os.path.join(opts.get('logpath', ''), opts.get('logname', default))
        # Replace name now to simplify later logic
        logname = logname.replace('{name}', opts.get('name', 'job'))
        opts['logname'] = logname
        job.stdoutname = logname

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

        return job.write(self.CMDPRE, prolog, epilog)

    def submit(self, job, dryrun=False):
        """Submit a job to the Batch System"""
        script = self.write_script(job)
        if dryrun:
            print(script)
            return

        bsub = subprocess.run(self.SUBMIT_CMD, input=script, capture_output=True,
                              text=True)
        match = self.SUBMIT_OUT.match(bsub.stdout)
        if bsub.returncode == 0 and match:
            jobid = match.group('id')
            fname = job.stdoutname.format(jobid=jobid, ind='arr') + '.shell'
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
            for ind in arr2list(job.options['array']):
                name = job.options['logname'].format(jobid=job.id, ind=ind)
                task = copy.copy(job)
                task.ind = ind
                self.parse_log(os.path.join(path, name+'.err'), task)
                tasks.append(task)
            return tasks
        else:
            self.parse_log(script, job)
            return job


class NoBatchSystem(BatchSystemBase):
    """Dummy class for when we don't have a batch system"""

    platform = 'pyjob'
    PREFIX = '#BATCH'
    ENVVAR = {}

    def submit(self, job, dryrun=False):
        script = self.write_script(job)
        print(script)

    def encode_options(self, options):
        return [f'{self.PREFIX} {i[0]} {i[1]}' for i in options.items()]
