"""
Dummy backend for running jobs locally via subprocess.run
"""
import subprocess
from pyjob.core import NoBatchSystem


class BatchSystem(NoBatchSystem):
    """Run jobs locally"""

    def submit(self, job, dryrun=False):
        if isinstance(job.command, list) and len(job.command) > 1:
            raise Exception('non trivial jobs not supported by local backend')
        subprocess.run(job.command, shell=True)
