"""
pyjob
=====

A package for generating and submitting batch jobs to a workload manager (e.g.
LSF, Slurm).

Generates batch templates of the form:

    #!/bin/sh
    {Script Directives  e.g. #SBATCH}

    #PYJOB setup
    {Batch setup including environment variables and cluster specific setup}

    #PYJOB script
    {User defined script to execute on node}

    #PYJOB end
    status=$?
    [ $status -eq 0 ] && echo DONE>&2 || echo FAIL $status>&2
    {Batch job cleanup}
    exit $status

PYJOB setup
-----------
This section includes renaming cluster specific environment variables to:
    JOBID
        Job ID
    JOBINDEX
        Job Array Index

It will also include any user defined setup from the "jobsetup" configuration
option.

PYJOB script
------------
This is the user supplied batch script

PYJOB end
---------
This records the return code from the final user command and prints a simple
DONE / FAIL message to the stderr. It also inludes any user defined shutdown or
cleanup from the "jobend" configuration option.

"""
__author__ = "Owen Embury"

from . import batchsys
from .config import config
from .job import Job


# Try setting the default
if 'platform' in config['pyjob']:
    cluster = batchsys.get()


def use(platform):
    """Set the default batch system"""
    global cluster
    cluster = batchsys.get(platform)
