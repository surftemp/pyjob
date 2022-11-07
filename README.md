# pyjob

pyjob is a simple Python pacakage for submitting batch jobs and array jobs to a
the Lotus cluster. As such it mainly targets the [slurm platform](https://slurm.schedmd.com/)
though it does have incomplete backends for LSF and immediately running jobs on
the local computer.

When submitting jobs it will ensure that you have a saved copy of the batch script
itself (`.shell`) along with the standard output (`.out`) and error (`.err`) files.
This ensures you can easily check the status of multiple (completed) batch jobs and
resubmit any failures without needing to use complex platform-specific work manager
commands.

# Install

```bash
python -m pip install .
```

To install in developer mode (i.e. so updates to pyjob source files will take
immediate effect without needing to reinstall) use:
```bash
python -m pip install -e .
```

# Getting Started

pyjob reads configuration info for your cluster setup from configuration files:
* `~/.pyjobrc`
* `pyjob.ini` in the current directory

You should create an appropriate set of defaults for your system. For example on
Jasmin you might use:
```ini
[pyjob]
platform = slurm
queue = short-serial
```

## Submitting a job from Python

```python
import pyjob

jobopts = {
    'runtime': '00:10',
    'memlimit': '1000',
    'queue': 'test',
    'name': 'testjob',
    }

job = pyjob.Job('hostname', options=jobopts)
pyjob.cluster.submit(job)

```

## Checking log files

Log files can be checked with the pyjob interactive shell. e.g
```
$ pyjob 
pyjob interactive shell. Type help or ? to list commands

pyjob>>> checklog test/argo-sst_avhrr-n14
314 completed
---
10 incomplete
    10 FAIL 1
```
Currently supported commands are:
* `checklog` read a directory of log files
* `jobs` list failed jobs
* `host` list hosts where failures occured
* `cat <jobid>` show the shell and err file for the specified job
* `setopt <opt> <value>` override job setting (e.g. `setopt memlimit=16000`)
* `resub` resubmit failed jobs


need to add old check_log equivalent
