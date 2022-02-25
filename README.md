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
python -m pip install -e . --prefix=~/.local
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

need to add check_log equivalent
