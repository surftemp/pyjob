import argparse
import collections
import cmd
import re
import os

import pyjob

rresub = re.compile(r'_retry(\d+)$')

parse_checklog = argparse.ArgumentParser()
parse_checklog.add_argument('path', help='Log file directory to scan')


def flatten(lst):
    """Flatten a list of lists"""
    for item in lst:
        if isinstance(item, list):
            yield from flatten(item)
        else:
            yield item


def listdirs(path, pattern=''):
    """Return a list containing the names of directories in the specified path.

    This is similar to os.listdir execpt is only returns directories. We always
    append a trailing slash to returns directories to speed up autocomplete."""
    return [os.path.join(d.name, '') for d in os.scandir(path or '.')
            if d.is_dir() and d.name.startswith(pattern)]


def append_retry(path):
    """Append _retryX to the logpath"""
    if path.endswith('/'):
        path = path[:-1]
    m = rresub.search(path)
    if m:
        return path[:m.start()] + '_retry{0:d}'.format(int(m.group(1))+1)
    else:
        return path + '_retry1'


class PyjobShell(cmd.Cmd):
    intro = 'pyjob interactive shell. Type help or ? to list commands\n'
    prompt = 'pyjob>>> '

    def do_exit(self, arg):
        """Quit"""
        return True

    do_EOF = do_exit

    def do_checklog(self, arg):
        """Read the job shell and stderr files in path"""
        parts = arg.split()
        if not parts:
            if hasattr(self, 'logpath'):
                print(f'Log path: {self.logpath}')
            else:
                print('Usage: checklog log_path')
                return
        else:
            self.logpath = parts[0]
            files = [f.path for f in os.scandir(self.logpath) if f.name.endswith('shell')]
            # Array jobs will be returned a list, so flatten possible list-of-lists
            jobs = list(flatten(pyjob.cluster.parse_script(f) for f in files))
            self.jobs_done = [j for j in jobs if j.done]
            self.jobs_fail = [j for j in jobs if not j.done]
            self.results = collections.Counter([j.result for j in self.jobs_fail])
            self.jobopts = {}   # Empty dict for overriding job options
        print(f"{len(self.jobs_done)} completed")
        print("---")
        print(f"{len(self.jobs_fail)} incomplete")
        for r in self.results:
            print(f"{self.results[r]:6d} {r}")

    def complete_checklog(self, text, line, begidx, endidx):
        ipos = line.rfind(' ')
        if ipos < begidx:
            path = line[ipos+1: begidx]
        else:
            path = ''
        return listdirs(path, text)

    def no_log_loaded(self):
        if not hasattr(self, 'results'):
            print('ERROR - load a log directory with checklog first')
            return True

    def do_jobs(self, arg):
        """Show details on failed jobs"""
        if self.no_log_loaded():
            return
        if not self.jobs_fail:
            print('No failed jobs')
            return
        for r in self.results:
            jobs = [j for j in self.jobs_fail if j.result == r]
            print(f"{self.results[r]:6d} {r}")
            for j in jobs:
                print(f'{j.jobid} : {j.command[-1]}')

    def do_host(self, arg):
        """List hosts by job result code: host [result]"""
        if self.no_log_loaded():
            return
        if arg:
            toshow = arg.split()
        else:
            toshow = self.results
        if 'DONE' in toshow:
            toshow.remove('DONE')
            hosts = collections.Counter([i.host for i in self.jobs_done])
            print(f"{len(self.jobs_done):6d} DONE")
            for h in hosts:
                print(f"{hosts[h]:6d} {h}")
        if toshow and not self.jobs_fail:
            print('No failed jobs')
            return
        for r in self.results:
            jobs = [j for j in self.jobs_fail if j.result == r]
            hosts = collections.Counter([i.host for i in jobs])
            print(f"{self.results[r]:6d} {r}")
            for h in hosts:
                print(f"{hosts[h]:6d} {h}")

    def do_cat(self, arg):
        """Print job shell / stderr to screen"""
        if self.no_log_loaded():
            return
        if not arg:
            print('Usage: cat jobid')
            return
        jid = arg.split()[0]
        jobs = [j for j in self.jobs_fail if j.jobid == jid]
        if not jobs:
            jobs = [j for j in self.jobs_done if j.jobid == jid]
        if jobs:
            j = jobs[0]
            print(j)
            print('job stderr:\n' + ''.join(j.stderr))
            print('batch system:\n' + ''.join(j.baterr))
        else:
            print(f'No such job: {jid}')

    def do_setopt(self, arg):
        """Override a job option when resubmitting"""
        opts = arg.split(maxsplit=1)
        if not opts:
            print('Usage: setopt option [value]')
        elif len(opts) == 1:
            if opts[0] in self.jobopts:
                del self.jobopts[opts[0]]
        else:
            self.jobopts[opts[0]] = opts[1]
        print(self.jobopts)

    def do_resub(self, arg):
        """Resubmit failed jobs to cluster system"""
        logpath = append_retry(self.logpath)
        print(f'Creating outputdir {logpath}')
        os.makedirs(logpath, exist_ok=True)
        for j in self.jobs_fail:
            j.options.update(self.jobopts)
            if 'array' in j.options:
                j.options['array'] = j.ind
            j.options['logpath'] = logpath
            pyjob.cluster.submit(j)


def main():
    PyjobShell().cmdloop()


if __name__ == "__main__":
    main()
