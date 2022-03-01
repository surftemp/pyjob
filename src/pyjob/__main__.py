import argparse
import collections
import cmd
import os

import pyjob


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


class PyjobShell(cmd.Cmd):
    intro = 'pyjob interactive shell. Type help or ? to list commands\n'
    prompt = 'pyjob>>> '

    def do_exit(self, arg):
        """Quit"""
        return True

    do_EOF = do_exit

    def do_checklog(self, arg):
        """Read the job shell and stderr files in path"""
        path = arg
        files = [f.path for f in os.scandir(path) if f.name.endswith('shell')]
        # Array jobs will be returned a list, so flatten possible list-of-lists
        jobs = list(flatten(pyjob.cluster.parse_script(f) for f in files))
        self.jobs_done = [j for j in jobs if j.done]
        self.jobs_fail = [j for j in jobs if not j.done]
        self.results = collections.Counter([j.result for j in self.jobs_fail])
        print(f"{len(self.jobs_done)} completed")
        print("---")
        print(f"{len(self.jobs_fail)} incomplete")

    def complete_checklog(self, text, line, begidx, endidx):
        ipos = line.rfind(' ')
        if ipos < begidx:
            path = line[ipos+1: begidx]
        else:
            path = ''
        return listdirs(path, text)

    def do_jobs(self, arg):
        """Show details on failed jobs"""
        if not getattr(self, 'results', None):
            print('ERROR - load a log directory with checklog first')
            return
        for r in self.results:
            jobs = [j for j in self.jobs_fail if j.result == r]
            print(f"{self.results[r]:6d} {r}")
            for j in jobs:
                print(f'{j.jobid} : {j.command[-1]}')

    def do_host(self, arg):
        """List hosts where job failed"""
        if not getattr(self, 'results', None):
            print('ERROR - load a log directory with checklog first')
            return
        for r in self.results:
            jobs = [j for j in self.jobs_fail if j.result == r]
            hosts = collections.Counter([i.host for i in jobs])
            print(f"{self.results[r]:6d} {r}")
            for h in hosts:
                print(f"{hosts[h]:6d} {h}")


def main():
    PyjobShell().cmdloop()


if __name__ == "__main__":
    main()
