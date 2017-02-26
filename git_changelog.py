from __future__ import print_function
from collections import defaultdict
import glob
import os
import re
import subprocess

DEBUG = False
GIT_EXEC = "/usr/bin/git"
REPOSITORIES = glob.glob("/ssd/swinbank/src/*")  # Everything in w_2017_8

class Repository(object):
    def __init__(self, path):
        self.path = path

    def __call_git(self, *args):
        to_exec = [GIT_EXEC] + list(args)
        if DEBUG:
            print(to_exec)
        return subprocess.check_output(to_exec, cwd=self.path)

    def commits(self, reachable_from=None, merges_only=False):
        args = ["log", "--pretty=format:%H"]
        if reachable_from:
            args.append(reachable_from)
        if merges_only:
            args.append("--merges")
        return self.__call_git(*args).split()

    def tags(self, pattern=r".*"):
        return [tag for tag in self.__call_git("tag").split()
                if re.search(pattern, tag)]

    def message(self, commit_hash):
        return self.__call_git("show", commit_hash, "--pretty=format:%s")

    @staticmethod
    def ticket(message):
        try:
            return re.search(r"(DM-\d+)", message, re.IGNORECASE).group(1)
        except AttributeError:
            if DEBUG:
                print(message)


def format_output(changelog):
    # Ew, needs a proper templating engine
    print("<html>")
    print("<body>")
    print("<h1>LSST Changelog</h1>")

    for tag in sorted(changelog, reverse=True):
        print("<h2>New in {}</h2>".format(tag))
        print("<ul>")
        for ticket in changelog[tag]:
            print("<li><a href=https://jira.lsstcorp.org/browse/{ticket}>{ticket}</a> [{pkgs}]</li>".format(ticket=ticket, pkgs=", ".join(changelog[tag][ticket])))
        print("</ul>")
    print("</body>")
    print("</html>")


def generate_changelog(repositories):
    # Dict of tag -> ticket -> affected packages
    changelog =  defaultdict(lambda: defaultdict(set))
    for repository in repositories:
        if DEBUG:
            print(repository)
        r = Repository(repository)
        tags = sorted(r.tags("w\.\d{4}"), reverse=True)
        for newtag, oldtag in zip(tags, tags[1:]):
            merges = (set(r.commits(newtag, merges_only=True)) -
                      set(r.commits(oldtag, merges_only=True)))

            for sha in merges:
                changelog[newtag][r.ticket(r.message(sha))].add(os.path.basename(repository))


if __name__ == "__main__":
    changelog = generate_changelog(REPOSITORIES)
    format_output(changelog)
