#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Rsync wrapper for CausalityGroup """

import logging
import os
from shutil import rmtree
import sys
from argparse import ArgumentParser
import subprocess
from datetime import datetime, timedelta

import config

class CauSync(object):
    """ CauSync object for sync-related functions.

        Args:
            config (module): config module is passed to CauSync here
            src (str): backup source directory, this is backed up to the destination
            dst (str): backup destination directory, this is where source is backed up to
            task (str): contains the task to execute
            no_incremental (bool): if True, CauSync doesn't do incremental backups
            quiet (bool): if set to True, CauSync doesn't print anything to console
            dry_run (bool): same as rsync's -n argument, does a dry run
            selfname (str): the program's name, should be copied from sys.argv[0], used by is_running()
            excludes(str|list): string or list of files to exclude
            exclude_from(str): exclude file
            loglevel(int): logging level (see config or help(logging)

        Attributes:
            pid (int): PID of the current process
            src_abs (list): list containing source directory absolute paths
            dst_abs (str): absolute path of destination directory, joined with date_ival
            curdate (datetime): datetime object containing the current date
            logger (object): the logger object used for logging to file/console
    """

    curdate = None
    multiple_sources = False

    def __init__(self, config, src, dst, task, no_incremental=False, quiet=False,
                 dry_run=False, selfname="causync.py", excludes=None, exclude_from=False,
                 loglevel=None):
        self.config = config
        self.name = selfname
        self.pid = os.getpid()
        self.no_incremental = no_incremental
        self.excludes = excludes if excludes else []
        if exclude_from:
            self.excludes = self.excludes + self.parse_exclude_file(exclude_from)

        self.src = src
        self.src_abs = self.parse_src(self.src)
        if not self.src_abs:
            exit(-1)

        self.dst = dst
        self.dst_abs = os.path.abspath(dst)

        self.task = task
        self.quiet = quiet
        self.dry_run = dry_run
        self.curdate = datetime.now()
        self.logger = self.get_logger(loglevel)

    def parse_src(self, src):
        if type(src) == type(list()):
            self.multiple_sources = True if len(src) > 1 else False
            return [os.path.abspath(i) for i in src]
        elif type(self.src) == str:
            return [os.path.abspath(self.src)]
        else:
            self.logger.error("source directory type error")
            return False

    def run(self):
        """ Main run function. """

        is_running = self.is_running()
        lockfile_exists = self.check_lockfiles()

        self.logger.info("started with PID {}".format(self.pid))
        if self.multiple_sources:
            self.logger.info("syncing multiple source directories")

        if self.dry_run:
            self.logger.info("doing dry run")
        if self.task == 'cleanup':
            self.run_cleanup()
        elif self.task in ['check', 'sync']:
            if lockfile_exists or is_running:
                self.logger.info("causync is already running on {}".format(", ".join(self.src_abs)))
            else:
                self.logger.info("causync is not already running on {}".format(", ".join(self.src_abs)))
                if self.task == 'sync':
                    self.run_sync()

    def run_sync(self):
        """ This is the backup function.
            It is executed when the task argument is 'sync'.
        """

        self.curdate = datetime.now().strftime(config.DATE_FORMAT)
        extra_flags = ""

        if self.dry_run:
            extra_flags += " -n "

        if self.excludes:
            for e in self.excludes:
                extra_flags += " --exclude={} ".format(e)

        self.makedirs(self.dst_abs)

        if not self.no_incremental:
            incremental_basedirs = self.find_latest_backups(os.listdir(self.dst_abs), self.config.BACKUPS_LINK_DEST_COUNT)
            if incremental_basedirs != []:
                self.logger.debug("inc_basedirs={}".format(incremental_basedirs))
                self.logger.info("found incremental basedirs, using them in --link-dest")

                for basedir in incremental_basedirs:
                    extra_flags += " --link-dest={} ".format(basedir)
            else:
                self.logger.info("incremental basedirs not found, skipping --link-dest")

        dst = os.path.abspath(os.path.join(self.dst_abs, self.curdate))
        cmd = "rsync {f} {ef} {src} {dst}".format(f=self.config.RSYNC_FLAGS,
                                                  ef=extra_flags,
                                                  src=" ".join(self.src_abs),
                                                  dst=dst)

        self.logger.debug("rsync cmd = {}".format(cmd))

        self.logger.info("syncing {} to {}".format(self.src_abs, dst))
        self.create_lockfiles()

        result = subprocess.check_output(cmd, shell=True).decode()
        self.logger.debug(result)

        self.remove_lockfiles()
        self.logger.info("sync finished")

        return result

    def makedirs(self, path):
        """ Recursively creates a directory (mostly used for destination dir). """
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except OSError as e:
            self.logger.error(e)
            exit(1)

    def get_parent_dir(self, path):
        """ Returns the parent directory of the path argument.
            Example: path '/tmp/causync/test' results in '/tmp/causync'.
        """

        if path == '/': return '/'
        path = path.rstrip('/')
        if path == '': raise ValueError(path)
        path = os.path.normpath(path)
        path = os.path.dirname(path)
        if '//' in path: raise ValueError(path)

        return path

    def get_basename(self, path):
        """ Returns the baseneme of the path argument.
            Example: path '/tmp/causync/test' results in 'test'.
        """

        if path == '/': raise ValueError(path)
        path = path.rstrip('/')
        if path == '': raise ValueError(path)
        basename = os.path.basename(path)
        if '//' in basename: raise ValueError(basename)

        return basename

    def check_lockfiles(self):
        """ check if logfiles exist for all source directories """
        result = False
        for path in self.src_abs:
            result = True if self.lockfile_exists(path) else False
        return result

    def get_lockfile_path(self, path):
        """ Returns the lockfile path, depends on the source directory. """

        src_parent_dir = self.get_parent_dir(path)
        src_basedir = self.get_basename(path)
        src_basedir = "{}.lock".format(src_basedir)
        return os.path.join(src_parent_dir, src_basedir)

    def get_dirdate(self, dirname):
        """ Returns the date extracted from a backup directory name.
            Example: '180410_111237' results in a datetime object for '18-04-10 11:12:37'
            (if this is the date format in config.py)
        """

        try:
            if isinstance(dirname, datetime):
                return dirname
            dirdate = datetime.strptime(dirname, self.config.DATE_FORMAT)
            return dirdate
        except ValueError as e:
            self.logger.error(e)
            exit(1)

    def find_latest_backups(self, dirnames, count=5):
        """ Returns the latest daily backup directory names. """

        if len(dirnames) == 0:
            return []

        # extract dates from directory names and sort them
        dirdates = [ self.get_dirdate(d) for d in dirnames ]
        dirdates.sort(reverse=True)

        # determine list length (if len < delete count, we don't do anything)
        list_len = count if len(dirdates) >= count else len(dirdates)
        # get a reverse sorted list of dirdates (descending by date)
        latest_dates = dirdates[:list_len]
        # convert them to string for rsync --link-dest (example: datetime obj becomes 'YYMMHH' string)
        latest_dates = [ i.strftime(config.DATE_FORMAT) for i in latest_dates ]
        # join each one with the destination directory (example: '/path/dest/sourcedir_YYMMHH'
        latest_dates = [ os.path.join(self.dst_abs, i) for i in latest_dates ]

        return latest_dates

    def find_old_backups(self, dirnames, ival='daily', count=5):
        """ Returns old backups we should delete.
            The time interval is specified by 'ival'. Values: daily, weekly, monthly, yearly.
        """

        multiplier = timedelta(days=self.config.BACKUP_MULTIPLIERS[ival])

        # extract dates from directory names and sort them
        dirdates = list(sorted([ self.get_dirdate(d) for d in dirnames ]))

        (keep, delete) = (list(), list())

        for d in dirdates:
            m = multiplier * count
            keepdate = self.curdate - m - multiplier

            if ival == 'yearly' and (d.day != 1 or d.month != 1):
                continue
            elif ival == 'monthly' and d.day != 1:
                continue
            elif ival == 'weekly' and d.weekday() != 0:
                continue
            else:
                if d > keepdate:
                    keep.append(d)
                else:
                    delete.append(d)

        delete.sort(reverse=True)
        keep.sort(reverse=True)

        return keep, delete

    def run_cleanup(self):
        """ Deletes old backups.
            You can set how many you want to keep for each date/time interval in config.py.
            This function is executed when the task argument is 'cleanup'.
        """

        listdir = os.listdir(self.dst_abs)

        # find_old_backups() works like this:
        # yearly[0] values are yearly dates we keep AND all backups after that
        #   (date > now - KEEP_COUNT)
        #   This results in yearly keep list + the rest of the dates (monthly + weekly + daily)
        # yearly[1] contains old yearly backups we should delete
        #   (date < now - KEEP_COUNT)
        yearly = self.find_old_backups(listdir, 'yearly', self.config.BACKUPS_TO_KEEP['yearly'])
        # monthly[0] contains monthly dates we keep AND all backups after that: weekly, daily
        # monthly[1] contains dates that are less than what we keep, INCLUDING yearly
        monthly = self.find_old_backups(listdir, 'monthly', self.config.BACKUPS_TO_KEEP['monthly'])
        # same logic as above
        weekly = self.find_old_backups(listdir, 'weekly', self.config.BACKUPS_TO_KEEP['weekly'])
        daily = self.find_old_backups(listdir, 'daily', self.config.BACKUPS_TO_KEEP['daily'])

        # since monthly[1] also contains yearly dates we want to keep,
        #   we subtract these, the result is a correct monthly delete list
        monthly = [monthly[0], set(monthly[1]) - set(yearly[0])]
        # same logic as above, weekly[1] contains yearly and monthly,
        #   because their date value is less than weekly
        weekly = [weekly[0], set(weekly[1]) - set(yearly[0]) - set(monthly[0])]
        daily = [daily[0], set(daily[1]) - set(monthly[0]) - set(weekly[0]) - set(yearly[0])]

        d = set(list(yearly[1]) + list(monthly[1]) + list(weekly[1]) + list(daily[1]))
        self.rmtree(sorted(d))

        self.logger.info("successfully deleted old backups")

    def rmtree(self, dirnames):
        """ This is actually a wrapper for shutil.rmtree. """
        for d in dirnames:
            try:
                path = os.path.join(self.dst_abs, d.strftime(self.config.DATE_FORMAT))
                if not self.dry_run:
                    rmtree(path)
                self.logger.debug("removed {}".format(path))
            except FileNotFoundError:
                pass

    def create_lockfile(self, path):
        """ Creates a lockfile at the specified path. """

        lockfile = self.get_lockfile_path(path)
        self.logger.debug("creating lockfile {}".format(lockfile))

        try:
            open(lockfile, 'a').close()
            return True

        except IOError as e:
            self.logger.error(e)

    def create_lockfiles(self):
        """ Creates lockfiles at sync source directories. """
        for path in self.src_abs:
            self.create_lockfile(path)
        return True

    def remove_lockfile(self, path):
        """ Removes an existing lockfile at the specified path. """

        lockfile = self.get_lockfile_path(path)
        self.logger.debug("removing lockfile {}".format(lockfile))

        try:
            os.remove(lockfile)
            return True

        except IOError as e:
            self.logger.error(e)

    def remove_lockfiles(self):
        """ Removes lockfiles related to sync source directories. """
        for path in self.src_abs:
            self.remove_lockfile(path)
        return True

    def lockfile_exists(self, path):
        """ Checks if lockfile exists at the specified path. """

        lockfile = self.get_lockfile_path(path)
        if os.path.isfile(lockfile):
            return True
        return False

    def is_running(self):
        """ Tries to guess (from processlist) whether sync
            for the specific source and destination directory is already running.
        """

        cmd = "pgrep -f '[p]ython3.*{}.*{}.*{}'".format(self.name, " ".join(self.src), self.dst)

        try:
            result = subprocess.check_output(cmd, shell=True).splitlines()
            #self.logger.debug("result={}".format(result))
            self.logger.debug("is_running() lines: {}".format(result))

            if len(result) > 1:
                return True
            return False

        except subprocess.CalledProcessError as e:
            # pgrep returns with 1 when there are no results
            if e.returncode == 1:
                return False
            self.logger.debug(("command '{}' returned with error "
                               "(code {}): {}").format(e.cmd, e.returncode, e.output))

    def get_logger(self, loglevel=None):
        """ Returns a logger object with the current settings in config.py.
            If the -q (quiet) flag is set, it doesn't log to console.
        """

        # please don't change these numbers
        LOGLEVELS = {'debug': 10,
                     'info': 20,
                     'warning': 30,
                     'error': 40,
                     'critical': 50}

        if loglevel:
            self.config.LOGLEVEL = LOGLEVELS[loglevel]

        logger = logging.getLogger()
        logger.setLevel(self.config.LOGLEVEL)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

        # logfile handler
        lf_handler = logging.FileHandler(self.config.LOGFILE)
        lf_handler.setFormatter(formatter)
        logger.addHandler(lf_handler)

        if not self.quiet:
            # console handler
            c_handler = logging.StreamHandler()
            c_handler.setFormatter(formatter)
            logger.addHandler(c_handler)

        return logger

    def parse_exclude_file(self, fname):
        """ Read exclude file and return a list of paths """
        excludes = list()

        with open(fname, 'r') as f:
            for line in f.readlines():
                line = line.rstrip('\n')
                if line != '':
                    excludes.append(line)

        return excludes



def parse_args():
    """ Parses command-line arguments and
        sets a few variables depending on the 'task' argument.
    """
    parser = ArgumentParser(description="Causality backup solution")

    parser.add_argument('task', choices=['check', 'sync', 'cleanup'], help='task to execute')

    parser.add_argument('sources',
                        metavar='sources',
                        type=str,
                        nargs='+',
                        help='sync source directory')

    parser.add_argument('destination', help='sync destination directory')

    parser.add_argument('--no-incremental',
                        dest='no_incremental',
                        action='store_true',
                        default=False,
                        help="don't do incremental backups")

    parser.add_argument('--exclude',
                        dest='excludes',
                        nargs='+',
                        help="exclude files matching PATTERN")

    parser.add_argument('--exclude-from',
                        dest='exclude_from',
                        default=False,
                        help="read exclude patterns from a FILE")

    parser.add_argument('-n',
                        '--dry-run',
                        dest='dry_run',
                        action='store_true',
                        default=False,
                        help="do a dry run")

    parser.add_argument('-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help="don't print to console")

    parser.add_argument('--loglevel',
                        action='store',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        default=None,
                        help="don't print to console")

    arguments = parser.parse_args()

    arguments.selfname = sys.argv[0]

    return arguments

if __name__ == "__main__":
    args = parse_args()

    cs = CauSync(config,
                 args.sources,
                 args.destination,
                 args.task,
                 args.no_incremental,
                 args.quiet,
                 args.dry_run,
                 args.selfname,
                 args.excludes,
                 args.exclude_from,
                 args.loglevel)
    cs.run()
