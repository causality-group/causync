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
import signal

import config as conf


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
            excludes (str|list): string or list of files to exclude
            exclude_from (str): exclude file
            loglevel (str): logging level (see config or help(logging)
            verbose (bool): increase verbosity by one step
            pidfile (str): file containing the process ID

        Attributes:
            pid (int): PID of the current process
            src_abs (list): list containing source directory absolute paths
            dst_abs (str): absolute path of destination directory, joined with date_ival
            curdate (datetime): datetime object containing the current date
            logger (object): the logger object used for logging to file/console
    """

    curdate = None

    def __init__(self, config, src, dst, task, no_incremental=False, quiet=False,
                 dry_run=False, selfname="causync.py", excludes=None, exclude_from=False,
                 loglevel=None, verbose=False, pidfile=None):

        self.config = config
        self.name = selfname
        self.pid = os.getpid()
        if pidfile:
            self.config.PIDFILE = pidfile

        self.no_incremental = no_incremental
        self.excludes = excludes if excludes else []
        if exclude_from:
            self.excludes = self.excludes + CauSync.parse_exclude_file(exclude_from)

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
        self.logger = self.get_logger(loglevel, verbose)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def parse_src(self, src):
        """ Parses source directory arguments into a list. """
        if isinstance(src, list):
            return [os.path.abspath(i) for i in src]
        elif type(src) == str:
            return [os.path.abspath(src)]
        else:
            self.logger.error("source directory type error")
            return False

    def run(self):
        """ Main run function. """

        pidfile_exists = os.path.isfile(self.config.PIDFILE)
        is_running = self.is_running() if self.config.CHECK_PGREP else False

        self.logger.info("started with PID {}".format(self.pid))

        if len(self.src_abs) > 1:
            self.logger.info("syncing multiple source directories")

        if self.dry_run:
            self.logger.info("doing dry run")

        if self.task == 'cleanup':
            try:
                self.create_pidfile()
                self.run_cleanup()
            finally:
                self.remove_pidfile()

        elif self.task in ['check', 'sync']:

            if pidfile_exists or is_running:
                self.logger.info(
                    "causync is already running on {} or the default pidfile exists".format(", ".join(self.src_abs)))
            else:
                self.logger.info("causync is not running yet on {}".format(", ".join(self.src_abs)))
                if self.task == 'sync':
                    try:
                        self.create_pidfile()
                        self.run_sync()
                    finally:
                        self.remove_pidfile()

    def signal_handler(self, signum, frame):
        self.logger.info("received SIGINT ({}, {}), removing PIDFILE".format(signum, frame))
        self.remove_pidfile()

    def create_pidfile(self):
        self.logger.debug("creating pidfile {}".format(self.config.PIDFILE))
        try:
            with open(self.config.PIDFILE, 'w') as f:
                f.write(str(self.pid))
        except IOError as e:
            self.logger.error(e)
            exit(-1)

    def remove_pidfile(self):
        self.logger.debug("removing pidfile {}".format(self.config.PIDFILE))
        try:
            os.remove(self.config.PIDFILE)
        except IOError as e:
            self.logger.error(e)
            exit(-1)

    def run_sync(self):
        """ This is the backup function.
            It is executed when the task argument is 'sync'.
        """

        self.curdate = datetime.now().strftime(self.config.DATE_FORMAT)
        extra_flags = ""

        if self.dry_run:
            extra_flags += " -n "

        if self.excludes:
            for e in self.excludes:
                extra_flags += " --exclude={} ".format(e)

        CauSync.makedirs(self.dst_abs)

        if not self.no_incremental:
            incremental_basedirs = self.find_latest_backups(os.listdir(self.dst_abs),
                                                            self.config.BACKUPS_LINK_DEST_COUNT)
            if incremental_basedirs:
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

        self.logger.debug("rsync command is: {}".format(cmd))
        self.logger.info("syncing {} to {}".format(self.src_abs, dst))

        result = subprocess.check_output(cmd, shell=True).decode()
        self.logger.debug(result)

        self.logger.info("sync finished")

        return result

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
        dirdates = [self.get_dirdate(d) for d in dirnames]
        dirdates.sort(reverse=True)

        # determine list length (if len < delete count, we don't do anything)
        list_len = count if len(dirdates) >= count else len(dirdates)
        # get a reverse sorted list of dirdates (descending by date)
        latest_dates = dirdates[:list_len]
        # convert them to string for rsync --link-dest (example: datetime obj becomes 'YYMMHH' string)
        latest_dates = [i.strftime(self.config.DATE_FORMAT) for i in latest_dates]
        # join each one with the destination directory (example: '/path/dest/sourcedir_YYMMHH'
        latest_dates = [os.path.join(self.dst_abs, i) for i in latest_dates]

        return latest_dates

    def find_old_backups(self, dirnames, ival='daily', count=5):
        """ Returns old backups we should delete.
            The time interval is specified by 'ival'. Values: daily, weekly, monthly, yearly.
        """

        multiplier = timedelta(days=self.config.BACKUP_MULTIPLIERS[ival])

        # extract dates from directory names and sort them
        dirdates = list(sorted([self.get_dirdate(d) for d in dirnames]))

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

    def is_running(self):
        """ Tries to guess (from processlist) whether sync
            for the specific source and destination directory is already running.
        """

        cmd = "pgrep -f '[p]ython[23].*{}.*{}.*{}'".format(self.name, " ".join(self.src), self.dst)

        try:
            result = subprocess.check_output(cmd, shell=True).splitlines()
            # self.logger.debug("result={}".format(result))
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

    def get_logger(self, loglevel=None, verbose=False):
        """ Returns a logger object with the current settings in config.py.
            If the -q (quiet) flag is set, it doesn't log to console.
            If the -v (verbose) flag is set, it increases logging verbosity by one step.
        """

        # please don't change these numbers
        loglevels = {'debug': 10,
                     'info': 20,
                     'warning': 30,
                     'error': 40,
                     'critical': 50}

        if not loglevel:
            loglevel = loglevels[self.config.LOGLEVEL]
        else:
            loglevel = loglevels[loglevel]

        if verbose and loglevel != 10:
            loglevel -= 10

        logger = logging.getLogger()
        logger.setLevel(loglevel)
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

    @staticmethod
    def get_parent_dir(path):
        """ Returns the parent directory of the path argument.
            Example: path '/tmp/causync/test' results in '/tmp/causync'.
        """

        if path == '/':
            return '/'

        path = path.rstrip('/')
        if path == '':
            raise ValueError(path)

        path = os.path.normpath(path)
        path = os.path.dirname(path)

        if '//' in path:
            raise ValueError(path)

        return path

    @staticmethod
    def get_basename(path):
        """ Returns the baseneme of the path argument.
            Example: path '/tmp/causync/test' results in 'test'.
        """

        if path == '/':
            raise ValueError(path)
        path = path.rstrip('/')
        if path == '':
            raise ValueError(path)
        basename = os.path.basename(path)
        if '//' in basename:
            raise ValueError(basename)

        return basename

    @staticmethod
    def makedirs(path):
        """ Recursively creates a directory (mostly used for destination dir). """
        try:
            os.makedirs(path)
        except Exception as e:
            if e.errno == 17 and e.strerror == "File exists":
                pass
            else:
                raise e
        return True

    @staticmethod
    def parse_exclude_file(fname):
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
                        help="set a custom loglevel if 'info' or -v is not enough")

    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help="increase verbosity (by one step)")

    parser.add_argument('-p',
                        '--pidfile',
                        action='store',
                        help='specify pidfile location')

    arguments = parser.parse_args()

    arguments.selfname = sys.argv[0]

    return arguments


if __name__ == "__main__":
    args = parse_args()

    cs = CauSync(conf,
                 args.sources,
                 args.destination,
                 args.task,
                 args.no_incremental,
                 args.quiet,
                 args.dry_run,
                 args.selfname,
                 args.excludes,
                 args.exclude_from,
                 args.loglevel,
                 args.verbose,
                 args.pidfile)
    cs.run()
