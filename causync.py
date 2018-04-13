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
            selfname (string): the program's name, should be copied from sys.argv[0], used by is_running()

        Attributes:
            pid (int): PID of the current process
            src_abs (string): absolute path of source directory
            dst_abs (string): absolute path of destination directory, joined with date_ival
            logger (object): the logger object used for logging to file/console
    """
    def __init__(self, config, src, dst, task, no_incremental=False, quiet=False, selfname="causync.py"):
        self.config = config
        self.name = selfname
        self.pid = os.getpid()
        self.no_incremental = no_incremental

        self.src = src
        self.src_abs = os.path.abspath(self.src)
        self.dst = dst
        self.dst_abs = os.path.abspath(dst)

        self.task = task
        self.quiet = quiet
        self.logger = self.get_logger()

    def run(self):
        """ Main run function. """

        is_running = self.is_running()
        lockfile_exists = self.lockfile_exists()

        self.logger.info("started with PID {}".format(self.pid))

        if self.task == 'cleanup':
            self.run_cleanup()
        elif self.task in ['check', 'sync']:
            if lockfile_exists or is_running:
                self.logger.info("causync is already running on {}".format(self.src_abs))
            else:
                self.logger.info("causync is not already running on {}".format(self.src_abs))
                if self.task == 'sync':
                    self.run_sync()

    def run_sync(self):
        """ This is the backup function.
            It is executed when the task argument is 'sync'.
        """

        curdate = datetime.now().strftime(config.DATE_FORMAT)
        extra_flags = ""

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

        dst = os.path.abspath(os.path.join(self.dst_abs, curdate))
        cmd = "rsync {f} {ef} {src} {dst}".format(f=self.config.RSYNC_FLAGS,
                                                  ef=extra_flags,
                                                  src=self.src_abs,
                                                  dst=dst)

        self.logger.debug("rsync cmd = {}".format(cmd))

        self.logger.info("syncing {} to {}".format(self.src_abs, dst))
        self.create_lockfile()

        result = subprocess.check_output(cmd, shell=True).decode()
        self.logger.debug(result)

        self.remove_lockfile()
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

    def get_lockfile_path(self):
        """ Returns the lockfile path, depends on the source directory. """

        src_parent_dir = self.get_parent_dir(self.src_abs)
        src_basedir = self.get_basename(self.src_abs)
        src_basedir = "{}.lock".format(src_basedir)
        return os.path.join(src_parent_dir, src_basedir)

    def get_dirdate(self, dirname):
        """ Returns the date extracted from a backup directory name.
            Example: 'causync_180410_111237' results in a datetime object for '18-04-10 11:12:37'
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
            The time interval is specified by 'ival'. Examples: daily, weekly, monthly, yearly.
        """

        curdate = datetime.now()
        multiplier = timedelta(days=self.config.BACKUP_MULTIPLIERS[ival])

        # extract dates from directory names and sort them
        dirdates = list(sorted([ self.get_dirdate(d) for d in dirnames ]))

        (keep, delete) = (list(), list())

        for d in dirdates:
            m = multiplier * count + multiplier
            keepdate = curdate - m

            if ival == 'yearly' and d.day != 1 and d.month != 1:
                continue
            elif ival == 'monthly' and d.day != 1:
                continue
            elif ival == 'weekly' and d.weekday() != 0:
                continue

            if d > keepdate:
                keep.append(d)
            else:
                delete.append(d)

        delete.sort(reverse=True)
        dirdates.sort(reverse=True)
        keep.sort(reverse=True)

        return keep, delete

    def run_cleanup(self):
        """ Deletes old backups.
            You can set how many you want to keep for each date/time interval in config.py.
            This function is executed when the task argument is 'cleanup'.
        """

        listdir = os.listdir(self.dst_abs)

        yearly = self.find_old_backups(listdir, 'yearly', self.config.BACKUPS_TO_KEEP['yearly'])
        monthly = self.find_old_backups(listdir, 'monthly', self.config.BACKUPS_TO_KEEP['monthly'])
        weekly = self.find_old_backups(listdir, 'weekly', self.config.BACKUPS_TO_KEEP['weekly'])
        daily = self.find_old_backups(listdir, 'daily', self.config.BACKUPS_TO_KEEP['daily'])

        monthly = [monthly[0], set(monthly[1]) - set(yearly[0])]
        weekly = [weekly[0], set(weekly[1]) - set(yearly[0]) - set(monthly[0])]
        daily = [daily[0], set(daily[1]) - set(monthly[0]) - set(weekly[0]) - set(yearly[0])]

        self.rmtree(list(sorted(yearly[1])))
        self.rmtree(list(sorted(monthly[1])))
        self.rmtree(list(sorted(weekly[1])))
        self.rmtree(list(sorted(daily[1])))

        self.logger.info("successfully deleted old backups")

    def rmtree(self, dirnames):
        for d in dirnames:
            try:
                path = os.path.join(self.dst_abs, d.strftime(self.config.DATE_FORMAT))
                rmtree(path)
                self.logger.debug("removed {}".format(path))
            except FileNotFoundError:
                pass


    def create_lockfile(self):
        """ Creates a lockfile at sync source directory. """

        lockfile = self.get_lockfile_path()
        self.logger.debug("creating lockfile {}".format(lockfile))

        try:
            open(lockfile, 'a').close()
            return True

        except IOError as e:
            self.logger.error(e)

    def remove_lockfile(self):
        """ Creates an existing lockfile at sync source directory. """

        lockfile = self.get_lockfile_path()
        self.logger.debug("removing lockfile {}".format(lockfile))

        try:
            os.remove(lockfile)
            return True

        except IOError as e:
            self.logger.error(e)

    def lockfile_exists(self):
        """ Checks if lockfile exists at sync source directory. """

        lockfile = self.get_lockfile_path()
        if os.path.isfile(lockfile):
            return True
        return False

    def is_running(self):
        """ Tries to guess (from processlist) whether sync
            for the specific source and destination directory is already running.
        """

        cmd = "pgrep -f '[p]ython3.*{}.*{}.*{}'".format(self.name, self.src, self.dst)

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

    def get_logger(self):
        """ Returns a logger object with the current settings in config.py.
            If the -q (quiet) flag is set, it doesn't log to console.
        """

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


def parse_args():
    """ Parses command-line arguments and
        sets a few variables depending on the 'task' argument.
    """
    parser = ArgumentParser(description="Causality backup solution")

    parser.add_argument('task', choices=['check', 'sync', 'cleanup'], help='task to execute')

    parser.add_argument('source', help='sync source directory')

    parser.add_argument('destination', help='sync destination directory')

    parser.add_argument('--no-incremental',
                        dest='no_incremental',
                        action='store_true',
                        default=False,
                        help="don't do incremental backups")

    parser.add_argument('-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help="don't print to console")

    arguments = parser.parse_args()

    arguments.selfname = sys.argv[0]

    return arguments

if __name__ == "__main__":
    args = parse_args()

    cs = CauSync(config,
                 args.source,
                 args.destination,
                 args.task,
                 args.no_incremental,
                 args.quiet,
                 args.selfname)
    cs.run()
