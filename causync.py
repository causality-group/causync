#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Rsync wrapper for CausalityGroup """

import logging
import os
from shutil import rmtree
import sys
from argparse import ArgumentParser
import subprocess
from datetime import datetime

import config

class CauSync(object):
    """ CauSync object for sync-related functions.

        Args:
            config (module): config module is passed to CauSync here
            src (str): backup source directory, this is backed up to the destination
            dst (str): backup destination directory, this is where source is backed up to
            checkonly (bool): if True, CauSync only check if it is running on the specified directories (source, dest)
            cleanup (bool): if True, CauSync deletes old backups - see run_cleanup()
            no_incremental (bool): if True, CauSync doesn't do incremental backups
            date_ival (string): select date interval for backups (daily, monthly, etc)
            quiet (bool): if set to True, CauSync doesn't print anything to console
            selfname (string): the program's name, should be copied from sys.argv[0], used by is_running()

        Attributes:
            pid (int): PID of the current process
            src_abs (string): absolute path of source directory
            dst_abs (string): absolute path of destination directory, joined with date_ival
            logger (object): the logger object used for logging to file/console
    """
    def __init__(self,
                 config,
                 src,
                 dst,
                 checkonly=False,
                 cleanup=False,
                 no_incremental=False,
                 date_ival='daily',
                 quiet=False,
                 selfname="causync.py"):

        self.config = config
        self.name = selfname

        self.pid = os.getpid()

        self.no_incremental = no_incremental
        self.date_ival = date_ival

        self.src = src
        self.src_abs = os.path.abspath(self.src)

        self.dst = dst
        self.dst_abs = os.path.join(os.path.abspath(dst), date_ival)

        self.cleanup = cleanup
        self.checkonly = checkonly
        self.quiet = quiet

        self.logger = self.get_logger()

    def run(self):
        """ Main run function. """

        is_running = self.is_running()
        lockfile_exists = self.lockfile_exists()

        self.logger.info("started with PID {}".format(self.pid))

        if self.cleanup:
            self.run_cleanup()
            exit(0)
        else:
            if lockfile_exists or is_running:
                self.logger.info("causync is already running on {}".format(self.src_abs))
                exit(1)
            else:
                self.logger.info("causync is not already running on {}".format(self.src_abs))

        if not self.checkonly:
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

        dst = os.path.abspath("{dst}/{src}_{d}".format(dst=self.dst_abs,
                                                       src=self.get_basename(self.src_abs),
                                                       d=curdate))
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
        return "{}/{}.lock".format(src_parent_dir, src_basedir)

    def get_dirdate(self, dirname):
        """ Returns the date extracted from a backup directory name.
            Example: 'causync_180410_111237' results in a datetime object for '18-04-10 11:12:37'
        """

        try:
            dirdate = dirname.split('_', 1)[1]
            dirdate = datetime.strptime(dirdate, self.config.DATE_FORMAT)
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
        # join them with the source directory basename (example: 'sourcedir_YYMMHH')
        latest_dates = [ "{}_{}".format(self.get_basename(self.src_abs), i) for i in latest_dates ]
        # join each one with the destination directory (example: '/path/dest/sourcedir_YYMMHH'
        latest_dates = [ os.path.join(self.dst_abs, i) for i in latest_dates ]

        return latest_dates

    def find_old_backups(self, dirnames, ival='daily'):
        """ Returns old backups we should delete. """

        # extract dates from directory names and sort them
        dirdates = [ self.get_dirdate(d) for d in dirnames ]
        dirdates.sort(reverse=True)
        # determine list length (if backup count < delete count, we don't do anything)
        list_len = len(dirdates) if len(dirdates) <= self.config.BACKUPS_TO_KEEP[ival] else config.BACKUPS_TO_KEEP[ival]

        rmstrings = []

        # walk through deletable dates
        for d in dirdates[:list_len]:
            # join destination path with ival (example: /path/dest/daily)
            rmstring = os.path.join(os.path.abspath(self.dst), ival)
            # join source basename with date string
            rmstring = os.path.join(rmstring, '{}_{}'.format(self.get_basename(self.src_abs),
                                                             d.strftime(self.config.DATE_FORMAT)))
            rmstrings.append(rmstring)

        return rmstrings

    def run_cleanup(self):
        """ Deletes old backups.
            You can set how many you want to keep for each date/time interval in config.py.
            This function is executed when the task argument is 'cleanup'.
        """

        for ival in ['daily', 'mondays', 'monthly', 'yearly']:
            try:
                dirnames = self.find_old_backups(os.listdir(os.path.join(os.path.abspath(self.dst), ival)))

                for d in dirnames:
                    rmtree(d)

                self.logger.debug("ival={}, delete={}".format(ival, list(dirnames)))

            except FileNotFoundError:
                # this means one of the ival directories is missing
                pass

        self.logger.info("successfully deleted old backups")

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
            self.logger.debug("result={}".format(result))
            self.logger.debug("is_running lines: {}".format(result))

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

    parser.add_argument('-d',
                        '--date-interval',
                        dest='date_interval',
                        action='store',
                        default='daily',
                        choices=['yearly', 'monthly', 'mondays', 'daily'],
                        help="date interval for backups")

    parser.add_argument('-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help="don't print to console")

    arguments = parser.parse_args()

    arguments.selfname = sys.argv[0]
    arguments.checkonly = True if arguments.task == 'check' else False
    arguments.cleanup = True if arguments.task == 'cleanup' else False

    return arguments

if __name__ == "__main__":
    args = parse_args()

    cs = CauSync(config,
                 args.source,
                 args.destination,
                 args.checkonly,
                 args.cleanup,
                 args.no_incremental,
                 args.date_interval,
                 args.quiet,
                 args.selfname)
    cs.run()
