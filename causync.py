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
            cs_config (module): config module is passed to CauSync here
            src (str): backup source directory, this is backed up to the destination
            dst (str): backup destination directory, this is where source is backed up to
            checkonly (bool): if True, CauSync only check if it is running on the specified directories (source, dest)
            cleanup (bool): if True, CauSync deletes old backups - see run_cleanup()
            incremental (bool): if True, CauSync does incremental backups
            incremental_basedir (string): the basedir for incrementals
            quiet (bool): if set to True, CauSync doesn't print anything to console
            selfname (string): the program's name, should be copied from sys.argv[0], used by is_running()

        Attributes:
            pid (int): PID of the running process, uses is_running() to guess the number.
                It will fail to get the correct number if another process is already running
                for the same directory, but in that case the sync shouldn't be started anyway.
            src_abs (string): absolute path of source directory
            dst_abs (string): absolute path of destination directory
            logger (object): the logger object used for logging to file/console
    """
    def __init__(self,
                 cs_config,
                 src,
                 dst,
                 checkonly=False,
                 cleanup=False,
                 incremental=False,
                 incremental_basedir=None,
                 quiet=False,
                 selfname="causync.py"):

        self.config = cs_config
        self.name = selfname

        self.pid = None

        self.src = src
        self.src_abs = os.path.abspath(self.src)

        self.dst = dst
        self.dst_abs = os.path.abspath(dst)

        self.cleanup = cleanup
        self.checkonly = checkonly
        self.incremental = incremental
        self.incremental_basedir = os.path.abspath(incremental_basedir) if incremental_basedir else None
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
        """ This function is executed when the task argument is 'sync'. """

        curdate = datetime.now().strftime(config.DATE_FORMAT)
        extra_flags = ""

        if self.incremental:
            if not self.incremental_basedir:
                self.incremental_basedir = self.find_incremental_basedir()
            inc_basedir_dirname = self.get_basename(self.incremental_basedir)

            dst = os.path.abspath("{dst}/{bd}_inc_{d}".format(dst=self.dst_abs,
                                                              bd=inc_basedir_dirname,
                                                              d=curdate))

            extra_flags = "--link-dest={}".format(self.incremental_basedir)

        else:
            dst = os.path.abspath("{dst}/{src}_{d}".format(dst=self.dst_abs,
                                                           src=self.get_basename(self.src_abs),
                                                           d=curdate))

        cmd = "rsync {f} {ef} {src} {dst}".format(f=self.config.RSYNC_FLAGS,
                                                  ef=extra_flags,
                                                  src=self.src_abs,
                                                  dst=dst)

        self.logger.info("syncing {} to {}".format(self.src_abs, dst))
        self.create_lockfile()

        result = subprocess.check_output(cmd, shell=True).decode()
        self.logger.debug(result)

        self.remove_lockfile()
        self.logger.info("sync finished")

        return result

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
            dirdate = "_".join(dirname.rsplit('_', 2)[1:])
            dirdate = datetime.strptime(dirdate, config.DATE_FORMAT)
            return dirdate
        except ValueError as e:
            self.logger.error(e)
            exit(1)

    def find_incremental_basedir(self):
        """ Returns the latest basedir of the last backup. """

        dirnames = os.listdir(self.dst_abs)
        dirdates = []
        self.logger.debug(dirnames)
        basename = self.get_basename(self.src_abs)

        for dirname in dirnames:
            if basename not in dirname:
                continue
            elif self.incremental and "_inc_" in dirname:
                continue
            else:
                dirdates.append(self.get_dirdate(dirname))

        dirdates.sort()
        self.logger.debug(dirdates)
        basedir = "{}_{}".format(self.get_basename(self.src_abs), dirdates[-1].strftime(config.DATE_FORMAT))
        self.logger.debug("basedir = " + basedir)

        return os.path.join(self.dst_abs, basedir)

    def run_cleanup(self):
        """ Deletes old backups. You can set (in the config) the count of backups to keep.
            This function is executed when the task argument is 'cleanup'.
        """

        dirnames = os.listdir(self.dst_abs)
        dirdates = []
        self.logger.debug(dirnames)

        if self.incremental:
            basename = self.incremental_basedir.rsplit('/', 1)[1]
        else:
            basename = self.src_abs.split('_', 1)[0]

        self.logger.debug(basename)

        for dirname in dirnames:
            if basename not in dirname:
                continue
            elif self.incremental and "_inc_" not in dirname:
                continue
            #elif incremental and basename in dirname and "_inc_" in dirname:
            #    dirdates.append(self.get_dirdate(dirname))
            else:
                dirdates.append(self.get_dirdate(dirname))

        dirdates.sort()

        keep = dirdates[-5:]
        delete = dirdates[:-5]
        self.logger.debug("keep={}".format(keep))
        self.logger.debug("delete={}".format(delete))

        if self.incremental:
            rmstring = "{dst}/{bn}_inc_{d}"
        else:
            rmstring = "{dst}/{bn}_{d}"

        for dirname in delete:
            rmtree(rmstring.format(dst=self.dst_abs.rstrip('/'),
                                   bn=basename,
                                   d=dirname.strftime(config.DATE_FORMAT)))

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

        cmd = "pgrep -f .*python3.*{}.*{}.*{}".format(self.name, self.src, self.dst)

        try:
            result = subprocess.check_output(cmd, shell=True).splitlines()
            self.logger.debug("is_running lines: {}".format(result))

            try:
                self.pid = result[0].decode()
            except IndexError:
                pass

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

    parser.add_argument('--incremental',
                        dest='incremental',
                        action='store_true',
                        default=False,
                        help='incremental backup')

    parser.add_argument('--incremental-basedir',
                        dest='incremental_basedir',
                        action='store',
                        default=None,
                        help='basedir of full backup')

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
                 args.incremental,
                 args.incremental_basedir,
                 args.quiet,
                 args.selfname)
    cs.run()
