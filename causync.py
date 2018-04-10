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
    """ CauSync object for Sync-related functions. """
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
        self.src_basedir = self.src_abs.rstrip('/').split('/')[-1]
        self.src_parent_dir = self.src_abs.rstrip('/').rsplit('/', 1)[0]
        self.src_abs = os.path.abspath(src)
        self.src_lockfile_path = "{}/{}.lock".format(self.src_parent_dir, self.src_basedir)

        self.dst = dst
        self.dst_abs = os.path.abspath(dst)

        self.cleanup = cleanup
        self.checkonly = checkonly
        self.incremental = incremental
        self.incremental_basedir = os.path.abspath(incremental_basedir) if incremental else None
        self.quiet = quiet

        self.logger = self.get_logger()

    def run(self):
        """ Main run function. """
        self.logger.info("started with PID {}".format(self.pid))

        if self.cleanup:
            self.run_cleanup(self.incremental)
        else:
            if self.lockfile_exists() or self.is_running():
                self.logger.info("causync is already running on {}".format(self.src_abs))
            else:
                self.logger.info("causync is not already running on {}".format(self.src_abs))

            if not self.checkonly:
                self.run_sync()

    def run_sync(self):
        """ This function is executed when the task argument is 'sync'. """

        curdate = datetime.now().strftime(config.DATE_FORMAT)
        extra_flags = ""

        if not self.incremental:
            dst = os.path.abspath("{dst}/{src}_{d}".format(dst=self.dst,
                                                           src=self.src_basedir,
                                                           d=curdate))
        else:
            inc_basedir_dirname = self.incremental_basedir.rstrip('/').rsplit('/', 1)[1]
            dst = os.path.abspath("{dst}/{bd}_inc_{d}".format(dst=self.dst,
                                                              bd=inc_basedir_dirname,
                                                              d=curdate))

        if self.incremental and self.incremental_basedir:
            extra_flags = "--link-dest={}".format(self.incremental_basedir)

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

    def run_cleanup(self, incremental=False):
        """ This function is executed when the task argument is 'cleanup'. """

        dirnames = os.listdir(self.dst)

        self.logger.debug(dirnames)

        dirdates = []

        if incremental:
            basename = self.incremental_basedir.rsplit('/', 1)[1]
        else:
            basename = self.src_abs.split('_', 1)[0]

        self.logger.debug(basename)

        for dirname in dirnames:
            if basename not in dirname:
                continue
            elif incremental and "_inc_" not in dirname:
                continue
            elif incremental and basename in dirname and "_inc_" in dirname:
                try:
                    dirdate = "_".join(dirname.rsplit('_', 2)[1:])
                    dirdate = datetime.strptime(dirdate, config.DATE_FORMAT)
                    dirdates.append(dirdate)
                except ValueError as e:
                    self.logger.debug(e)
            else:
                try:
                    dirdate = "_".join(dirname.rsplit('_', 2)[1:])
                    dirdate = datetime.strptime(dirdate, config.DATE_FORMAT)
                    dirdates.append(dirdate)

                except ValueError as e:
                    self.logger.debug(e)

        dirdates.sort()

        keep = dirdates[-5:]
        delete = dirdates[:-5]
        self.logger.debug("keep={}".format(keep))
        self.logger.debug("delete={}".format(delete))

        if incremental:
            rmstring = "{dst}/{bn}_inc_{d}"
        else:
            rmstring = "{dst}/{bn}_{d}"

        for dirname in delete:
            rmtree(rmstring.format(dst=self.dst.rstrip('/'),
                                   bn=basename,
                                   d=dirname.strftime(config.DATE_FORMAT)))

        self.logger.info("successfully deleted old backups")

    def create_lockfile(self):
        """ Creates a lockfile at sync source directory. """

        self.logger.debug("creating lockfile {}".format(self.src_lockfile_path))

        try:
            open(self.src_lockfile_path, 'a').close()
            return True

        except IOError as e:
            self.logger.error(e)

    def remove_lockfile(self):
        """ Creates an existing lockfile at sync source directory. """

        self.logger.debug("removing lockfile {}".format(self.src_lockfile_path))

        try:
            os.remove(self.src_lockfile_path)
            return True

        except IOError as e:
            self.logger.error(e)

    def lockfile_exists(self):
        """ Checks if lockfile exists at sync source directory. """
        if os.path.isfile(self.src_lockfile_path):
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

    if arguments.incremental and not arguments.incremental_basedir:
        parser.print_usage()
        exit("{}: error: --incremental needs --incremental-basedir argument".format(arguments.selfname))

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
