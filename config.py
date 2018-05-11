#!/usr/bin/env python3
# config file for causync

LOGFILE = "causync.log"
PIDFILE = "/tmp/causync.pid"

# choices: ['debug', 'info', 'warning', 'error', 'critical']
LOGLEVEL = 'debug'

# how many backups should we get
BACKUPS_LINK_DEST_COUNT = 5
BACKUPS_TO_KEEP = {
    'yearly': 10,
    'monthly': 6,
    'weekly': 4,
    'daily': 7
}

BACKUP_MULTIPLIERS = {
    'yearly': 365,
    'monthly': 31,
    'weekly': 7,
    'daily': 1
}

RSYNC_FLAGS = (
    "--archive --one-file-system --hard-links "
    "--human-readable --inplace --numeric-ids "
    "--stats "
)

DATE_FORMAT = "%Y%m%d"

# use pgrep to determine whether CS is running with the same arguments
CHECK_PGREP = True