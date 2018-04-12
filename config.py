#!/usr/bin/env python3
# config file for causync

LOGFILE="causync.log"
# debug=10, info=20, warning=30, error=40, critical=50
LOGLEVEL=10

# how many backups should we get
BACKUPS_LINK_DEST_COUNT=5
BACKUPS_TO_KEEP = {
    'yearly': 1,
    'monthly': 3,
    'mondays': 3,
    'daily': 3
}

RSYNC_FLAGS=(
    "--archive --one-file-system --hard-links "
    "--human-readable --inplace --numeric-ids "
    "--stats "
)

DATE_FORMAT="%Y%m%dT%H%M%S"
