#!/usr/bin/env python3
# config file for causync

LOGFILE="causync.log"
# debug=10, info=20, warning=30, error=40, critical=50
LOGLEVEL=10

BACKUPS_TO_KEEP=5

RSYNC_FLAGS=(
    "--archive --one-file-system --hard-links "
    "--human-readable --inplace --numeric-ids "
    "--stats "
)

DATE_FORMAT="%y%m%d_%H%M%S"
