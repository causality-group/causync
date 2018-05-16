# causync
Rsync wrapper for doing incremental yearly/monthly/weekly/daily backups using hard links.

# Install

The program itself runs on Python3. It uses rsync and pgrep (called with `subprocess.check_output()`).
Just clone the repo and it's ready to go.

# Usage

The first argument is a `task`. Its values can be `check`, `sync`, `cleanup`.
Only the selected task is executed, then the program exits.

## Check

Checks if causync is already running on the specified source and backup directories.

Example: 
```text
$ python3 causync.py check /var/www/localhost/site /backups/site
2018-05-02 13:34:25,475 INFO started with PID 17677
2018-05-02 13:34:25,475 INFO causync is not already running on /var/www/localhost/site
```

## Sync

Collects previous N number of backups (defined in `config.py`) and calls `rsync` with multiple `--link-dest` arguments to do an "incremental" backup.
If no previous backups are found, then `--link-dest` is skipped and a "full" backup is created.

The sync task creates a lockfile with the name of the source directory. In the above example this lockfile will be at  
`/var/www/localhost/site.lock`

Example:
```text
# python3 causync.py sync /var/www/localhost/site /backups/site                                                                                           
2018-05-02 13:36:07,069 DEBUG is_running() lines: [b'17754']                                                                                                                                     
2018-05-02 13:36:07,069 INFO started with PID 17754                                                                                                                                              
2018-05-02 13:36:07,069 INFO causync is not already running on /var/www/localhost/site                                                                                                           
2018-05-02 13:36:07,089 DEBUG inc_basedirs=['/backups/site/20180502']                                                                                                                            
2018-05-02 13:36:07,090 INFO found incremental basedirs, using them in --link-dest                                                                                                               
2018-05-02 13:36:07,090 DEBUG rsync cmd = rsync --archive --one-file-system --hard-links --human-readable --inplace --numeric-ids --stats   --link-dest=/backups/site/20180502  /var/www/localhost/site /backups/site/20180502
2018-05-02 13:36:07,090 INFO syncing /var/www/localhost/site to /backups/site/20180502                                                                                                           
2018-05-02 13:36:07,090 DEBUG creating pidfile /tmp/causync.pid                                                                                                                     
2018-05-02 13:36:07,095 DEBUG                                                                                                                                                                    
Number of files: 4 (reg: 3, dir: 1)
Number of created files: 0
Number of deleted files: 0
Number of regular files transferred: 0
Total file size: 20 bytes
Total transferred file size: 0 bytes
Literal data: 0 bytes
Matched data: 0 bytes
File list size: 0
File list generation time: 0.001 seconds
File list transfer time: 0.000 seconds
Total bytes sent: 145
Total bytes received: 25

sent 145 bytes  received 25 bytes  340.00 bytes/sec
total size is 20  speedup is 0.12

2018-05-02 13:36:07,095 DEBUG removing pidfile /tmp/causync.pid                                                                                                                     
2018-05-02 13:36:07,095 INFO sync finished                                  
```  

## Sync from multiple sources

Pass multiple sources to rsync.

Example:
```text
python3 causync.py sync /var/www/localhost/site1 /var/www/localhost/site2 /backups/sites
```

## Sync with `--exclude`

Pass multiple exclude parameters or an exclude file to rsync with this functionality.

Example: 
```text
python3 causync.py sync --exclude=index.html /var/www/localhost/site /backups/site
python3 causync.py sync --exclude-from=exclude_list.txt /var/www/localhost/site /backups/site
```

## Cleanup

Collects old backups and deletes them. Backup counts for yearly/monthly/weekly/daily are set in config.py.

Example:

```text
# python3 causync.py cleanup /var/www/localhost/site /backups/site                                                                                        
2018-05-02 13:47:38,365 DEBUG is_running() lines: [b'18317']                                                                                                                                     
2018-05-02 13:47:38,366 INFO started with PID 18317                                                                                                                                              
2018-05-02 13:47:38,371 DEBUG removed /backups/site/20180420                                                                                                                                     
2018-05-02 13:47:38,372 DEBUG removed /backups/site/20180421                                                                                                                                     
2018-05-02 13:47:38,372 DEBUG removed /backups/site/20180422                                                                                                                                     
2018-05-02 13:47:38,372 DEBUG removed /backups/site/20180424                                                                                                                                     
2018-05-02 13:47:38,373 INFO successfully deleted old backups
``` 

# Running tests

You can run tests with `nose`. Install it with `pip install nose`, then do the following:
```bash
cd tests
nosetests .
```

Or if you want to run specific tests: `nosetests test_sync.py`