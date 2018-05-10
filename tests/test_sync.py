from nose.tools import *
from datetime import datetime
import os
from shutil import rmtree

from causync import CauSync
import config

date_format = "%Y%m%d"
lorem = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, " \
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " \
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi " \
        "ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit " \
        "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. " \
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui " \
        "officia deserunt mollit anim id est laborum."

# get 3 parts from lorem for writing into testfiles
lorem_parts = ((0, 123), (124, 232), (233, 335))

src = "./temp/causync_src"
dst = "./temp/causync_dst"

curdate = datetime.now()
curdate_str = curdate.strftime(date_format)

def remove_temp():
    # remove ./temp if exists
    rmtree('./temp') if os.path.isdir('./temp') else False

def create_temp():
    remove_temp()

    # create ./temp/causync_src
    [os.makedirs(d) for d in ['./temp', './temp/causync_src']]
    # create two testdirs inside ./temp/causync_src
    [os.mkdir(os.path.join(src, d)) for d in ['testdir1', 'testdir2']]

    # collect source testfile paths
    files = [os.path.join(src, 'testdir1', 'testfile1'),
             os.path.join(src, 'testdir1', 'testfile2'),
             os.path.join(src, 'testdir2', 'testfile3')]

    # write 3 sentences from lorem ipsum into 3 files
    for i in range(0, 3):
        with open(files[i], 'w') as fp:
            fp.write(lorem[ lorem_parts[i][0] : lorem_parts[i][1] ])

def test_lockfile_after_sync():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # lockfile should not exist after sync
    assert_false(os.path.isfile("{}.lock".format(src)))

    remove_temp()

def test_dry_run():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.dry_run = True
    cs.run_sync()

    # check for non-existing test dirs
    [assert_false(os.path.isdir(os.path.join(dst, curdate_str, 'causync_src', d))) for d in ['testdir1', 'testdir2']]

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for non-existing testfiles
    [assert_false(os.path.isfile(f)) for f in files]

    remove_temp()

def test_sync():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # check for test dirs
    [assert_true(os.path.isdir(os.path.join(dst, curdate_str, 'causync_src', d))) for d in ['testdir1', 'testdir2']]

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for existing testfiles
    [assert_true(os.path.isfile(f)) for f in files]

    # compare testfile contents
    for i in range(0, 3):
        with open(files[i], 'r') as fp:
            assert_equals(fp.read(), lorem[ lorem_parts[i][0] : lorem_parts[i][1] ])

    remove_temp()


def test_exclude():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.excludes = ['testfile2']
    cs.run_sync()

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for file1 and file3
    [assert_true(os.path.isfile(f)) for f in [files[0] ,files[2]]]
    # file2 should not exist because of exclude
    assert_false(os.path.isfile(files[1]))

    remove_temp()

def test_exclude_file():
    create_temp()

    with open('./temp/exclude.txt', 'w') as f:
        f.write('testfile1\ntestfile3\n')

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.excludes = cs.parse_exclude_file('./temp/exclude.txt')
    cs.run_sync()

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for file1 and file3
    [assert_false(os.path.isfile(f)) for f in [files[0] ,files[2]]]
    # file2 should not exist because of exclude
    assert_true(os.path.isfile(files[1]))

    remove_temp()