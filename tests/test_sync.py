from nose.tools import *

from causync import CauSync
import config

from tests.testhelper import *


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
            assert_equals(fp.read(), lorem[lorem_parts[i][0]: lorem_parts[i][1]])

    remove_temp()
