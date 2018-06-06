from nose.tools import *

from causync import CauSync
import config

from tests.testhelper import *

def test_exclude():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync', excludes=['testfile2'])
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for file1 and file3
    [assert_true(os.path.isfile(f)) for f in [files[0], files[2]]]
    # file2 should not exist because of exclude
    assert_false(os.path.isfile(files[1]))

    remove_temp()

def test_multi_exclude():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync', excludes=['testfile2', 'testfile3'])
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # collect destination testfile paths
    files = [os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile1'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir1', 'testfile2'),
             os.path.join(dst, curdate_str, 'causync_src', 'testdir2', 'testfile3')]

    # check for file1 and file4
    assert_true(os.path.isfile(files[0]))
    # file2 and file3 should not exist because of exclude
    [assert_false(os.path.isfile(f)) for f in [files[1], files[2]]]

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
    [assert_false(os.path.isfile(f)) for f in [files[0], files[2]]]
    # file2 should not exist because of exclude
    assert_true(os.path.isfile(files[1]))

    remove_temp()
