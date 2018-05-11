from nose.tools import *
from datetime import datetime
import os
import shutil

from causync import CauSync
import config

date_format = "%Y%m%d"

dirnames = ['20040101', '20050101', '20060101', '20070101', '20080101',
            '20090101', '20100101', '20110101', '20120101', '20130101',
            '20140101', '20150101', '20160101', '20160201', '20160411',
            '20170101', '20170701', '20170801', '20170901', '20171001',
            '20171101', '20171201', '20180101', '20180201', '20180301',
            '20180401', '20180402', '20180403', '20180404', '20180405',
            '20180406', '20180407', '20180408', '20180409', '20180410',
            '20180411']

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

curdate = datetime(year=2018, month=4, day=11)
curdate_str = curdate.strftime(date_format)


def remove_temp():
    # remove ./temp if exists
    shutil.rmtree('./temp') if os.path.isdir('./temp') else False


def copytree(source, dest, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(source, item)
        d = os.path.join(dest, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def create_temp():
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
            fp.write(lorem[lorem_parts[i][0]: lorem_parts[i][1]])

    for dirname in dirnames:
        copytree('./temp/causync_src', "./temp/causync_dst/{}/".format(dirname))


def test_cleanup():
    remove_temp()

    cs = CauSync(config, src, dst, task='cleanup')
    cs.config.DATE_FORMAT = "%Y%m%d"
    cs.config.BACKUPS_TO_KEEP = {'yearly': 10, 'monthly': 6,
                                 'weekly': 4, 'daily': 7}
    cs.config.BACKUP_MULTIPLIERS = {'yearly': 365, 'monthly': 31,
                                    'weekly': 7, 'daily': 1}
    cs.config.BACKUPS_LINK_DEST_COUNT = 5
    cs.curdate = curdate

    dirnames_keep = ['20080101', '20090101', '20100101', '20110101', '20120101',
                     '20130101', '20140101', '20150101', '20160101', '20170101',
                     '20171001', '20171101', '20171201', '20180101', '20180201',
                     '20180301', '20180401', '20180402', '20180404', '20180405',
                     '20180406', '20180407', '20180408', '20180409', '20180410',
                     '20180411']

    dirnames_delete = list(set(dirnames) - set(dirnames_keep))

    create_temp()

    cs.run_cleanup()

    for dirname in dirnames_keep:
        assert_true(os.path.isdir(os.path.join("./temp/causync_dst/{}".format(dirname))))

    for dirname in dirnames_delete:
        assert_false(os.path.isdir(os.path.join("./temp/causync_dst/{}".format(dirname))))

    remove_temp()
