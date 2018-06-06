import os
from shutil import rmtree
from datetime import datetime
import shutil

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

dirnames = ['20040101', '20050101', '20060101', '20070101', '20080101',
            '20090101', '20100101', '20110101', '20120101', '20130101',
            '20140101', '20150101', '20160101', '20160201', '20160411',
            '20170101', '20170701', '20170801', '20170901', '20171001',
            '20171101', '20171201', '20180101', '20180201', '20180301',
            '20180401', '20180402', '20180403', '20180404', '20180405',
            '20180406', '20180407', '20180408', '20180409', '20180410',
            '20180411']

dirnames_keep = ['20080101', '20090101', '20100101', '20110101', '20120101',
                 '20130101', '20140101', '20150101', '20160101', '20170101',
                 '20171001', '20171101', '20171201', '20180101', '20180201',
                 '20180301', '20180401', '20180402', '20180404', '20180405',
                 '20180406', '20180407', '20180408', '20180409', '20180410',
                 '20180411']

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
            fp.write(lorem[lorem_parts[i][0]: lorem_parts[i][1]])

def copytree(source, dest, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(source, item)
        d = os.path.join(dest, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)