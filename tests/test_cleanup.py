from nose.tools import *

from causync import CauSync
import config

from tests.testhelper import *

curdate = datetime(year=2018, month=4, day=11)
curdate_str = curdate.strftime(date_format)


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

    create_temp()

    os.makedirs(dst)
    [ os.makedirs(os.path.join(dst, i)) for i in dirnames ]


    dirnames_delete = list(set(dirnames) - set(dirnames_keep))

    cs.run_cleanup()

    for dirname in dirnames_keep:
        isdircheck = os.path.realpath(os.path.join("./temp/causync_dst", dirname))
        assert_true(os.path.isdir(isdircheck))

    for dirname in dirnames_delete:
        isdircheck = os.path.realpath(os.path.join("./temp/causync_dst", dirname))
        assert_false(os.path.isdir(isdircheck))

    remove_temp()
