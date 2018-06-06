from os import path

from causync import CauSync
import config

from tests.testhelper import *

src = path.realpath("/tmp/causync_src")
dst = path.realpath("/tmp/causync_dest")


def test_find_latest_backups():
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = "%Y%m%d"

    paths = ['20180411', '20180410', '20180409', '20180408', '20180407']

    dirnames_result = [path.realpath("{}/{}".format(dst, i)) for i in paths]

    print(cs.find_latest_backups(dirnames))
    assert cs.find_latest_backups(dirnames) == dirnames_result


def test_find_old_backups_daily():
    cs = CauSync(config, src, dst, task='cleanup')
    cs.config.DATE_FORMAT = "%Y%m%d"
    cs.config.BACKUPS_TO_KEEP = {'yearly': 10, 'monthly': 6,
                                 'weekly': 4, 'daily': 7}
    cs.config.BACKUP_MULTIPLIERS = {'yearly': 365, 'monthly': 31,
                                    'weekly': 7, 'daily': 1}
    cs.config.BACKUPS_LINK_DEST_COUNT = 5
    cs.curdate = datetime(year=2018, month=4, day=11)

    (dirnames_keep, dirnames_delete) = ([datetime(2018, 4, 11, 0, 0), datetime(2018, 4, 10, 0, 0),
                                         datetime(2018, 4, 9, 0, 0), datetime(2018, 4, 8, 0, 0),
                                         datetime(2018, 4, 7, 0, 0), datetime(2018, 4, 6, 0, 0)],
                                        [datetime(2018, 4, 5, 0, 0), datetime(2018, 4, 4, 0, 0),
                                         datetime(2018, 4, 3, 0, 0), datetime(2018, 4, 2, 0, 0),
                                         datetime(2018, 4, 1, 0, 0), datetime(2018, 3, 1, 0, 0),
                                         datetime(2018, 2, 1, 0, 0), datetime(2018, 1, 1, 0, 0),
                                         datetime(2017, 12, 1, 0, 0), datetime(2017, 11, 1, 0, 0),
                                         datetime(2017, 10, 1, 0, 0), datetime(2017, 9, 1, 0, 0),
                                         datetime(2017, 8, 1, 0, 0), datetime(2017, 7, 1, 0, 0),
                                         datetime(2017, 1, 1, 0, 0), datetime(2016, 4, 11, 0, 0),
                                         datetime(2016, 2, 1, 0, 0), datetime(2016, 1, 1, 0, 0),
                                         datetime(2015, 1, 1, 0, 0), datetime(2014, 1, 1, 0, 0),
                                         datetime(2013, 1, 1, 0, 0), datetime(2012, 1, 1, 0, 0),
                                         datetime(2011, 1, 1, 0, 0), datetime(2010, 1, 1, 0, 0),
                                         datetime(2009, 1, 1, 0, 0), datetime(2008, 1, 1, 0, 0),
                                         datetime(2007, 1, 1, 0, 0), datetime(2006, 1, 1, 0, 0),
                                         datetime(2005, 1, 1, 0, 0), datetime(2004, 1, 1, 0, 0)])

    assert cs.find_old_backups(dirnames, 'daily') == (dirnames_keep, dirnames_delete)


def test_find_old_backups_weekly():
    cs = CauSync(config, src, dst, task='cleanup')
    cs.config.DATE_FORMAT = "%Y%m%d"
    cs.config.BACKUPS_TO_KEEP = {'yearly': 10, 'monthly': 6,
                                 'weekly': 4, 'daily': 7}
    cs.config.BACKUP_MULTIPLIERS = {'yearly': 365, 'monthly': 31,
                                    'weekly': 7, 'daily': 1}
    cs.config.BACKUPS_LINK_DEST_COUNT = 5
    cs.curdate = datetime(year=2018, month=4, day=11)

    (dirnames_keep, dirnames_delete) = ([datetime(2018, 4, 9, 0, 0), datetime(2018, 4, 2, 0, 0)],
                                        [datetime(2018, 1, 1, 0, 0), datetime(2016, 4, 11, 0, 0),
                                         datetime(2016, 2, 1, 0, 0), datetime(2007, 1, 1, 0, 0)])

    assert cs.find_old_backups(dirnames, 'weekly') == (dirnames_keep, dirnames_delete)


def test_find_old_backups_monthly():
    cs = CauSync(config, src, dst, task='cleanup')
    cs.config.DATE_FORMAT = "%Y%m%d"
    cs.config.BACKUPS_TO_KEEP = {'yearly': 10, 'monthly': 6,
                                 'weekly': 4, 'daily': 7}
    cs.config.BACKUP_MULTIPLIERS = {'yearly': 365, 'monthly': 31,
                                    'weekly': 7, 'daily': 1}
    cs.config.BACKUPS_LINK_DEST_COUNT = 5
    cs.curdate = datetime(year=2018, month=4, day=11)

    (dirnames_keep, dirnames_delete) = ([datetime(2018, 4, 1, 0, 0), datetime(2018, 3, 1, 0, 0),
                                         datetime(2018, 2, 1, 0, 0), datetime(2018, 1, 1, 0, 0),
                                         datetime(2017, 12, 1, 0, 0), datetime(2017, 11, 1, 0, 0)],
                                        [datetime(2017, 10, 1, 0, 0), datetime(2017, 9, 1, 0, 0),
                                         datetime(2017, 8, 1, 0, 0), datetime(2017, 7, 1, 0, 0),
                                         datetime(2017, 1, 1, 0, 0), datetime(2016, 2, 1, 0, 0),
                                         datetime(2016, 1, 1, 0, 0), datetime(2015, 1, 1, 0, 0),
                                         datetime(2014, 1, 1, 0, 0), datetime(2013, 1, 1, 0, 0),
                                         datetime(2012, 1, 1, 0, 0), datetime(2011, 1, 1, 0, 0),
                                         datetime(2010, 1, 1, 0, 0), datetime(2009, 1, 1, 0, 0),
                                         datetime(2008, 1, 1, 0, 0), datetime(2007, 1, 1, 0, 0),
                                         datetime(2006, 1, 1, 0, 0), datetime(2005, 1, 1, 0, 0),
                                         datetime(2004, 1, 1, 0, 0)])

    assert cs.find_old_backups(dirnames, 'monthly') == (dirnames_keep, dirnames_delete)


def test_find_old_backups_yearly():
    cs = CauSync(config, src, dst, task='cleanup')
    cs.config.DATE_FORMAT = "%Y%m%d"
    cs.config.BACKUPS_TO_KEEP = {'yearly': 10, 'monthly': 6,
                                 'weekly': 4, 'daily': 7}
    cs.config.BACKUP_MULTIPLIERS = {'yearly': 365, 'monthly': 31,
                                    'weekly': 7, 'daily': 1}
    cs.config.BACKUPS_LINK_DEST_COUNT = 5
    cs.curdate = datetime(year=2018, month=4, day=11)

    (dirnames_keep, dirnames_delete) = ([datetime(2018, 1, 1, 0, 0), datetime(2017, 1, 1, 0, 0),
                                         datetime(2016, 1, 1, 0, 0), datetime(2015, 1, 1, 0, 0),
                                         datetime(2014, 1, 1, 0, 0), datetime(2013, 1, 1, 0, 0)],
                                        [datetime(2012, 1, 1, 0, 0), datetime(2011, 1, 1, 0, 0),
                                         datetime(2010, 1, 1, 0, 0), datetime(2009, 1, 1, 0, 0),
                                         datetime(2008, 1, 1, 0, 0), datetime(2007, 1, 1, 0, 0),
                                         datetime(2006, 1, 1, 0, 0), datetime(2005, 1, 1, 0, 0),
                                         datetime(2004, 1, 1, 0, 0)])

    assert cs.find_old_backups(dirnames, 'yearly') == (dirnames_keep, dirnames_delete)
