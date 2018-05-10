from datetime import datetime

from causync import CauSync
import config

dirnames = ['20040101', '20050101', '20060101', '20070101', '20080101',
            '20090101', '20100101', '20110101', '20120101', '20130101',
            '20140101', '20150101', '20160101', '20160201', '20160411',
            '20170101', '20170701', '20170801', '20170901', '20171001',
            '20171101', '20171201', '20180101', '20180201', '20180301',
            '20180401', '20180402', '20180403', '20180404', '20180405',
            '20180406', '20180407', '20180408', '20180409', '20180410',
            '20180411']


def test_find_latest_backups():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", task='sync')
    cs.config.DATE_FORMAT = "%Y%m%d"

    dirnames_result = ['/tmp/causync_dest/20180411',
                       '/tmp/causync_dest/20180410',
                       '/tmp/causync_dest/20180409',
                       '/tmp/causync_dest/20180408',
                       '/tmp/causync_dest/20180407']

    assert cs.find_latest_backups(dirnames) == dirnames_result


def test_find_old_backups_daily():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", task='cleanup')
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
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", task='cleanup')
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
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", task='cleanup')
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
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", task='cleanup')
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
