from causync import CauSync
import config

def test_find_latest_backups():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.config.DATE_FORMAT = "%y%m%d_%H%M%S"

    dirnames = [
        'causync_160201_180912',
        'causync_160411_180844',
        'causync_170111_180845',
        'causync_180411_201836',
        'causync_180411_201845',
        'causync_180411_202010',
        'causync_180411_202027',
        'causync_180411_202042',
        'causync_180411_202046',
        'causync_180411_202047',
        'causync_180411_202048'
    ]

    dirnames_result = [
        '/tmp/causync_dest/daily/causync_src_180411_202048',
        '/tmp/causync_dest/daily/causync_src_180411_202047',
        '/tmp/causync_dest/daily/causync_src_180411_202046',
        '/tmp/causync_dest/daily/causync_src_180411_202042',
        '/tmp/causync_dest/daily/causync_src_180411_202027'
    ]

    assert cs.find_latest_backups(dirnames) == dirnames_result

def test_find_old_backups():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.config.DATE_FORMAT = "%y%m%d_%H%M%S"

    dirnames = [
        'causync_160201_180912',
        'causync_160411_180844',
        'causync_170111_180845',
        'causync_180411_201836',
        'causync_180411_201845',
        'causync_180411_202010',
        'causync_180411_202027',
        'causync_180411_202042',
        'causync_180411_202046',
        'causync_180411_202047',
        'causync_180411_202048'
    ]

    dirnames_result = [
        '/tmp/causync_dest/daily/causync_src_180411_202048',
        '/tmp/causync_dest/daily/causync_src_180411_202047',
        '/tmp/causync_dest/daily/causync_src_180411_202046'
    ]

    assert cs.find_old_backups(dirnames) == dirnames_result

