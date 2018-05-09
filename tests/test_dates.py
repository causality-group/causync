from datetime import datetime

from causync import CauSync
import config

def test_get_dirdate():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest", 'check')
    now = datetime.now()
    cs.config.DATE_FORMAT = "%Y%m%d"
    dirname = now.strftime(config.DATE_FORMAT)

    assert isinstance(cs.get_dirdate(dirname), datetime)
    assert cs.get_dirdate(dirname).strftime(cs.config.DATE_FORMAT) == now.strftime(cs.config.DATE_FORMAT)

