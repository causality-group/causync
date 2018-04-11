from nose.tools import *
from datetime import datetime

from causync import CauSync
import config

def test_get_dirdate():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    now = datetime.now()
    dirname = "testdir_{}".format(now.strftime(config.DATE_FORMAT))

    assert isinstance(cs.get_dirdate(dirname), datetime)
    assert cs.get_dirdate(dirname).strftime("%Y-%m-%d %H:%M:%S") == now.strftime("%Y-%m-%d %H:%M:%S")

def test_get_dirdate_inc():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    now = datetime.now()
    dirname = "testdir_180101_010101_inc_{}".format(now.strftime(config.DATE_FORMAT))

    assert isinstance(cs.get_dirdate(dirname), datetime)
    assert cs.get_dirdate(dirname).strftime("%Y-%m-%d %H:%M:%S") == now.strftime("%Y-%m-%d %H:%M:%S")
