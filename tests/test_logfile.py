from nose.tools import *

from causync import CauSync
import config

from tests.testhelper import *


def test_logfile_default():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync')
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # lockfile should not exist after sync
    assert_true(os.path.isfile(cs.config.LOGFILE))

    remove_temp()

def test_logfile_custom():
    create_temp()

    # start sync testing
    cs = CauSync(config, src, dst, task='sync', logfile='./temp/test.log')
    cs.config.DATE_FORMAT = date_format
    cs.run_sync()

    # lockfile should not exist after sync
    assert_true(os.path.isfile('./temp/test.log'))

    remove_temp()