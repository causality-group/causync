from nose.tools import *
import os

from causync import CauSync
import config


def test_get_lockfile_path():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    assert_equals(cs.get_lockfile_path(), '/tmp/causync_src.lock')

def test_create_lockfile():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.create_lockfile()
    assert_true(os.path.isfile('/tmp/causync_src.lock'))

def test_remove_lockfile():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.remove_lockfile()
    assert_false(os.path.isfile('/tmp/causync_src.lock'))

def test_lockfile_exists():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.create_lockfile()
    assert_true(os.path.isfile('/tmp/causync_src.lock') and cs.lockfile_exists())
    cs.remove_lockfile()

