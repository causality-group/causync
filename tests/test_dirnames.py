from nose.tools import *

from causync import CauSync
import config


def test_get_parent_dir():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")

    assert_equals(cs.get_parent_dir('/tmp/causync_src/test'), '/tmp/causync_src')
    assert_equals(cs.get_parent_dir('/tmp/causync_src/test/'), '/tmp/causync_src')
    assert_equals(cs.get_parent_dir('/tmp/causync_src'), '/tmp')
    assert_equals(cs.get_parent_dir('/tmp/causync_src/'), '/tmp')
    assert_equals(cs.get_parent_dir('/tmp'), '/')
    assert_equals(cs.get_parent_dir('/tmp/'), '/')
    assert_equals(cs.get_parent_dir('/'), '/')

def test_get_parent_dir_doubleslash():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")

    assert_equals(cs.get_parent_dir('/tmp/causync_src/test//'), '/tmp/causync_src')
    assert_equals(cs.get_parent_dir('/tmp//causync_src//test//'), '/tmp/causync_src')
    assert_equals(cs.get_parent_dir('/tmp///causync_src/test'), '/tmp/causync_src')
    assert_equals(cs.get_parent_dir('/tmp////causync_src/test'), '/tmp/causync_src')

@raises(ValueError)
def test_get_parent_dir_toomuchroot1():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.get_parent_dir('//tmp/causync_src')

@raises(ValueError)
def test_get_parent_dir_toomuchroot2():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.get_parent_dir('//')

def test_get_basename():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    assert_equals(cs.get_basename('/tmp/causync_src/test'), 'test')
    assert_equals(cs.get_basename('/tmp'), 'tmp')

@raises(ValueError)
def test_get_basename_root():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.get_basename('/')

@raises(ValueError)
def test_get_basename_root2():
    cs = CauSync(config, "/tmp/causync_src", "/tmp/causync_dest")
    cs.get_basename('//')
