from nose.tools import *
import os

from causync import CauSync
import config

src = '/tmp/causync_src'
src_list = ['/tmp/causync_src1', '/tmp/causync_src2']
dst = '/tmp/causync_dest'


def test_get_lockfile_path():
    cs = CauSync(config, src, dst, 'check')
    assert_equals(cs.get_lockfile_path(src), '/tmp/causync_src.lock')

def test_create_lockfile():
    cs = CauSync(config, src, dst, 'check')
    cs.create_lockfile(src)

    assert_true(os.path.isfile('/tmp/causync_src.lock'))

def test_create_lockfiles():
    cs = CauSync(config, src_list, dst, 'check')
    cs.create_lockfiles()

    assert_true(os.path.isfile('/tmp/causync_src1.lock'))
    assert_true(os.path.isfile('/tmp/causync_src2.lock'))

def test_remove_lockfile():
    cs = CauSync(config, src, dst, 'check')
    cs.remove_lockfile(src)

    assert_false(os.path.isfile('/tmp/causync_src.lock'))

def test_remove_lockfiles():
    cs = CauSync(config, src_list, dst, 'check')
    cs.remove_lockfile(src_list[0])
    cs.remove_lockfile(src_list[1])

    assert_false(os.path.isfile('/tmp/causync_src1.lock'))
    assert_false(os.path.isfile('/tmp/causync_src2.lock'))

def test_lockfile_exists():
    cs = CauSync(config, src, dst, 'check')
    cs.create_lockfile(src)
    assert_true(os.path.isfile('/tmp/causync_src.lock') and cs.lockfile_exists(src))
    cs.remove_lockfile(src)

def test_lockfiles_exist():
    cs = CauSync(config, src_list, dst, 'check')
    cs.create_lockfile(src_list[0])
    cs.create_lockfile(src_list[1])
    assert_true(os.path.isfile('/tmp/causync_src1.lock') and cs.lockfile_exists(src_list[0]))
    assert_true(os.path.isfile('/tmp/causync_src2.lock') and cs.lockfile_exists(src_list[1]))
    cs.remove_lockfile(src_list[0])
    cs.remove_lockfile(src_list[1])