#!/usr/bin/env python
import os
import stat
import sys
from glob import glob
import pytest
from testosterone.log import log
from testosterone.Platform import Platform
from testosterone.helper import get_no_valgrind, pyv3io_binding
from testosterone.BridgeAdapterSim import BridgeAdapterSim, SyncHandler
from testosterone.recovery_utils import query_node_state_util, recovery_utils_init, trigger_crash_point_once_without_mds, set_n_retries, wait_for_recovery_without_mds
import paths
import time
from testosterone.data_dog_pytest_report import datadog_report as datadog_report
from testosterone.helper import try_until_timeout
from subprocess import Popen, PIPE


SRC_DIR = sys.argv[1]
BIN_DIR = sys.argv[2]

paths = paths.get_paths(BIN_DIR, SRC_DIR)
recovery_utils_init(paths)

NODE_CONFIG_CMD = os.path.join(SRC_DIR, paths.node_directory_src_relative_path, 'config', 'node_config.json')
NODE_CONFIG_EMERGENCY_SHUTDOWN_CMD = os.path.join(SRC_DIR, paths.node_directory_src_relative_path, 'config', 'node_config_emergency_shutdown.json')
NODE_CONFIG_DEFAULT_CMD = os.path.join(SRC_DIR, paths.node_directory_src_relative_path, 'config', 'node_config_default.json')
BRIDGE_CONFIG_CMD = os.path.join(SRC_DIR, paths.bridge_directory_src_relative_path, 'config', 'bridge_config.json')

run_node_valgrind = not get_no_valgrind(BIN_DIR, False)


def add_objects(node, imin, imax):
    put_path = "/a/b/c/"
    buff = ""
    for d in xrange(imin, imax):
        objname = put_path + str(d)
        buff = buff + "begin{}end".format(d)
        assert node.vn_object_put(objname, buff, 0)


def damage_pptr_files(pptr_file_path, damage_left_percents=1, damage_right_percents=0, damage_parent_percents=0):
    log.info("damage pptr heap file {}".format(pptr_file_path))
    to_exec = [paths.node_pptr_damage_path]
    to_exec.extend(['-i', pptr_file_path])
    if damage_left_percents > 0:
        to_exec.extend(['-l %d' % damage_left_percents])
    if damage_right_percents > 0:
        to_exec.extend(['-r %d' % damage_right_percents])
    if damage_parent_percents > 0:
        to_exec.extend(['-p %d' % damage_parent_percents])
    proc = Popen(to_exec, stdout=PIPE, stderr=PIPE)
    stdoutdata, stderrdata = proc.communicate()
    log.info("node_pptr_damage stdoutdata:\n %s", stdoutdata)


def fix_pptr_files(pptr_file_path):
    log.info("fix pptr heap file {}".format(pptr_file_path))
    to_exec = [paths.node_pptr_util_path]
    to_exec.extend(['-i', pptr_file_path])
    to_exec.extend(['-f'])
    proc = Popen(to_exec, stdout=PIPE, stderr=PIPE)
    stdoutdata, stderrdata = proc.communicate()
    log.info("node_pptr_util stdoutdata:\n %s", stdoutdata)


def overwrite_pptr_files(node):
    pptr_files_list = list()
    pattern_list = ['vn*-backing_file_long_lived.bin']
    metadata_paths = set()
    for directory in node.node_conf.get_service_metadata_paths():
        metadata_paths.add(directory)
    for directory in metadata_paths:
        for pattern in pattern_list:
            for filename in glob(os.path.join(directory, pattern)):
                if os.path.isfile(filename):
                    damage_pptr_files(filename, 20, 20)
                    fix_pptr_files(filename)


def empty_metadata_paths(node):
    metadata_paths = set()
    for directory in node.node_conf.get_service_metadata_paths():
        metadata_paths.add(directory)
    for directory in metadata_paths:
        assert os.system("rm -rf " + directory + "/*") == 0


def remove_metadata_paths(node):
    metadata_paths = set()
    for directory in node.node_conf.get_service_metadata_paths():
        metadata_paths.add(directory)
    for directory in metadata_paths:
        assert os.system("rm -rf " + directory) == 0


def create_persistent_metadata_paths(node):
    for directory in node.node_conf.get_service_persistent_metadata_paths():
        os.system("mkdir " + directory)


def remove_persistent_metadata_paths(node):
    for directory in node.node_conf.get_service_persistent_metadata_paths():
        assert os.system("rm -rf " + directory) == 0


def delete_container(node, index):
    sim = node.node_services.sim
    handler = SyncHandler()

    handler.reset()
    sim.send_delete_container(index, handler)
    assert handler.status == node.node_services.sim.platform_messages.MessagingResponse.Status.Success.noContent

    while True:
        handler.reset()
        sim.send_container_mapping_get(index, handler)

        if handler.status == node.node_services.sim.platform_messages.MessagingResponse.Status.Errors.notFound:
            break

        if handler.status == node.node_services.sim.platform_messages.MessagingResponse.Status.Errors.gone:
            time.sleep(0.1)
            continue

        assert 0, 'Got invalid response from node'

@pytest.mark.timeout(500)
def test_read_test(datadog_report):
    test_name = 'test_read_test'

    log.info(test_name)
    NODE_CONFIG_CMD_MD_SYNC_TEST = os.path.join(
        SRC_DIR,
        paths.node_directory_src_relative_path,
        'config',
        'node_config_with_persistent_metadata_path.json')

    conf = dict()
    conf['node_config_path'] = NODE_CONFIG_CMD
    conf['bridge_config_path'] = BRIDGE_CONFIG_CMD
    conf['load_node'] = True
    conf['node_valgrind'] = run_node_valgrind
    conf['num_creates'] = 20
   
    # start reload
    plat = Platform(paths, [conf], test_name)

    reload_node = plat.nodes[0]

    reload_node.set_current_container(1)

    path1 = "/hello222.bin"
    path2 = "/hello333.bin"
    file1 = "hello222.bin"
    file2 = "hello333.bin"
    num = 17
    key = {'name': 'A_55', 'schemaId': 0}
    attr = {'key': key, 'value': {'qword': num}}
    attrs_objname = "attrs_obj1.test"
    attrs_objpath = "/attrs_obj1.test"

    put_path = "/a/b/c/"
    buff = "1234567890"
    num_puts = 100

    assert reload_node.vn_file_exists(path1)

    write_buffer = "0123456789"
    offset = 1234
    file_handle = reload_node.vn_open_file(path1, os.O_APPEND | os.O_RDWR, stat.S_IRWXU | stat.S_IRGRP)
    assert write_buffer == reload_node.vn_read_file(file_handle, offset, len(write_buffer))
    assert offset + len(write_buffer) == reload_node.vn_get_size_by_fd(file_handle)

    reload_node.vn_close_file(file_handle)

    res, read_dir_data = reload_node.vn_list_dir_entries("/")
    # TODO - no way to validate this, the attributes are checked on the FS bypassing the node layer
    assert read_dir_data.dirlist[0].name == file1
    assert reload_node.vn_file_exists(path1)

    reload_node.set_current_container(2)
    assert reload_node.vn_file_exists(path2)

    write_buffer = "9876543210"
    offset = 4321
    file_handle = reload_node.vn_open_file(path2, os.O_APPEND | os.O_RDWR, stat.S_IRWXU | stat.S_IRGRP)
    assert write_buffer == reload_node.vn_read_file(file_handle, offset, len(write_buffer))
    assert offset + len(write_buffer) == reload_node.vn_get_size_by_fd(file_handle)

    reload_node.vn_close_file(file_handle)

    res, read_dir_data = reload_node.vn_list_dir_entries("/")
    # skip "." ".."
    # TODO - no way to validate this, the attributes are checked on the FS bypassing the node layer
    assert set([e.name for e in read_dir_data.dirlist]) == {file2, attrs_objname}
    assert reload_node.vn_file_exists(path2)

    for d in xrange(num_puts):
        objname = put_path + str(d)
        response, data, _ = reload_node.vn_object_get(objname, 100, 0)
        assert response
        assert data == buff
        #assert reload_node.vn_object_put(objname, buff, 0)

    #objname = put_path + 'ddd/aaa'
    #assert reload_node.vn_object_put(objname, buff, 0)

    resp, values, _ = reload_node.vn_object_get_attrs(attrs_objpath, [key])

    assert values.values[0].key.name == key["name"]
    assert values.values[0].key.schemaId == key['schemaId']
    assert values.values[0].value.qword == num

    plat.tearDown(False)

    #remove_metadata_paths(reload_node)
    #remove_persistent_metadata_paths(node)


if __name__ == '__main__':
    from testosterone.PyTestRunner import test_main
    test_main()
