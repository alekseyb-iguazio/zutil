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
from testosterone.recovery_utils import query_node_state_util, recovery_utils_init
import paths
import time
from testosterone.data_dog_pytest_report import datadog_report as datadog_report
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


#@pytest.mark.timeout(500)
def test_db_fill(datadog_report):
    test_name = 'test_db_fill'

    log.info(test_name)

    conf = dict()
    conf['node_config_path'] = NODE_CONFIG_CMD
    conf['bridge_config_path'] = BRIDGE_CONFIG_CMD
    conf['load_node'] = False
    conf['node_valgrind'] = run_node_valgrind
    conf['num_creates'] = 20

    plat = Platform(paths, [conf], test_name)
    node = plat.nodes[0]

    node.set_current_container(1)
    path1 = "/hello222.bin"
    file1 = "hello222.bin"
    if node.vn_file_exists(path1):
        node.vn_delete_file(path1)

    write_buffer = "0123456789"
    offset = 1234
    file_handle = node.vn_open_file(path1, os.O_CREAT | os.O_RDWR, stat.S_IRWXU | stat.S_IRGRP)
    assert len(write_buffer) == node.vn_write_file(file_handle, offset, len(write_buffer), write_buffer)
    assert write_buffer == node.vn_read_file(file_handle, offset, len(write_buffer))
    assert offset + len(write_buffer) == node.vn_get_size_by_fd(file_handle)

    node.vn_close_file(file_handle)
    assert node.vn_file_exists(path1)

    node.set_current_container(2)
    path2 = "/hello333.bin"
    file2 = "hello333.bin"
    if node.vn_file_exists(path2):
        node.vn_delete_file(path2)

    write_buffer = "9876543210"
    offset = 4321
    file_handle = node.vn_open_file(path2, os.O_CREAT | os.O_RDWR, stat.S_IRWXU | stat.S_IRGRP)
    assert len(write_buffer) == node.vn_write_file(file_handle, offset, len(write_buffer), write_buffer)
    assert write_buffer == node.vn_read_file(file_handle, offset, len(write_buffer))
    assert offset + len(write_buffer) == node.vn_get_size_by_fd(file_handle)

    node.vn_close_file(file_handle)
    assert node.vn_file_exists(path2)

    put_path = "/a/b/c/"
    buff = "1234567890"
    num_puts = 100

    for d in xrange(num_puts):
        objname = put_path + str(d)
        assert node.vn_object_put(objname, buff, 0)

    attrs_objname = "attrs_obj1.test"
    attrs_objpath = "/attrs_obj1.test"
    assert node.vn_object_put(attrs_objpath, "unimportant", len(buff))

    num = 17
    key = {'name': 'A_55', 'schemaId': 0}
    attr = {'key': key, 'value': {'qword': num}}

    empty_filter = {'astRoot': {'nil': None}}
    status, _ = node.vn_object_item_update(
        "/", attrs_objpath[1:len(attrs_objpath)], {'keyValuePairs': [attr]}, empty_filter, 'overwriteEntireRow')
    assert status == 0

    resp, values, _ = node.vn_object_get_attrs(attrs_objpath, [key])

    assert values.values[0].key.name == key["name"]
    assert values.values[0].key.schemaId == key['schemaId']
    assert values.values[0].value.qword == num

    # TODO - no way to validate this, the attributes are checked on the FS bypassing the node layer
    # assert node.vn_file_exists(path1)

    #node.set_current_container(2)
    # TODO - no way to validate this, the attributes are checked on the FS bypassing the node layer
    # assert node.vn_file_exists(path2)

    # fill all 8 containers and then delete some
    # it couses free memory fragmetation needed for
    # pptr heap recovery test
    for  icontainer in xrange(3, 20):
        node.set_current_container(icontainer)
        add_objects(node, 1, 200)

    plat.tearDown(False)

    # start reload
    #conf['load_node'] = True
    #plat = Platform(paths, [conf], test_name)

    #reload_node = plat.nodes[0]

    #plat.tearDown()

if __name__ == '__main__':
    from testosterone.PyTestRunner import test_main
    test_main()
