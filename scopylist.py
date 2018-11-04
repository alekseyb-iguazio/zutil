#!/usr/bin/env python

import os
import sys
import string
import shutil
import argparse

def is_remote(url) :
    if url.find(":"):
        return True
    return False

def join_file_name(path, name) :
    if not is_remote(path) :
        path = os.path.abspath(path)
        return os.path.join(path, name)
    if not path[-1:] == '/' :
        path = path + "/"
    return path + name

parser = argparse.ArgumentParser()
parser.add_argument("list", help="files list")
parser.add_argument("-s", "--source", help="source directoty")
parser.add_argument("-d", "--destination", help="destination directoty")
parser.add_argument("-p", "--passwd", help="password")
args = parser.parse_args()  # get command line argumets


src_path = None
dst_path = None
list_path = None

if args.source :
    src_path = args.source
if args.destination :
    dst_path = args.destination
if os.path.exists(args.list) :
    list_path = os.path.abspath(args.list)

if not src_path or (not is_remote(src_path) and not os.path.isdir(src_path)):
    print("Source path is not found!")
    sys.exit(1)


if not dst_path or (not is_remote(dst_path) and not os.path.isdir(dst_path)):
    print(dst_path) # + " " +  is_remote(dst_path))
    print("Destination path is not found!")
    sys.exit(1)

if not os.path.isfile(list_path):
    print("Files list is not found!")
    sys.exit(1)

if not args.passwd:
    print("Password is not defined!")
    sys.exit(1)

file_list = open(list_path, "r")
buffer_list = file_list.readlines()
file_list.close()

for line in buffer_list:
    if not line.lstrip():  # skip the blank lines
        continue
    line = line.strip()
    line ="sshpass -p " + args.passwd + " scp " + join_file_name(src_path,line) + " " + join_file_name(dst_path,line)
    print(line)
    os.system(line)