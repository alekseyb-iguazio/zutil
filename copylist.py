#!/usr/bin/env python

import os
import sys
import string
import shutil
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("list", help="files list")
parser.add_argument("-s", "--source", help="source directoty")
parser.add_argument("-d", "--destination", help="destination directoty")
args = parser.parse_args()  # get command line argumets


src_path = None
dst_path = None
list_path = None

if os.path.exists(args.source) :
    src_path = os.path.abspath(args.source)
if os.path.exists(args.destination) :
    dst_path = os.path.abspath(args.destination)
if os.path.exists(args.list) :
    list_path = os.path.abspath(args.list)

if not os.path.isdir(src_path):
    print("Source path is not found!")
    sys.exit(1)
    
if not os.path.isdir(dst_path):
    print("Destination path is not found!")
    sys.exit(1)

if not os.path.isfile(list_path):
    print("Files list is not found!")
    sys.exit(1)


file_list = open(list_path, "r")
buffer_list = file_list.readlines()
file_list.close()

for line in buffer_list:
    if not line.lstrip():  # skip the blank lines
        continue
    line = line.strip()
    line = "cp " + os.path.join(src_path,line) + " " + os.path.join(dst_path,line)
    print(line)
    os.system(line)