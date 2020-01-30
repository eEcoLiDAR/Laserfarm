#!/usr/bin/env python3

import os, sys, subprocess, traceback, time, datetime, math, multiprocesssing



def shellExecute(command, timeout = [None,None], showOutErr = False):
    """ Execute the command in the SHELL and shows both stdout and stderr. Will optionally retry ntimess and timeout"""
    print(command)
    proc = subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if timeout[0] != None:
        retry = True
        cnt=0
        while retry == True:
            if cnt == timeout[1]:
                proc.kill
                (out,err) = proc.communicate()
                retry = False
            else:
                try:
                    (out,err) = proc.communicate(timeout=timeout[0])
                    retry=False
                except :#subprocess.TimeoutExpired:
                    cnt+=1
    else:
        (out,err) = proc.communicate()

    rcode = proc.returncode
    print((out,err))
    print(rcode)

    r = '\n'.join((out.decode("utf-8") , err.decode("utf-8")))
    if showOutErr:
        print(r)
    return [rcode,r]


def read_input_list():



def split_input_list():


def create_input_paths(inputList,args,config):


get_outDict_paths(args,config,timestampNow):


check_input_access(inputFile,waitLoop=repeat):


local_copy_inputFile(inputFile,config):


remote_copy_outputFile(localOutputFile,args,config):


remove_local_outPutFile(localOutputFile):
