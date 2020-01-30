#!/usr/bin/env python3

import  os, argparse,multiprocessing, time, traceback, shutil, json, math, datetime, sys, subprocess
import numpy as np
from pympc import utils


def argument_parser():
    parser = argparse.ArgumentParser(description="""This script is used to merge subsegments of retiled tiles.""")
    #parser.add_argument('-i','--input',default='',help='Input data folder (with LAS/LAZ files)',type=str, required=True)
    parser.add_argument('-o','--outputDir',default='',help='Output data folder for the merged',type=str, required=True)
    parser.add_argument('-t','--tempDir',default='',help='Temporary folder where required processing is done',type=str, required=True)
    parser.add_argument('-i','--inputDir',default='',help='Input directory where with subdirectories corresponding to tiles',type=str, required=True)
    parser.add_argument('-p','--proc',default=1,help='Number of processes [default is 1]',type=int)
    parser.add_argument('-l','--inputList',default=None,help='List of tiles (retiled) to be merged. These should exist as subdirectories in inputDir.',type=str)

    parser.add_argument('-cf','--dlconfigfile',default=None,help='File path to download run config file for which downloadLists are to be merged.',type=str)
    parser.add_argument('-nvm','--numbervms',default=1,help='number of vms on which merging of given input is being run in parallel.',type=int)
    parser.add_argument('-v','--vmj',default=0,help='id of vm script is running on')
    parser.add_argument('-nid','--idnumber',default=0,help='identification number of VM, index into list of VM ids',type=int)

    return parser



def get_input_list(inputList):

    inList= None

    if inputList is not None:
        try:
            with open(inputList,'r') as inListFile:
                lines = inListFile.readlines()
            inList=[]
            for line in lines:
                inList.append(line.strip())
        except:
            print('failure to open specified input list')
            print( traceback.format_exc())


    else:
        print('still to do')
        #TODO add json reader of touched tiles

    return inList



"""
this is currently duplicated from download_ahn3.py in the downloadAHN repository. to be refactored
"""
def split_input_list(numberSplits,inList):

    subInLists =[]
    lengthInList = len(inList)
    nSplit = int(numberSplits)
    lengthSubInList = np.int(np.floor(lengthInList/float(nSplit)))
    lengthFinSubInList = lengthInList - ((nSplit-1)*lengthSubInList)

    for i in range(nSplit):
        if i != (nSplit-1):
            subInList = inList[i*lengthSubInList:(i+1)*lengthSubInList]
        else :
            subInList = inList[i*lengthSubInList:]

        subInLists.append(subInList)

    return subInLists




def get_paths(tile, inputDir,outputDir,tempDir):

    inPath = None
    tempPath = None
    outPath = None
    #check dirs
    outputDirExists = os.path.isdir(outputDir)
    tempDirExists = os.path.isdir(tempDir)

    if not outputDirExists:
        utils.shellExecute('mkdir -p ' + outputDir)
    if not tempDirExists:
        utils.shellExecute('mkdir -p ' + tempDir )

    inPath = os.path.join(inputDir,tile)

    if not os.path.isdir(inPath):
        print('not a valid input path: {}'.format(inPath))
        inPath = None

    outPath = os.path.join(outputDir,tile+'.LAZ')
    tempPath = os.path.join(tempDir,tile+'.LAZ')

    return inPath, tempPath, outPath




def run(inList, numberVms, vmId, vmNid, inputDir, outputDir, tempDir, numberProcs):

    lenInList = len(inList)
    # Create queues for the distributed processing
    tasksQueue = multiprocessing.Queue() # The queue of tasks (inputFiles)
    resultsQueue = multiprocessing.Queue() # The queue of results

    # Add tasks/inputFiles
    for i in range(lenInList):
        tasksQueue.put(inList[i])
    for i in range(numberProcs): #we add as many None jobs as numberProcs to tell them to terminate (queue is FIFO)
        tasksQueue.put(None)

    processes = []
    # We start numberProcs users processes
    for i in range(numberProcs):
        processes.append(multiprocessing.Process(target=run_merge_process,
            args=(i, tasksQueue, resultsQueue, inputDir, outputDir, tempDir )))
        processes[-1].start()

    resultsList =[]
    for i in range(lenInList):
        result = resultsQueue.get()
        resultsList.append(result)

    # wait for all users to finish their execution
    for i in range(numberProcs):
        processes[i].join()

    resultsDict = {resultsListElement[0]:resultsListElement[1:] for resultsListElement in resultsList}

    now = datetime.datetime.now()

    #legacymergedjs = '/mergedList_vm'+str(vmId)+'-'+str(tag)+'-'+str(now.timestamp())+'.js'
    #currentmergedjs = '/mergedList_vm'+str(vmId)+'-'+str(tag)+'-latest.js'
    legacymergedjs = '/mergedList_vm'+str(vmId)+'-'+str(now.timestamp())+'.js'
    currentmergedjs = '/mergedList_vm'+str(vmId)+'-latest.js'

    with  open(outputDir+legacymergedjs,'w') as mfile:
        mfile.write(json.dumps(resultsDict,indent=4))

    with  open(outputDir+currentmergedjs,'w') as mfile:
        mfile.write(json.dumps(resultsDict,indent=4))




def run_merge_process(i,tasksQueue, resultsQueue,inputDir,outputDir,tempDir):

    kill_received = False
    while not kill_received:
        inTile = None
        try:
            # This call will patiently wait until new job is available
            inTile = tasksQueue.get()
        except:
            # if there is an error we will quit
            kill_received = True
        if inTile == None:
            # If we receive a None job, it means we can stop
            kill_received = True
        else:

            conn = True#check_for_webdav(inputDir)

            if conn == True:

                inPath, tempPath, outPath = get_paths(inTile,inputDir,outputDir,tempDir)
                print(inPath)

                if inPath is not None:

                    command1 = "lasmerge -i " + inPath + "/*.LAZ -o " + tempPath + " -rescale 0.01 0.01 0.01"
                    command2 = "mv " + tempPath + " " + outPath
                    re1 = utils.shellExecute(command1)
                    re2 = utils.shellExecute(command2)
                    print(re1)
                    print(re2)
                    #(return_cmd1, err_cmd1) = utils.shellExecute(command1)
                    #(return_cmd2, err_cmd2) = utils.shellExecute(command2)

                    returns = [inTile, i, re1, re2]
                    resultsQueue.put(returns)

                else:
                    print('No valid input path specified. Proceeding to next specified tile.')
                    resultsQueue.put([inTile,i,'WARINING: invalid inPath',''])

            else:
                print('connection to inputDir {} failed'.format(inputDir))
                print('moving to next tile...')
                resultsQueue.put([intile, i, 'CONNECTION FAILED',''])


def check_for_webdav(inputPath):
    cnt = 0
    reached = False
    retry = True
    while retry == True:
        ret = os.path.isdir(inputPath)
        if ret == True:
            reached == True
            retry == False
        else:
            if cnt < 6:
                time.sleep((cnt+1)*10)
                cnt+=1
            else:
                retry = False
                print('failed to establish connection to inputPath {}'.format(inputPath))
                print('skipping...')

    return reached





def main():

    args = argument_parser().parse_args()

    inputList = get_input_list(args.inputList)

    inputListSplits = split_input_list(args.numbervms,inputList)
    print(inputListSplits)

    subInputList = inputListSplits[int(args.idnumber)]
    print(subInputList)

    try:
        t0 = time.time()
        print('starting tile merging...')

        run(subInputList, args.numbervms, args.vmj, args.idnumber, args.inputDir, args.outputDir, args.tempDir,args.proc)

        print('finished in %.2f seconds' % (time.time() - t0))

    except:
        print('Execution of merging failed!')
        print( traceback.format_exc())





if __name__ == '__main__':
    main()
