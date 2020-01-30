#!/usr/bin/env python3




import  os, sys, argparse, multiprocessing, time, traceback
import struct, json, math, datetime, subprocess
import numpy as np



def argument_parser():


    parser = argparse.ArgumentParser(description="""This script is used to distribute the points of a bunch of LAS/LAZ files in
different tiles. The XY extent of the different tiles match the XY extent of the
nodes of a certain level of a octree defined by the provided bounding box (z
is not required by the XY tiling). Which level of the octree is matched
depends on specified number of tiles:
 - 4 (2X2) means matching with level 1 of octree
 - 16 (4x4) means matching with level 2 of octree
 and so on. """)
    #parser.add_argument('-i','--input',default='',help='Input data folder (with LAS/LAZ files)',type=str, required=True)
    #split configuration
    parser.add_argument('-o','--output',default='',help='Output data folder for the different tiles',type=str, required=True)
    parser.add_argument('-t','--temp',default='',help='Temporal folder where required processing is done',type=str, required=True)
    parser.add_argument('-e','--extent',default='',help='XY extent to be used for the tiling, specify as "minX minY maxX maxY". maxX-minX must be equal to maxY-minY. This is required to have a good extent matching with the octree',type=str, required=True)
    parser.add_argument('-n','--number',default='',help='Number of tiles (must be the power of 4. Example: 4, 16, 64, 256, 1024, etc.)',type=int, required=True)

    #input. Either downlod config file or input list input directory combination required
    parser.add_argument('-cf','--dlconfigfile',default=None,help='File path to download run config file for which downloadLists are to be merged.')
    parser.add_argument('-l','--list',default=None,help='Optional inputlist of tiles to retile. Overrides dlconfig.')
    parser.add_argument('-i','--input',default=None,help='Optional path to input directory. Used if --list is provided. ignored otherwise. ')
    # computational load distribution.
    #local node
    parser.add_argument('-p','--proc',default=1,help='Number of processes [default is 1]',type=int)
    #distributed setup. Passed for ease of invocation
    parser.add_argument('-nvm','--numbervms',default=1,help='number of vms on which retiling of given input is being run in parallel.')
    parser.add_argument('-v','--vmj',default=0,help='id of vm script is running on')
    parser.add_argument('-nid','--idnumber',default=0,help='identification number of VM, index into list of VM ids')

    return parser






def getTileIndex(pX, pY, minX, minY, maxX, maxY, axisTiles):
    xpos = int((pX - minX) * axisTiles / (maxX - minX))
    ypos = int((pY - minY) * axisTiles / (maxY - minY))
    if xpos == axisTiles: # If it is in the edge of the box (in the maximum side) we need to put in the last tile
        xpos -= 1
    if ypos == axisTiles:
        ypos -= 1
    return (xpos, ypos)


def getTileName(xIndex, yIndex):
    return 'tile_%d_%d' % (int(xIndex), int(yIndex))


#def shellExecute(command, showOutErr = False):
#    """ Execute the command in the SHELL and shows both stdout and stderr"""
#    print(command)
#    proc=subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#    (out,err) = proc.communicate()
#    r = '\n'.join((out.decode("utf-8") , err.decode("utf-8")))
#    if showOutErr:
#        print(r)
#    return r

def shellExecute(command, timeout = [None,None], showOutErr = False):
    """ Execute the command in the SHELL and shows both stdout and stderr. Willl retry timeout twice"""
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




def getPCFileDetails(absPath):
    """ Get the details (count numPoints and extent) of a LAS/LAZ file (using LAStools, hence it is fast)"""
    count = None
    (minX, minY, minZ, maxX, maxY, maxZ) = (None, None, None, None, None, None)
    (scaleX, scaleY, scaleZ) = (None, None, None)
    (offsetX, offsetY, offsetZ) = (None, None, None)

    command = 'lasinfo ' + absPath + ' -nc -nv -nco'
    r_command = shellExecute(command)
    ret = r_command[1]
    #for line in shellExecute(command).split('\n'):
    if r_command[0] == 0:
        for line in ret.split('\n'):
            if line.count('min x y z:'):
                [minX, minY, minZ] = line.split(':')[-1].strip().split(' ')
                minX = float(minX)
                minY = float(minY)
                minZ = float(minZ)
            elif line.count('max x y z:'):
                [maxX, maxY, maxZ] = line.split(':')[-1].strip().split(' ')
                maxX = float(maxX)
                maxY = float(maxY)
                maxZ = float(maxZ)
            elif line.count('number of point records:'):
                count = int(line.split(':')[-1].strip())
            elif line.count('scale factor x y z:'):
                [scaleX, scaleY, scaleZ] = line.split(':')[-1].strip().split(' ')
                scaleX = float(scaleX)
                scaleY = float(scaleY)
                scaleZ = float(scaleZ)
            elif line.count('offset x y z:'):
                [offsetX, offsetY, offsetZ] = line.split(':')[-1].strip().split(' ')
                offsetX = float(offsetX)
                offsetY = float(offsetY)
                offsetZ = float(offsetZ)
    else:
        raise Exception('Failure in lasinfo call')


    return (count, minX, minY, minZ, maxX, maxY, maxZ, scaleX, scaleY, scaleZ, offsetX, offsetY, offsetZ)

"""
this is currently duplicated from download_ahn3.py in the downloadAHN repository. to be refactored
"""
def build_filename(top10nlMapTile):
    filename='C_'+top10nlMapTile+'.LAZ'
    return filename




"""
this is currently duplicated from merge_downloadList.py in the downloadAHN repository. to be refactored
"""
def read_dl_config(configfile):

    downloadOutputDir = None
    #numberVMs = None
    tag = None

    try:
        with open(configfile,'r') as cfile:
            lines = cfile.readlines()
            for line in lines:
                key = line.split('=')[0]
                val = line.split('=')[1].rstrip()
                if key == 'OUTPUTDIRECTORY':
                    downloadOutputDir = val
                elif key == 'NUMBERVMS':
                    numberVMs = int(val)
                elif key == 'TAG' :
                    tag = val
                else:
                    pass

    except :
        print('Error opening config file')

    return downloadOutputDir, tag


def read_downloadList(downloadList):

    downloadDict = None
    success = False

    try:
        with open(downloadList,'r') as jf:
            downloadDict = json.load(jf)
            success=True

    except :
        print('failed to read downloadList at {}'.format(downloadList))

    return downloadDict, success


def construct_downloadList_filepath(downloadOutputDir, tag):

    downloadListFilePath = os.path.join(downloadOutputDir,'downloadList-'+str(tag)+'-latest.js')

    return downloadListFilePath




def construct_inputfile_paths(inputList,downloadOutputDir):

    inputFilePaths = []
    for input in inputList:
        fileName = build_filename(input)
        filePath = os.path.join(downloadOutputDir,fileName)
        inputFilePaths.append(filePath)

    return inputFilePaths



def read_input_list(inListPath):

    with open(inListPath,'r') as infile:
        lines = infile.readlines()

    tiles=[]
    for line in lines:
        tiles.append(line.strip())

    return tiles





def get_updated_top10NL(downloadOutputDir, tag):

    downloadListFilePath = construct_downloadList_filepath(downloadOutputDir, tag)

    downloadDict = None
    successDownloadDict = False
    top10NLDownloaded = None

    if os.path.isfile(downloadListFilePath):

        downloadDict, successDownloadDict = read_downloadList(downloadListFilePath)

    else:
        print('downloadList {} is not a file. Aborting.'.format(downloadListFilePath))

    if successDownloadDict == True:

        top10NLKeys = [key for key in downloadDict.keys()]

        top10NLDownloaded = [key for key in top10NLKeys if downloadDict[key][2]==True]

    return top10NLDownloaded


"""
this is currently duplicated from download_ahn3.py in the downloadAHN repository. to be refactored
"""
def split_input_list(numberSplits,inputList):

    subInputLists =[]
    lengthInputList = len(inputList)
    nSplit = int(numberSplits)
    lengthSubInputList = np.int(np.floor(lengthInputList/float(nSplit)))
    lengthFinSubInputList = lengthInputList - ((nSplit-1)*lengthSubInputList)

    for i in range(nSplit):
        if i != (nSplit-1):
            subInputList = inputList[i*lengthSubInputList:(i+1)*lengthSubInputList]
        else :
            subInputList = inputList[i*lengthSubInputList:]

        subInputLists.append(subInputList)

    return subInputLists


def runProcess_incremental(processIndex, tasksQueue, resultsQueue, minX, minY, maxX, maxY, outputFolder, tempFolder, axisTiles):
    kill_received = False
    while not kill_received:
        inputFile = None
        try:
            # This call will patiently wait until new job is available
            inputFile = tasksQueue.get()
        except:
            # if there is an error we will quit
            print('error in tasksQueue. Killing process {}.'.format(processIndex))
            kill_received = True

        if inputFile == None:
            # If we receive a None job, it means we can stop
            print('reached end of tasksQueue. Killing process {}.'.format(processIndex))
            kill_received = True
        else:
            #this checks whether the outputFolder can be reached. Used a sa proxy for the webdav,
            #as the inputfolder also resides there
            print('checking for connection')
            conn = check_for_webdav(outputFolder)

            if conn == True:

                try:
                    # Get number of points and BBOX of this file
                    print('here')
                    (fCount, fMinX, fMinY, _, fMaxX, fMaxY, _, _, _, _, _, _, _) = getPCFileDetails(inputFile)

                    #posMinXMinY = None
                    #posMinXMaxY = None
                    #posMaxXMinY = None
                    #posMaxXMaxY = None

                    if fCount != None:
                        print ('Processing', os.path.basename(inputFile), fCount, fMinX, fMinY, fMaxX, fMaxY)
                        # For the four vertices of the BBOX we get in which tile they should go
                        posMinXMinY = getTileIndex(fMinX, fMinY, minX, minY, maxX, maxY, axisTiles)
                        posMinXMaxY = getTileIndex(fMinX, fMaxY, minX, minY, maxX, maxY, axisTiles)
                        posMaxXMinY = getTileIndex(fMaxX, fMinY, minX, minY, maxX, maxY, axisTiles)
                        posMaxXMaxY = getTileIndex(fMaxX, fMaxY, minX, minY, maxX, maxY, axisTiles)

                        tileFoldersTouched =[]

                        if (posMinXMinY == posMinXMaxY) and (posMinXMinY == posMaxXMinY) and (posMinXMinY == posMaxXMaxY):
                        # If they are the same the whole file can be directly copied to the tile
                            tileFolder = outputFolder + '/' + getTileName(*posMinXMinY)


                            if not os.path.isdir(tileFolder):
                                r_mkdirTileFolder = shellExecute('mkdir -p ' + tileFolder)
                            print(datetime.datetime.now())
                            cpcmd='cp ' + inputFile + ' ' + tileFolder
                            r_cpInputFileTileFolder = shellExecute(cpcmd,timeout=[600,3])
                            if r_cpInputFileTileFolder[0] != 0:
                                raise Exception('{0} failed after {1} times {2} seconds'.format(cpcmd,timeout[1],timeout[0]))
                            else:
                                tileFoldersTouched.append(os.path.basename(tileFolder))

                        else:
                            # If not, we run PDAL gridder to split the file in pieces that can go to the tiles
                            tGCount, TFT = runPDALSplitter_incremental(processIndex, inputFile, outputFolder, tempFolder, minX, minY, maxX, maxY, axisTiles)
                            tileFoldersTouched.append(TFT)
                            if tGCount != fCount:
                                raise Exception('WARNING: split version of ', inputFile, ' does not have same number of points (', tGCount, 'expected', fCount, ')')

                        resultsQueue.put([inputFile, processIndex, fCount, tileFoldersTouched])

                    else:
                        print('reading inputFile {} with lasinfo failed, returning None'.format(inputFile))
                        resultsQueue.put([inputFile, -97 , -97, ['None return']])

                except:
                    print('Exception during processing of inputFile {}. Skipping...'.format(inputFile))
                    resultsQueue.put([inputFile, -98 , -98, ['Access exception']])

            else:
                print('no connection to output. Skipping inputFile {}'.format(inputFile))
                resultsQueue.put([inputFile, -99 , -99, ['Skipped, no connection']])


def runPDALSplitter_incremental(processIndex, inputFile, outputFolder, tempFolder, minX, minY, maxX, maxY, axisTiles):
    TFTouched =[]

    pTempFolder = tempFolder + '/' + str(processIndex)
    if not os.path.isdir(pTempFolder):
        shellExecute('mkdir -p ' + pTempFolder)

    # Get the lenght required by the PDAL split filter in order to get "squared" tiles
    lengthPDAL = (maxX - minX) /  float(axisTiles)

    pdalcmd = 'pdal split -i ' + inputFile + ' -o ' + pTempFolder + '/' + os.path.basename(inputFile) + ' --origin_x=' + str(minX) + ' --origin_y=' + str(minY) + ' --length ' + str(lengthPDAL)

    r_pdalcmd= shellExecute(pdalcmd,timeout=[1800,3])
    if r_pdalcmd[0] != 0 :
        raise Exception('Pdal split of {} failed. Aborting and moving to next'.format(inputFile))

    tGCount = 0
    ngfiles = len(os.listdir(pTempFolder))
    for gFile in os.listdir(pTempFolder):
        (gCount, gFileMinX, gFileMinY, _, gFileMaxX, gFileMaxY, _, _, _, _, _, _, _) = getPCFileDetails(pTempFolder + '/' + gFile)
        # This tile should match with some tile. Let's use the central point to see which one
        pX = gFileMinX + ((gFileMaxX - gFileMinX) / 2.)
        pY = gFileMinY + ((gFileMaxY - gFileMinY) / 2.)
        tileFolder = outputFolder + '/' + getTileName(*getTileIndex(pX, pY, minX, minY, maxX, maxY, axisTiles))
        if not os.path.isdir(tileFolder):
            shellExecute('mkdir -p ' + tileFolder)
        print(datetime.datetime.now())
        cmdMvGfileTileFolder = 'mv ' + pTempFolder + '/' + gFile + ' ' + tileFolder + '/' + gFile
        r_cmdMvGfileTileFolder = shellExecute(cmdMvGfileTileFolder,timeout=[1800,3])
        if r_cmdMvGfileTileFolder[0] != 0:
            raise Exception('Exception while moving {0} of {1} total. Ab'.format(gfile,ngfiles))
        else:
            TFTouched.append(os.path.basename(tileFolder))
        tGCount += gCount
    return tGCount, TFTouched




def run_incremental(vmid, idnumber, tag, inputList, downloadOutputDir, outputFolder, tempFolder, extent, numberTiles, numberProcs):


    axisTiles = math.sqrt(numberTiles)
    if (not axisTiles.is_integer()) or (int(axisTiles) % 2):
        raise Exception('Error: Number of tiles must be the square of number which is power of 2!')
    axisTiles = int(axisTiles)

    # Create output and temporal folder
    shellExecute('mkdir -p ' + outputFolder)
    shellExecute('mkdir -p ' + tempFolder)

    (minX, minY, maxX, maxY) = extent.split(' ')
    minX = float(minX)
    minY = float(minY)
    maxX = float(maxX)
    maxY = float(maxY)

    if (maxX - minX) != (maxY - minY):
        raise Exception('Error: Tiling requires that maxX-minX must be equal to maxY-minY!')


    inputFiles = construct_inputfile_paths(inputList,downloadOutputDir)

    numInputFiles = len(inputFiles)
    print('processing {} input tiles'.format(numInputFiles))

    # Create queues for the distributed processing
    tasksQueue = multiprocessing.Queue() # The queue of tasks (inputFiles)
    resultsQueue = multiprocessing.Queue() # The queue of results

    # Add tasks/inputFiles
    for i in range(numInputFiles):
        tasksQueue.put(inputFiles[i])
    for i in range(numberProcs): #we add as many None jobs as numberProcs to tell them to terminate (queue is FIFO)
        tasksQueue.put(None)

    processes = []
    # We start numberProcs users processes
    for i in range(numberProcs):
        print('appending process {}'.format(i))
        processes.append(multiprocessing.Process(target=runProcess_incremental,
            args=(i, tasksQueue, resultsQueue, minX, minY, maxX, maxY, outputFolder, tempFolder, axisTiles)))
        print('starting process {}'.format(i))
        processes[-1].start()

    # Get all the results
    tilesTouchedList=[]
    retiledList = []
    numPoints = 0
    for i in range(numInputFiles):
        #this should wait for a result
        print('retrieving results for input (file) {}'.format(i))
        results = resultsQueue.get()
        retiledList.append(results)
        tilesTouchedList.append(results[3])
        #(processIndex, inputFile, inputFileNumPoints, tileFoldersTouched) = resultsQueue.get()
        #numPoints += inputFileNumPoints
        numPoints += results[2]
        print ('Completed %d of %d (%.02f%%)' % (i+1, numInputFiles, 100. * float(i+1) / float(numInputFiles)))
    # wait for all users to finish their execution
    for i in range(numberProcs):
        print('joining processes')
        processes[i].join()



    retiledDict = {retiledListElement[0]:retiledListElement[1:] for retiledListElement in retiledList}

    tilesTouchedListFlat =[]
    for ttl in tilesTouchedList:
        if len(ttl) > 0:
            if isinstance(ttl,list):
                tilesTouchedListFlat.extend(ttl)
            else:
                tilesTouchedListFlat.append(ttl)
        else:
            pass



    now = datetime.datetime.now()
    legacyretiledjs = '/retiledList_vm'+str(vmid)+'-'+str(tag)+'-'+str(now.timestamp())+'.js'
    currentretiledjs = '/retiledList_vm'+str(vmid)+'-'+str(tag)+'-latest.js'

    legacytilestouchedjs = '/tilestouched_vm'+str(vmid)+'-'+str(tag)+'-'+str(now.timestamp())+'.js'
    currenttilestouchedjs ='/tilestouched_vm'+str(vmid)+'-'+str(tag)+'-latest.js'

    legacytilestouchedflatjs = '/tilestouchedflat_vm'+str(vmid)+'-'+str(tag)+'-'+str(now.timestamp())+'.js'
    currenttilestouchedflatjs ='/tilestouchedflat_vm'+str(vmid)+'-'+str(tag)+'-latest.js'


    with  open(outputFolder+legacyretiledjs,'w') as dlfile:
        dlfile.write(json.dumps(retiledDict,indent=4))

    with  open(outputFolder+currentretiledjs,'w') as dlfile:
        dlfile.write(json.dumps(retiledDict,indent=4))


    with  open(outputFolder+legacytilestouchedjs,'w') as dlfile:
        dlfile.write(json.dumps(tilesTouchedList,indent=4))

    with  open(outputFolder+currenttilestouchedjs,'w') as dlfile:
        dlfile.write(json.dumps(tilesTouchedList,indent=4))

    with  open(outputFolder+legacytilestouchedflatjs,'w') as dlfile:
        dlfile.write(json.dumps(tilesTouchedListFlat,indent=4))

    with  open(outputFolder+currenttilestouchedflatjs,'w') as dlfile:
        dlfile.write(json.dumps(tilesTouchedListFlat,indent=4))



    # Write the tile.js file with information about the tiles
    cFile = open(outputFolder + '/tiles.js', 'w')
    d = {}
    d["NumberPoints"] = numPoints
    d["numXTiles"] = axisTiles
    d["numYTiles"] = axisTiles
    d["boundingBox"] = {'lx':minX,'ly':minY,'ux':maxX,'uy':maxY}
    cFile.write(json.dumps(d,indent=4,sort_keys=True))
    cFile.close()




def check_for_webdav(downloadOutputDir):
    cnt = 0
    reached = False
    retry = True
    while retry == True:
        ret = os.path.isdir(downloadOutputDir)
        print(ret)
        if ret == True:
            reached = True
            retry = False

        else:
            if cnt < 6:
                print(cnt)
                time.sleep((cnt+1)*30)
                cnt+=1
            else:
                retry = False

        return reached




def main():

    print(' Invocation environment PATH variable')
    print(os.environ)

    updated_top10NL = None

    args = argument_parser().parse_args()

    if args.list == None:
        if args.input != None:
            print('WARNING: input list specified but will be ignored!!')

        print('retrieveing DL run info :')
        downloadOutputDir, tag = read_dl_config(args.dlconfigfile)
        print('downloadOutputDir: '.format(downloadOutputDir))
        print('tag: '.format(tag))

        try:
            updated_top10NL = get_updated_top10NL(downloadOutputDir,tag)
            print('updated_top10NL')
            print(updated_top10NL)

        except :
            print('get updated_top10NL failed')
            print('Exiting...')




    else:

        if args.input == None:
            print('No input directory specified but required if input list supplied. ')
            print('Exiting...')

        #set tag and downloadOutputDir (input) for input list case
        tag=os.path.basename(args.list)
        downloadOutputDir = args.input

        try:
            print('read {} as input list'.format(args.list))
            updated_top10NL = read_input_list(args.list)


        except:
            print('failed to read specified input list {}'.format(args.list))
            print('Exiting...')






    if updated_top10NL != None:


        updated_top10NL_splits = split_input_list(args.numbervms,updated_top10NL)
        print('updated_top10NL_splits')
        print(updated_top10NL_splits)

        inputList = updated_top10NL_splits[int(args.idnumber)]
        print('inputList')
        print(inputList)
        #with open('out.out','w+') as of:
        #    print(updated_top10NL,file=of)
        #    print(updated_top10NL_splits,file=of)
        #    print(inputList,file=of)
        #    print(args.dlconfigfile,file=of)
        #    print(int(args.vmj),file=of)
        #    print(args.extent,file=of)

        try:


            print('Starting ' + os.path.basename(__file__) + '...')
            run_incremental(args.vmj,args.idnumber, tag, inputList, downloadOutputDir, args.output, args.temp, args.extent, args.number, args.proc)
            print( 'Finished in %.2f seconds' % (time.time() - t0))


        except:

            print('Execution failed!')
            print( traceback)



    else:
        print('No input list. Nothing to do. Finished')




if __name__ == '__main__':
    main()
