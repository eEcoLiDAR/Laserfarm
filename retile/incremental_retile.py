#!/usr/bin/env python3




import  os, argparse,multiprocessing, time, traceback, shutil, json, math, datetime, sys, subprocess
import numpy as np
from pympc import utils
from pympc.generate_tiles import getTileIndex, getTileName



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
    parser.add_argument('-o','--output',default='',help='Output data folder for the different tiles',type=str, required=True)
    parser.add_argument('-t','--temp',default='',help='Temporal folder where required processing is done',type=str, required=True)
    parser.add_argument('-e','--extent',default='',help='XY extent to be used for the tiling, specify as "minX minY maxX maxY". maxX-minX must be equal to maxY-minY. This is required to have a good extent matching with the octree',type=str, required=True)
    parser.add_argument('-n','--number',default='',help='Number of tiles (must be the power of 4. Example: 4, 16, 64, 256, 1024, etc.)',type=int, required=True)
    parser.add_argument('-p','--proc',default=1,help='Number of processes [default is 1]',type=int)
    parser.add_argument('-l','--list',default=None,help='Optional inputlist of tiles to retile. Overrides dlconfig.')
    parser.add_argument('-i','--input',default=None,help='Optional path to input directory. Used if --list is provided. ignored otherwise. ')

    parser.add_argument('-cf','--dlconfigfile',default=None,help='File path to download run config file for which downloadLists are to be merged.')
    parser.add_argument('-nvm','--numbervms',default=1,help='number of vms on which retiling of given input is being run in parallel.')
    parser.add_argument('-v','--vmj',default=0,help='id of vm script is running on')
    parser.add_argument('-nid','--idnumber',default=0,help='identification number of VM, index into list of VM ids')

    return parser


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

    except IOError:
        print('Error opening config file')

    return downloadOutputDir, tag


def read_downloadList(downloadList):

    downloadDict = None
    success = False

    try:
        with open(downloadList,'r') as jf:
            downloadDict = json.load(jf)
            success=True

    except IOError:
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
            subInputList = InputList[i*lengthSubInputList:(i+1)*lengthSubInputList]
        else :
            subInputList = InputList[i*lengthSubInputList:]

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
            kill_received = True
        if inputFile == None:
            # If we receive a None job, it means we can stop
            kill_received = True
        else:
            # Get number of points and BBOX of this file
            print('checking for connection (webdav resp. outputFolder)')
            conn = check_for_webdav(outputFolder)

            if conn == True:

                (fCount, fMinX, fMinY, _, fMaxX, fMaxY, _, _, _, _, _, _, _) = utils.getPCFileDetails(inputFile)
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
                        utils.shellExecute('mkdir -p ' + tileFolder)
                    utils.shellExecute('cp ' + inputFile + ' ' + tileFolder)
                    tileFoldersTouched.append(os.path.basename(tileFolder))

                else:
                    # If not, we run PDAL gridder to split the file in pieces that can go to the tiles
                    tGCount, TFT = runPDALSplitter_incremental(processIndex, inputFile, outputFolder, tempFolder, minX, minY, maxX, maxY, axisTiles)
                    tileFoldersTouched.append(TFT)
                    if tGCount != fCount:
                        print ('WARNING: split version of ', inputFile, ' does not have same number of points (', tGCount, 'expected', fCount, ')')
                    resultsQueue.put([inputFile, processIndex, fCount, tileFoldersTouched])

            else:
                print('no connection to output. Tile was skipped. Proceeding')
                resultsQueue.put([inputFile,'-','-','Skipped, no connection'])


def runPDALSplitter_incremental(processIndex, inputFile, outputFolder, tempFolder, minX, minY, maxX, maxY, axisTiles):
    TFTouched =[]

    pTempFolder = tempFolder + '/' + str(processIndex)
    if not os.path.isdir(pTempFolder):
        utils.shellExecute('mkdir -p ' + pTempFolder)

    # Get the lenght required by the PDAL split filter in order to get "squared" tiles
    lengthPDAL = (maxX - minX) /  float(axisTiles)

    utils.shellExecute('pdal split -i ' + inputFile + ' -o ' + pTempFolder + '/' + os.path.basename(inputFile) + ' --origin_x=' + str(minX) + ' --origin_y=' + str(minY) + ' --length ' + str(lengthPDAL))
    tGCount = 0
    for gFile in os.listdir(pTempFolder):
        (gCount, gFileMinX, gFileMinY, _, gFileMaxX, gFileMaxY, _, _, _, _, _, _, _) = utils.getPCFileDetails(pTempFolder + '/' + gFile)
        # This tile should match with some tile. Let's use the central point to see which one
        pX = gFileMinX + ((gFileMaxX - gFileMinX) / 2.)
        pY = gFileMinY + ((gFileMaxY - gFileMinY) / 2.)
        tileFolder = outputFolder + '/' + getTileName(*getTileIndex(pX, pY, minX, minY, maxX, maxY, axisTiles))
        if not os.path.isdir(tileFolder):
            utils.shellExecute('mkdir -p ' + tileFolder)
        utils.shellExecute('mv ' + pTempFolder + '/' + gFile + ' ' + tileFolder + '/' + gFile)
        TFTouched.append(os.path.basename(tileFolder))
        tGCount += gCount
    return tGCount, TFTouched




def run_incremental(vmid, idnumber, tag, inputList, downloadOutputDir, outputFolder, tempFolder, extent, numberTiles, numberProcs):


    axisTiles = math.sqrt(numberTiles)
    if (not axisTiles.is_integer()) or (int(axisTiles) % 2):
        raise Exception('Error: Number of tiles must be the square of number which is power of 2!')
    axisTiles = int(axisTiles)

    # Create output and temporal folder
    utils.shellExecute('mkdir -p ' + outputFolder)
    utils.shellExecute('mkdir -p ' + tempFolder)

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
        processes.append(multiprocessing.Process(target=runProcess_incremental,
            args=(i, tasksQueue, resultsQueue, minX, minY, maxX, maxY, outputFolder, tempFolder, axisTiles)))
        processes[-1].start()

    # Get all the results
    tilesTouchedList=[]
    retiledList = []
    numPoints = 0
    for i in range(numInputFiles):
        results = resultsQueue.get()
        retiledList.append(results)
        tilesTouchedList.append(results[3])
        #(processIndex, inputFile, inputFileNumPoints, tileFoldersTouched) = resultsQueue.get()
        #numPoints += inputFileNumPoints
        numPoints += results[2]
        print ('Completed %d of %d (%.02f%%)' % (i+1, numInputFiles, 100. * float(i+1) / float(numInputFiles)))
    # wait for all users to finish their execution
    for i in range(numberProcs):
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
                time.sleep((cnt+1)*10)
                cnt+=1
            else:

                retry = False


    return reached



def main():


    updated_top10NL = None

    args = argument_parser().parse_args()

    if args.list == None:
        if args.input != None:
            print('WARNING: input list specified but will be ignored!!')

        print('retrieveing DL run info :')
        downloadOutputDir, tag = read_dl_config(args.dlconfigfile)
        print(downloadOutputDir)
        print(tag)

        try:
            updated_top10NL = get_updated_top10NL(downloadOutputDir,tag)
            print('updated_top10NL')
            print(updated_top10NL)

        except :
            print('get updated_top10NL failed')



    else:

        if args.input == None:
            print('No input directory specified but required if input list supplied. Exiting')
            return
        tag=os.path.basename(args.list)
        downloadOutputDir = args.input

        try:
            updated_top10NL = read_input_list(args.list)
            print('read {} as input list'.format(args.list))

        except:
            print('failed to read specified input list')



    print(' Invocation environment PATH variable')
    print(os.environ)






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
            t0 = time.time()

            print('Starting ' + os.path.basename(__file__) + '...')
            run_incremental(args.vmj,args.idnumber, tag, inputList, downloadOutputDir, args.output, args.temp, args.extent, args.number, args.proc)
            print( 'Finished in %.2f seconds' % (time.time() - t0))


        except:

            print('Execution failed!')
            print( traceback.format_exc())



    else:
        print('No input list. Nothing to do. Finished')




if __name__ == '__main__':
    main()
