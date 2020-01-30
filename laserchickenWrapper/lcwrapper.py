#!/usr/bin/env python3
import sys, os, time, traceback, argparse, multiprocessing
import importsetup
import configparser
import laserchickenmodes as lcm
import wrapperutils as wu

path_of_laserchicken = importsetup.setlaserchickenpath()
sys.path.insert(0,path_of_laserchicken)



def argument_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('-m','--mode',default='',help='Mode in which to run Laserchicken. Possibilities are "norm" and "feat"', required=True)
    parser.add_argument('-cf','--configFile',default='',help='Configuration file for run',required=True)
    parser.add_argument('-vmid','--vmIdentity',default=1,help='identity of virtual machine')
    parser.add_argument('-idx','--vmIndex',default=0,help='index of VM in list of VMs')
    parser.add_argument('-nvm','--numberVms',default=1,help='number of VMs for distributed execution')
    parser.add_argument('-np','--nprocesses',default=1,help='number of processses')

    return parser



def parse_config(configfile):
    config = configparser.ConfigParser()
    config.read(configfile)
    return config



def invokeLaserchicken(localInputFile,args,config,resultsQueue):

    if args.mode=='norm':
        localOutputFile = lcm.normalize(localInputFile,args,config)
    elif args.mode=='feat':
        localOutputFile = lcm.featureextraction(localInputFile,args,config)
    else:
        raise Exception('No valid laserchicken mode selected')


    return localOutputFile




def run(inputList,args,config):

    wu.create_file_structures(args,config)

    inputFiles = wu.create_input_paths(inputList,args,config)
    numInputFiles = len(inputFiles)
    numberProcs = int(args.nprocesses)

    print('processing {} input tiles'.format(numInputFiles))

    # Create queues for the distributed processing
    tasksQueue = multiprocessing.Queue() # The queue of tasks (inputFiles)
    resultsQueue = multiprocessing.Queue() # The queue of results

    # Add tasks/inputFiles
    for i in range(numInputFiles):
        tasksQueue.put(inputFiles[i])
    for i in range(numberProcs): # add as many None jobs as numberProcs to tell them to terminate (queue is FIFO)
        tasksQueue.put(None)

    processes = []

    for i in range(numberProcs):
        print('appending process {}'.format(i))
        processes.append(multiprocessing.Process(target=runProcess,
            args=(i, tasksQueue, resultsQueue, args, config)))
        print('starting process {}'.format(i))
        processes[-1].start()

    processedList =[]
    for i in range(numInputFiles):
        #this should wait for a result
        print('retrieving results for input (file) {}'.format(i))
        results = resultsQueue.get()
        processedList.append(results)
        print ('Completed %d of %d (%.02f%%)' % (i+1, numInputFiles, 100. * float(i+1) / float(numInputFiles)))

    # wait for all users to finish their execution
    for i in range(numberProcs):
        print('joining processes')
        processes[i].join()

    timestampNow = datetime.datetime.now().timestamp()

    outDict = {processedListElement[0]:processedListElement[1:] for processedListElement in processedList}

    legacyjs, latestjs = wu.get_outDict_paths(args,config,timestampNow)

    with  open(legacyjs,'w') as ofile:
        ofile.write(json.dumps(outDict,indent=4))

    with  open(latestjs,'w') as ofile:
        ofile.write(json.dumps(outDict,indent=4))






def runProcess(procId, tasksQueue, resultsQueue, args, config):

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
            repeat=60
            print('checking for acces...')
            conn = wu.check_input_access(inputFile,waitLoop=repeat) # this enters a retry cycle for the input access. Units of 60 seconds

            if conn != False:
                retLocalCopy, localInputFile =wu.local_copy_inputFile(inputFile,config)

                if retLocalCopy[0] == 0:
                    lcfailure=False
                    try:
                        try:
                            localOutputFile=invokeLaserchicken(localInputFile,args,config,resultsQueue)
                        except:
                            lcfailure=True
                            raise Exception('Failure in Laserchicken invocation')
                            print(traceback.format_exc())

                        retRemotePush=wu.remote_copy_outputFile(localOutputFile,args,config)
                        if retRemotePush[0]!=0:
                            raise Exception('Failure in remote copy')

                    except:
                        if lcfailure == True:
                            kill_received = True
                        else:
                            print('removing local output file')
                            retRemoveLocalOutputFile = wu.remove_local_outPutFile(localOutputFile)
                            resultsQueue.put()


                else:
                    print('failed to copy {} to local temp directory. Skipping...'.format(inputFile))
                    resultsQueue.put()

             else:
                 print ('cannot acces input file (likely due to mounting). Retried for {0} times 60 seconds. Killing process {1}.'.format(repeat,processIndex))
                 kill_received=True








def main():

    args = argument_parser().parse_args()

    config = parse_config(args.configFile)

    parentInputList=wu.read_input_list(config)

    inputList = wu.split_input_list(parentInputList,args.numberVms,args.vmIndex)

   try:

       t0 = time.time()
       print('Starting ' + os.path.basename(__file__) + '...')
       run(inputList, args, config)
       print( 'Finished in %.2f seconds' % (time.time() - t0))

   except:
       print('Execution failed!')
       print(traceback)




if __name__='__main__':
    main()
