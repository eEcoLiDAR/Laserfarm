#!/usr/bin/env python3

import os, argparse, subprocess, time, multiprocessing, shutil, json, traceback, datetime



def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s','--source',help='source directory',required=True)
    parser.add_argument('-d','--destination',help='destination directory',required=True)
    parser.add_argument('-l','--list',help='list of files to be copied',required=True)
    parser.add_argument('-p','--prefix',default=None,help='prefix to  be added to name is list')
    parser.add_argument('-su','--suffix',default=None,help='suffix to be added to name in list')
    parser.add_argument('-n','--nproc',default=1,help='number of parallel processes')
    parser.add_argument('-m','--monitor',default=None,help='monitor volume usage for this volume. Pause if exceeds 98%')

    return parser


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


def get_input(args):
    inputList = None
    try:
        with open(args.list,'r') as inputBaseList:
            lines=inputBaseList.readlines()
            inputList=[]
            for line in lines:
                inline=line.rstrip()
                if args.prefix != None:
                    inline=args.prefix+inline
                if args.suffix != None:
                    inline=inline+args.suffix
                inputList.append(inline)

    except IOError:
        print('Error opening input list file')

    return inputList

def monitor(args):
    execute='wait'
    cnt=0
    while execute == 'wait':
        if cnt > 3:
            print('waited but local disk remains too full')
            execute ='full'
        else :
            usagetuple = shutil.disk_usage(args.monitor)
            print(usagetuple)
            usageratio = usagetuple[1]/(1.*usagetuple[0])
            if usageratio < 0.97:
                execute='run'
                print('running')
            else:
                print('sleeping')
                print(datetime.datetime.now())
                time.sleep(300)
                print(datetime.datetime.now())
            cnt+=1

    return execute


def run(args,inputList):

    lenInputList= len(inputList)
    # Create queues for the distributed processing
    tasksQueue = multiprocessing.Queue() # The queue of tasks (inputFiles)
    resultsQueue = multiprocessing.Queue() # The queue of results

    # Add tasks/inputFiles
    for i in range(lenInputList):
        tasksQueue.put(inputList[i])
    for i in range(int(args.nproc)): #we add as many None jobs as numberProcs to tell them to terminate (queue is FIFO)
        tasksQueue.put(None)

    print('process startup')
    processes = []
    # We start numberProcs users processes
    for i in range(int(args.nproc)):
        processes.append(multiprocessing.Process(target=run_process,
            args=(i, tasksQueue, resultsQueue, args )))
        processes[-1].start()

    resultsList =[]
    for i in range(lenInputList):
        result = resultsQueue.get()
        resultsList.append(result)

    # wait for all users to finish their execution
    for i in range(int(args.nproc)):
        print('joining processes')
        processes[i].join()

    resultsDict ={resultsElement[0]: resultsElement[1:] for resultsElement in resultsList}

    with open('files_copy_results.js','w') as jsf:
        jsf.write(json.dumps(resultsDict,indent=4))





def run_process(processIndex, tasksQueue,resultsQueue,args):
    print('this is process {}'.format(processIndex))
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

            try:
                srcpath = os.path.join(args.source,inputFile)
                destpath = os.path.join(args.destination,inputFile)
                proceed = 'full'
                if args.monitor == None:
                    proceed = 'run'
                else :
                    print('starting monintor')
                    proceed = monitor(args)
                    print('monitor returned {}'.format(proceed))
                if proceed == 'run':


                    cmd = 'cp ' + srcpath + ' ' + destpath
                    res2 = shellExecute(cmd)
                    #res = shutil.copy2(srcpath,destpath)
                    if res2[0] != 0:
                        result=[inputFile,processIndex,res2[1]]
                    else:
                        result=[inputFile,processIndex,destpath]
                        #result=[inputFile,processIndex,res]
                    resultsQueue.put(result)
                else :
                    print('monitoring disk {}'.format(args.monitor))
                    print('monitored disk too full')
                    result = [inputFile,processIndex,'failure to copy. Disk too full']
                    resultsQueue.put(result)


            except (IOError,OSError) as err:
                print('failure to copy {0}. error was: {1}'.format(inputFile,err))
                result=[inputFile,processIndex,'failed']
                resultsQueue.put(result)








def main():

    args=argument_parser().parse_args()

    inputList = get_input(args)
    print(inputList)

    if inputList != None:

        try:
            t0 = time.time()
            print('copying files ...')
            run(args,inputList)
            print('finished in {} seconds'.format(time.time() - t0))

        except:
            print('failure to execute copying!')
            traceback.print_exc()

    else:
        print('Empty inputList')






if __name__=='__main__':
    main()
