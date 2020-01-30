#!/usr/bin/env python3

import argparse, os, json



def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-lf','--logfile',help='json log file produced by files_copy.py to be checked',required=True)
    parser.add_argument('-s','--failurestring',help='string to identify failure record',required=True)
    parser.add_argument('-o','--output',default='files_copy_results_failed.txt',help='Outputpath for file listing failed copies')

    return parser

def read_json_log(logfile):
    log=None
    try:
        with open(logfile,'r') as jlf:
            log = json.load(jlf)

    except:
        print('failure to open log file')

    return log

def parse_log(log):

    logentries = [ key for key in log.keys()]

    return logentries

def get_failures(log,logentries,failstring):
    failed = None
    try:
        failed = [entry for entry in logentries if failstring in log[entry][1]]
        #print(failed)
    except:
        print('Exception during selection of failed')

    return failed

def serialize_failures(failed,output):
    try:
        with open(output,'w') as of:
            for entry in failed:
                of.write(entry+'\n')
    except:
        print('failed to write output')







def main():


    args = argument_parser().parse_args()
    print(args.logfile)

    log = read_json_log(args.logfile)

    if log != None:

        logentries = parse_log(log)

        failed = get_failures(log,logentries,args.failurestring)

        if failed != None:

            if len(failed) != 0 :
                serialize_failures(failed,args.output)

            else:
                print('no failed entried found')

        else:
            print('failed of None type. Exception')


    else:
        print('logfile of type None. Exception')







if __name__=='__main__':
    main()
