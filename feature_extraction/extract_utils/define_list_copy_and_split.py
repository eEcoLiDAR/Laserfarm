#!/usr/bin/env python3

import os, argparse
import numpy as np


def argument_parser():
    parser=argparse.ArgumentParser()
    parser.add_argument('-pl','--parentlist',help='list of all files that can be copied')
    parser.add_argument('-el','--exclusionlist',default=None,help='list of file not to be copied')
    parser.add_argument('-ed','--exclusiondirectory',default=None, help='directory path, the contents of which form the exclusionlist')
    parser.add_argument('-o','--output',help='path of output file')
    parser.add_argument('-n','--number',help='number of lists into which parentlist filtered by exclusion is to be split')

    return parser

def read_list(inf):
    with open(inf,'r') as inl:
        lines = inl.readlines()
        
    return [line.rstrip() for line in lines]


def exclusion_directory_to_list(directorypath):
    if os.path.isdir(directorypath) == False:
        print('{} is not a valid directory'.format(directorypath))
        raise OSError

    else:
        cont = os.listdir(directorypath)
        directorylist = [ os.path.splitext(file)[0].split('_norm')[0] for file in cont]
        return directorylist





def filter_list(inl,ex):
    return [ele for ele in inl if ele not in ex] 



def write_list(inf,out):
    with open(out,'w') as ol:
        for ele in inf:
            ol.write(ele+'\n')


def split_list(inf,n):
    lenIn = len(inf)
    lenSublist = int(np.floor(lenIn/n))
    lenremain = lenIn - n*lenSublist
    sublists = []
    for i in range(n-1):
        sublist = inf[i*lenSublist:(i+1)*lenSublist]
        sublists.append(sublist)
    sublistn = inf[(n-1)*lenSublist:]
    sublists.append(sublistn)

    return sublists
    


    


def main():
    args=argument_parser().parse_args()

    parentlist = read_list(args.parentlist)

    if args.exclusionlist != None :
        if args.exclusiondirectory == None:
            exclusionlist = read_list(args.exclusionlist)
        else:
            print('Both exclusion list and exclusiondirectory specified. Aborting')
            return

    else:
        if args.exclusiondirectory != None:
            exclusionlist = exclusion_directory_to_list(args.exclusiondirectory)
            print(exclusionlist)
        else:
            print('neither exclusion list nor directory specified. Aborting')
            return


    filtered_list = filter_list(parentlist,exclusionlist)

    if int(args.number) > 1 :
        outnames=[ os.path.splitext(args.output)[0]+'_'+str(i)+os.path.splitext(args.output)[1] for i in range(int(args.number))]

        sublists = split_list(filtered_list,int(args.number))

        for i in range(int(args.number)):

            sli = sublists[i]
            oname = outnames[i]

            write_list(sli,oname)
            



    oname = args.output
    write_list(filtered_list,oname)


        
    


if __name__=='__main__':
    main()
