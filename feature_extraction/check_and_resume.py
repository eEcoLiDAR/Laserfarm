#!/usr/bin/env python3

import os, argparse


def argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l','--list',help='list of tilees to be processed')
    parser.add_argument('-op','--outpath',help='output directory containing processed tiles')
    parser.add_argument('-ct','--checktag',help='tag to be appended to list name for output')


    return parser


def get_planned(inlist):
    with open(inlist,'r') as inf:
        lines = inf.readlines()
    intiles = []
    for line in lines:
        intiles.append(line.strip())

    return intiles


def get_processed(outdir):
    outcont = os.listdir(outdir)
    processed = [os.path.splitext(tile.strip())[0] for tile in outcont if tile.endswith('.ply') ]

    return processed


def write_filtered( args):

    listpathsplit = os.path.split(args.list)
    listpath = listpathsplit[0]
    listname = listpathsplit[1]
    listnamesplit = os.path.splitext(listname)
    listbase = listnamesplit[0]
    listext = listnamesplit[1] 
    outlist = listbase + args.checktag + listext
    outlistpath = os.path.join(listpath,outlist)
    
    planned = get_planned(args.list)
    processed = get_processed(args.outpath)

    with open(outlistpath,'w') as of:
        remaining = [tile for tile in planned if tile not in processed ]
        for rtile in remaining:
            of.write(rtile+'\n')



def main():

    args = argument_parser().parse_args()
    
    write_filtered(args)


if __name__=='__main__':
    main()
